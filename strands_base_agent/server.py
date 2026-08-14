# Copyright 2026 Booz Allen Hamilton Inc.
# SPDX-License-Identifier: Apache-2.0
"""A2A server entry point.

Provides the entry point for starting application and A2A protocol servers.
"""

import asyncio
import logging
import os
import threading
import weakref
from contextlib import asynccontextmanager
from typing import Any, Self, cast

import uvicorn
from a2a.utils.errors import ServerError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from strands.multiagent.a2a.server import A2AServer

from foundry_agent_core import AgentBackend
from foundry_strands_agent import AgentConfig, AgentFactory, StrandsAgentBackend, StrandsAgentFactory

from strands_base_agent.application.factory import create_application_container
from strands_base_agent.application.lifecycle import boot_system
from strands_base_agent.application.runtime_container import set_runtime_container

logger = logging.getLogger(__name__)


class MCPReconnectExhaustedError(RuntimeError):
    """Raised when MCP reconnect + retry still fails."""


class ReconnectingAgentProxy:
    """A proxy for an agent that reconnects to an MCP server if it fails."""

    def __init__(
        self,
        agent,
        agent_factory,
        max_retries: int = 1,
        config_overrides: dict[str, Any] | None = None,
    ):
        self._agent_factory = agent_factory
        self._max_retries = max_retries
        self._config_overrides = config_overrides
        self._lock = threading.RLock()
        self._agent = agent
        self._needs_rebuild = False

    @classmethod
    async def create(
        cls,
        agent_factory,
        max_retries: int = 1,
        config_overrides: dict[str, Any] | None = None,
    ) -> Self:
        create_kwargs = {"config_overrides": config_overrides} if config_overrides else {}
        agent = await agent_factory.create_agent(**create_kwargs)
        return cls(
            agent,
            agent_factory,
            max_retries=max_retries,
            config_overrides=config_overrides,
        )

    async def _recreate_agent_async(self) -> None:
        logger.info("Recreating agent due to MCP error...")

        old = None
        create_kwargs = {"config_overrides": self._config_overrides} if self._config_overrides else {}
        new_agent = await self._agent_factory.create_agent(**create_kwargs)

        with self._lock:
            old = self._agent
            self._agent = new_agent

        close_fn = getattr(old, "close", None)
        if callable(close_fn):
            try:
                maybe_coro = close_fn()
                if asyncio.iscoroutine(maybe_coro):
                    await maybe_coro
            except Exception:
                pass

        logger.info("Agent recreated successfully")

    def _is_mcp_error(self, err: Exception) -> bool:
        text = str(err).lower()
        markers = (
            "mcp",
            "mcpconnectionerror",
            "mcpclientinitializationerror",
            "client session is not running",
            "connection to the mcp server was closed",
            "streamablehttp",
        )
        return any(m in text for m in markers)

    def _event_has_mcp_failure(self, event: object) -> bool:
        text = f"{event!r} {event}".lower()
        markers = (
            "mcpclientinitializationerror",
            "client session is not running",
            "connection to the mcp server was closed",
            "tool execution failed",
            "failed to process tool",
            "Connection to the MCP server was closed",
        )
        return any(m in text for m in markers)

    def mark_stale(self) -> None:
        with self._lock:
            self._needs_rebuild = True

    async def _ensure_fresh_agent(self) -> None:
        with self._lock:
            needs_rebuild = self._needs_rebuild
            if needs_rebuild:
                self._needs_rebuild = False

        if not needs_rebuild:
            return

        await self._recreate_agent_async()

    async def _stream_once(self, *args, **kwargs):
        with self._lock:
            agent = self._agent
        async for event in agent.stream_async(*args, **kwargs):
            yield event

    def stream_async(self, *args, **kwargs):
        async def _gen():
            for attempt in range(self._max_retries + 1):
                await self._ensure_fresh_agent()
                saw_mcp_failure = False
                try:
                    async for event in self._stream_once(*args, **kwargs):
                        if self._event_has_mcp_failure(event):
                            saw_mcp_failure = True
                            self.mark_stale()
                            break
                        yield event
                except Exception as e:
                    if self._is_mcp_error(e):
                        saw_mcp_failure = True
                        self.mark_stale()
                    else:
                        raise

                if not saw_mcp_failure:
                    return

                if attempt < self._max_retries:
                    await self._ensure_fresh_agent()
                    continue

                # exhausted: emit explicit stream error event instead of silent end
                yield {
                    "type": "error",
                    "error": "mcp_reconnect_failed",
                    "message": f"MCP unavailable after {self._max_retries + 1} attempt(s)",
                    "retryable": True,
                }
                return

        return _gen()

    async def invoke_async(self, *args, **kwargs):
        await self._ensure_fresh_agent()
        with self._lock:
            agent = self._agent
        try:
            return await agent.invoke_async(*args, **kwargs)
        except Exception as e:
            if self._is_mcp_error(e):
                self.mark_stale()
                await self._recreate_agent_async()
                with self._lock:
                    agent = self._agent
                return await agent.invoke_async(*args, **kwargs)  # one retry
            raise

    def __getattr__(self, item: str):
        with self._lock:
            return getattr(self._agent, item)

    async def aclose(self) -> None:
        with self._lock:
            agent = self._agent

        close_fn = getattr(agent, "close", None)
        if callable(close_fn):
            result = close_fn()
            if asyncio.iscoroutine(result):
                await result


class _NoOpToolRegistry:
    """Empty registry used only while A2AServer builds its public agent card."""

    def get_all_tools_config(self) -> dict[str, Any]:
        """Return no card skills until an explicit card-skill bootstrap is added."""
        return {}


class ContextAgentProxy:
    """Lazy, context-scoped bridge from Strands' sync factory to Foundry's async factory.

    ``A2AServer.agent_factory`` must synchronously return an Agent-like object, but
    Foundry creates configured agents asynchronously because tool and MCP setup may
    perform I/O. This proxy is cheap to construct synchronously and creates exactly
    one real agent on the first async invocation for its A2A context.
    """

    def __init__(
        self,
        context_id: str,
        agent_factory,
        *,
        name: str,
        description: str,
        persist_session: bool,
        max_retries: int = 1,
    ) -> None:
        self.context_id = context_id
        self.name = name
        self.description = description
        self._agent_factory = agent_factory
        self._persist_session = persist_session
        self._max_retries = max_retries
        self._proxy: ReconnectingAgentProxy | None = None
        self._initialization_lock = asyncio.Lock()
        self._card_tool_registry = _NoOpToolRegistry()

    @property
    def tool_registry(self):
        """Expose real tools after initialization and a safe card-time registry before it."""
        if self._proxy is not None:
            return self._proxy.tool_registry
        return self._card_tool_registry

    async def _ensure_initialized(self) -> ReconnectingAgentProxy:
        """Create this context's real agent once, without blocking A2AServer construction."""
        if self._proxy is not None:
            return self._proxy

        async with self._initialization_lock:
            if self._proxy is None:
                # A2A context IDs become session IDs only when persistence is enabled.
                # Otherwise each dedicated agent still provides in-memory isolation
                # without implicitly enabling encrypted file sessions.
                config_overrides = {"session_id": self.context_id} if self._persist_session else None
                self._proxy = await ReconnectingAgentProxy.create(
                    self._agent_factory,
                    max_retries=self._max_retries,
                    config_overrides=config_overrides,
                )
                self._proxy._agent.name = self.name
                self._proxy._agent.description = self.description
        return self._proxy

    def stream_async(self, *args, **kwargs):
        """Initialize lazily, then stream through this context's reconnecting proxy."""

        async def _stream():
            proxy = await self._ensure_initialized()
            async for event in proxy.stream_async(*args, **kwargs):
                yield event

        return _stream()

    async def invoke_async(self, *args, **kwargs):
        """Initialize lazily, then invoke this context's agent."""
        proxy = await self._ensure_initialized()
        return await proxy.invoke_async(*args, **kwargs)

    def cancel(self) -> None:
        """Best-effort cancellation for an already initialized context."""
        if self._proxy is not None:
            self._proxy.cancel()

    async def aclose(self) -> None:
        """Close this context's agent and outbound clients if it was initialized."""
        if self._proxy is not None:
            await self._proxy.aclose()


def start_server() -> None:
    """Start the A2A server.

    This function handles starting an A2A protocol server.

    This function is blocking and will not return until the server is shut down.

    Raises:
        ImportError: If A2A dependencies are not installed
        RuntimeError: If server initialization fails or if called in client mode
    """
    # Load configuration
    config = AgentConfig.from_env()

    # Get deployment configuration
    host = os.getenv("HOST", "0.0.0.0")
    # Use agent-specific port if configured, otherwise fall back to general PORT
    port = config.agent_port if config.agent_port is not None else int(os.getenv("PORT", "8000"))

    # Get public URL for agent card (use service name in Docker, or AGENT_PUBLIC_URL if set)
    agent_public_url = os.getenv("STRANDS_AGENT_PUBLIC_URL", f"http://{config.agent_name}:{port}")
    if not os.getenv("STRANDS_AGENT_PUBLIC_URL"):
        logger.warning(
            "STRANDS_AGENT_PUBLIC_URL is not set. The agent card will advertise "
            "the default internal URL (%s) which may not be reachable externally. "
            "Set STRANDS_AGENT_PUBLIC_URL to the public ingress URL for production deployments.",
            agent_public_url,
        )

    # Get agent version for A2A identity
    version = config.agent_version

    logger.info("Starting A2A server on %s:%s (version: %s)", host, port, version)
    logger.info("Agent discovery endpoint %s", agent_public_url)

    # Create agent using the factory pattern (register for FastAPI deps — see runtime_container)
    container = create_application_container()

    # Register AgentFactory in the composition root — adopters customize here.
    # Pass session_manager_factories / model_provider_factories dicts to extend.
    container.register_factory(
        AgentFactory,
        lambda: StrandsAgentFactory(container),
        singleton=True,
    )

    container.register_factory(
        AgentBackend,
        lambda: StrandsAgentBackend(container),
        singleton=True,
    )

    set_runtime_container(container)
    foundry_agent_factory = container.resolve(AgentFactory)
    live_context_agents: weakref.WeakSet[ContextAgentProxy] = weakref.WeakSet()

    def create_context_agent(context_id: str):
        """Return a dedicated lazy agent for one A2A conversation context.

        Strands invokes this synchronously once for agent-card metadata and once
        for each new runtime context. The proxy defers Foundry's asynchronous
        model/tool initialization until the context receives its first request.
        """
        context_agent = ContextAgentProxy(
            context_id,
            foundry_agent_factory,
            name=config.agent_name,
            description=config.agent_description,
            persist_session=getattr(config, "session_type", None) is not None,
            max_retries=1,
        )
        live_context_agents.add(context_agent)
        return cast(Any, context_agent)

    a2a_server = A2AServer(
        # Factory mode is the Strands-recommended lifecycle: each context owns
        # an independent agent and can execute concurrently without snapshot
        # swapping or cross-context conversation state.
        agent_factory=create_context_agent,
        host=host,
        port=port,
        http_url=agent_public_url,
        version=version,
    )

    # Access the underlying FastAPI app
    fastapi_app = a2a_server.to_fastapi_app()

    from foundry_agent_fastapi import (
        add_cors_middleware,
        add_error_handling_middleware,
        add_request_logging_middleware,
        health_router,
    )

    add_cors_middleware(fastapi_app)
    add_request_logging_middleware(fastapi_app)
    add_error_handling_middleware(fastapi_app)

    from strands_base_agent.api.routes import chat_history_router, query_router

    fastapi_app.include_router(health_router)
    fastapi_app.include_router(query_router)
    fastapi_app.include_router(chat_history_router)

    original_lifespan = fastapi_app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with original_lifespan(app):
            try:
                yield
            finally:
                # Weak references preserve Strands' max_contexts/LRU ownership;
                # close every still-live initialized context during shutdown.
                for context_agent in list(live_context_agents):
                    try:
                        await context_agent.aclose()
                    except Exception as e:
                        logger.warning(
                            "Agent shutdown cleanup warning for context %s: %s",
                            context_agent.context_id,
                            e,
                        )

    fastapi_app.router.lifespan_context = lifespan

    def _is_mcp_chain(exc: Exception) -> bool:
        markers = (
            "mcpclientinitializationerror",
            "client session is not running",
            "connection to the mcp server was closed",
            "mcp",
        )
        seen = set()
        cur: BaseException | None = exc
        while cur and id(cur) not in seen:
            seen.add(id(cur))
            msg = str(cur).lower()
            if any(m in msg for m in markers):
                return True
            cur = cur.__cause__ or cur.__context__
        return False

    @fastapi_app.exception_handler(ServerError)
    async def handle_a2a_server_error(_: Request, exc: ServerError):  # pyright: ignore[reportUnusedFunction]
        if _is_mcp_chain(exc):
            # Reconnect state is owned by the failing context's proxy. The HTTP
            # contract remains retryable without marking every context stale.
            return JSONResponse(
                status_code=400,
                content={
                    "error": "mcp_session_stale",
                    "message": "MCP session became stale; retry the request.",
                    "retryable": True,
                },
            )

        return JSONResponse(
            status_code=500,
            content={"error": "internal_error", "message": "A2A server error"},
        )

    @fastapi_app.exception_handler(MCPReconnectExhaustedError)
    async def handle_mcp_reconnect_exhausted(_: Request, exc: MCPReconnectExhaustedError):  # pyright: ignore[reportUnusedFunction]
        return JSONResponse(
            status_code=400,
            content={
                "error": "mcp_reconnect_failed",
                "message": str(exc),
                "retryable": True,
            },
        )

    # Validate TLS posture before starting the server
    from strands_base_agent.application.tls_config import (
        TlsMode,
        load_tls_config,
        validate_tls_config,
    )

    tls_config = load_tls_config()
    validate_tls_config(tls_config)

    # Start server (blocking call)
    if tls_config.tls_mode == TlsMode.NATIVE:
        uvicorn.run(
            fastapi_app,
            host=host,
            port=port,
            ssl_keyfile=tls_config.tls_keyfile,
            ssl_certfile=tls_config.tls_certfile,
        )
    else:
        uvicorn.run(fastapi_app, host=host, port=port)


if __name__ == "__main__":
    boot_system()
    start_server()
