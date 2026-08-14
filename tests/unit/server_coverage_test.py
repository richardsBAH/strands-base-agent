# Copyright 2026 Booz Allen Hamilton Inc.
# SPDX-License-Identifier: Apache-2.0
"""Tests for server.py utility functions and ReconnectingAgentProxy."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from strands_base_agent.server import ContextAgentProxy, ReconnectingAgentProxy


def _agen(items):
    async def _gen():
        for item in items:
            yield item

    return _gen()


class TestContextAgentProxy:
    @pytest.mark.asyncio
    async def test_lazily_creates_persistent_context_agent(self):
        mock_agent = MagicMock()
        mock_agent.stream_async = MagicMock(return_value=_agen([{"type": "final"}]))
        mock_factory = MagicMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = ContextAgentProxy(
            "context-123",
            mock_factory,
            name="test-agent",
            description="test description",
            persist_session=True,
        )

        # Construction is synchronous and must not perform model, tool, or MCP I/O.
        mock_factory.create_agent.assert_not_awaited()

        events = [event async for event in proxy.stream_async("hello")]

        assert events == [{"type": "final"}]
        mock_factory.create_agent.assert_awaited_once_with(config_overrides={"session_id": "context-123"})
        assert mock_agent.name == "test-agent"
        assert mock_agent.description == "test description"

    @pytest.mark.asyncio
    async def test_nonpersistent_context_does_not_enable_file_sessions(self):
        mock_agent = MagicMock()
        mock_agent.invoke_async = AsyncMock(return_value="response")
        mock_factory = MagicMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = ContextAgentProxy(
            "context-456",
            mock_factory,
            name="test-agent",
            description="test description",
            persist_session=False,
        )

        assert await proxy.invoke_async("hello") == "response"
        mock_factory.create_agent.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_distinct_contexts_create_distinct_agents(self):
        first_agent = MagicMock()
        first_agent.invoke_async = AsyncMock(return_value="first")
        second_agent = MagicMock()
        second_agent.invoke_async = AsyncMock(return_value="second")
        mock_factory = MagicMock()
        mock_factory.create_agent = AsyncMock(side_effect=[first_agent, second_agent])

        first = ContextAgentProxy(
            "context-a",
            mock_factory,
            name="test-agent",
            description="test description",
            persist_session=False,
        )
        second = ContextAgentProxy(
            "context-b",
            mock_factory,
            name="test-agent",
            description="test description",
            persist_session=False,
        )

        assert await first.invoke_async("one") == "first"
        assert await second.invoke_async("two") == "second"
        assert first._proxy is not second._proxy
        assert mock_factory.create_agent.await_count == 2


class TestReconnectingAgentProxyCreate:
    @pytest.mark.asyncio
    async def test_create_calls_factory_and_returns_proxy(self):
        mock_agent = MagicMock()
        mock_factory = AsyncMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = await ReconnectingAgentProxy.create(mock_factory, max_retries=2)

        mock_factory.create_agent.assert_awaited_once()
        assert proxy._agent is mock_agent
        assert proxy._max_retries == 2


class TestReconnectingAgentProxyGetattr:
    @pytest.mark.asyncio
    async def test_getattr_returns_attribute_from_agent(self):
        mock_agent = MagicMock()
        mock_agent.name = "test-agent"
        mock_factory = AsyncMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = await ReconnectingAgentProxy.create(mock_factory)

        assert proxy.name == "test-agent"

    @pytest.mark.asyncio
    async def test_getattr_returns_callable_from_agent(self):
        mock_agent = MagicMock()
        mock_agent.some_method = MagicMock(return_value="result")
        mock_factory = AsyncMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = await ReconnectingAgentProxy.create(mock_factory)

        result = proxy.some_method()
        assert result == "result"


class TestReconnectingAgentProxyInvokeAsync:
    @pytest.mark.asyncio
    async def test_invoke_async_delegates_to_agent(self):
        mock_agent = AsyncMock()
        mock_agent.invoke_async = AsyncMock(return_value="response")
        mock_factory = AsyncMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = await ReconnectingAgentProxy.create(mock_factory)

        result = await proxy.invoke_async("hello")
        assert result == "response"
        mock_agent.invoke_async.assert_awaited_once_with("hello")

    @pytest.mark.asyncio
    async def test_invoke_async_retries_on_mcp_error(self):
        mock_agent = AsyncMock()
        mock_agent.invoke_async = AsyncMock(side_effect=RuntimeError("MCP connection error"))
        new_agent = AsyncMock()
        new_agent.invoke_async = AsyncMock(return_value="recovered")

        mock_factory = AsyncMock()
        mock_factory.create_agent = AsyncMock(side_effect=[mock_agent, new_agent])

        proxy = await ReconnectingAgentProxy.create(mock_factory)

        result = await proxy.invoke_async("hello")
        assert result == "recovered"

    @pytest.mark.asyncio
    async def test_invoke_async_raises_non_mcp_error(self):
        mock_agent = AsyncMock()
        mock_agent.invoke_async = AsyncMock(side_effect=ValueError("unrelated"))
        mock_factory = AsyncMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = await ReconnectingAgentProxy.create(mock_factory)

        with pytest.raises(ValueError, match="unrelated"):
            await proxy.invoke_async("hello")


class TestReconnectingAgentProxyAclose:
    @pytest.mark.asyncio
    async def test_aclose_calls_close_on_agent(self):
        mock_agent = MagicMock()
        mock_agent.close = MagicMock(return_value=None)
        mock_factory = AsyncMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = await ReconnectingAgentProxy.create(mock_factory)

        await proxy.aclose()
        mock_agent.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_aclose_handles_async_close(self):
        mock_agent = MagicMock()

        async def async_close():
            pass

        mock_agent.close = MagicMock(return_value=async_close())
        mock_factory = AsyncMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = await ReconnectingAgentProxy.create(mock_factory)

        await proxy.aclose()
        mock_agent.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_aclose_handles_no_close_method(self):
        mock_agent = MagicMock(spec=[])
        mock_factory = AsyncMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = await ReconnectingAgentProxy.create(mock_factory)

        await proxy.aclose()


class TestIsMcpError:
    @pytest.mark.asyncio
    async def test_detects_mcp_errors(self):
        mock_agent = MagicMock()
        mock_factory = AsyncMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = await ReconnectingAgentProxy.create(mock_factory)

        assert proxy._is_mcp_error(RuntimeError("MCP connection failed"))
        assert proxy._is_mcp_error(RuntimeError("client session is not running"))
        assert not proxy._is_mcp_error(RuntimeError("some other error"))

    @pytest.mark.asyncio
    async def test_event_has_mcp_failure(self):
        mock_agent = MagicMock()
        mock_factory = AsyncMock()
        mock_factory.create_agent = AsyncMock(return_value=mock_agent)

        proxy = await ReconnectingAgentProxy.create(mock_factory)

        assert proxy._event_has_mcp_failure({"error": "MCPClientInitializationError"})
        assert proxy._event_has_mcp_failure({"msg": "tool execution failed"})
        assert not proxy._event_has_mcp_failure({"msg": "everything is fine"})
