---
sidebar_position: 10
---

# A2A Agent Lifecycle Discrepancy

## Status

This document records a compatibility discrepancy between the current Strands Base
Agent baseline and the current Strands Agents SDK guidance. It is an architecture
note and migration recommendation; it does not change runtime behavior.

## Executive Summary

The BAH Strands Base Agent baseline currently constructs `A2AServer` with the
deprecated `agent=` argument and a lazy stand-in for the real agent. Current Strands
documentation recommends `agent_factory=` for any server handling multiple A2A
conversation contexts:

- `agent` — a single Strands Agent to wrap. Deprecated; prefer `agent_factory`.
- `agent_factory` — a callable `(context_id) -> Agent` that builds a fresh agent
  per context.

The difference is operationally significant. The shared-agent mode serializes
requests and swaps conversation snapshots on and off one agent. Factory mode gives
each A2A context its own agent instance, lock, conversation state, and optional
session manager.

## Sources Compared

### BAH baseline documentation

The BAH [Built-in A2A Protocol guide](https://boozallen.github.io/agent-foundry/docs/baselines/strands-base-agent/guides/a2a-protocol/)
states that the baseline “wires its `Agent` instance” into `A2AServer`.

The BAH [Configuration guide](https://boozallen.github.io/agent-foundry/docs/baselines/strands-base-agent/configuration/)
documents `a2a_servers`, but those entries are outbound peers that this agent can
call. They do not select the inbound A2A server's agent lifecycle.

### Baseline implementation

`strands_base_agent/server.py` currently uses:

```python
a2a_server = A2AServer(
    agent=cast(Any, _LazyAgentProxy()),
    host=host,
    port=port,
    http_url=agent_public_url,
    version=version,
)
```

`_LazyAgentProxy` exists because the real agent is created asynchronously during
application startup while `A2AServer` needs agent metadata during synchronous
construction. Its placeholder snapshot implementation returns `None`:

```python
def take_snapshot(self, **kwargs):
    return None
```

The placeholder tool registry also returns no tools, so the agent card can advertise
an empty `skills` list even when tools are configured later.

### Current Strands SDK documentation

The current [`A2AServer` API reference](https://strandsagents.com/docs/api/python/strands.multiagent.a2a.server/)
requires exactly one of `agent` or `agent_factory` and describes:

- `agent_factory` as the recommended, per-context lifecycle.
- `agent` as deprecated and not multi-tenant safe.
- `max_contexts` as the bound on retained per-context agents.

The [A2A executor reference](https://strandsagents.com/docs/api/python/strands.multiagent.a2a.executor/)
also states that factory mode allows contexts to execute concurrently without
sharing conversation history.

## Observed Failure

With the baseline's pinned Strands SDK and an Ollama-backed agent, an A2A
`message/send` request failed before model execution:

```text
AttributeError: 'NoneType' object has no attribute 'validate'
```

The execution path was:

```text
StrandsA2AExecutor
  -> restore shared-agent context
  -> agent.load_snapshot(template_snapshot)
  -> template_snapshot is None
  -> snapshot.validate() fails
```

The REST query endpoint continued working because it does not use this shared A2A
snapshot path.

## Why a Null-Snapshot Guard Is Not the Long-Term Fix

A compatibility guard could ignore the initial `None` snapshot and delegate later
snapshot operations to the real agent. That may unblock a short evaluation, but it
would preserve the deprecated shared-agent architecture:

- all A2A contexts would still share one agent;
- requests would remain serialized;
- conversation isolation would depend on snapshot swapping;
- context-scoped session managers would remain unsupported;
- an error or reconnect could affect every caller;
- the agent card could still be built from placeholder metadata.

The Strands documentation does not define `None` as a valid snapshot. A null guard
would therefore be a local workaround rather than documented SDK behavior.

## Recommended Target Architecture

Use `A2AServer(agent_factory=...)` and create one logical agent per A2A
`context_id`:

```text
A2AServer
  -> context A -> dedicated agent A -> isolated history/session
  -> context B -> dedicated agent B -> isolated history/session
  -> context C -> dedicated agent C -> isolated history/session
```

Conceptually:

```python
def create_agent(context_id: str) -> Agent:
    return build_context_agent(session_id=context_id)


a2a_server = A2AServer(
    agent_factory=create_agent,
    max_contexts=100,
)
```

## Migration Considerations

The migration is not a one-line argument replacement:

1. Strands expects a synchronous `(context_id) -> Agent` callback, while the
   Foundry `AgentFactory` creates configured agents asynchronously.
2. `A2AServer` invokes the factory once during construction to derive agent-card
   metadata and skills.
3. Runtime agents should be initialized lazily per context so asynchronous MCP and
   tool setup does not block the event loop.
4. The A2A `context_id` should map to the agent's `session_id` where persistent
   sessions are enabled.
5. MCP reconnect state must be scoped to one context rather than a process-wide
   shared agent.
6. Context eviction and application shutdown must close agent and MCP resources.
7. `max_contexts` must be sized for the deployment's memory and connection limits.
8. A client-supplied `context_id` is not an authentication boundary; authenticated
   tenant and user identity must be enforced by the gateway.
9. Durable task storage is a separate concern. The baseline still uses an in-memory
   A2A task store unless a durable `TaskStore` is configured.

## Acceptance Criteria

A production migration should demonstrate:

- `A2AServer` receives `agent_factory=`, not `agent=`.
- Two different context IDs create distinct agent instances.
- Repeated requests with one context ID reuse only that context's state.
- Different contexts can run concurrently without history leakage.
- Agent-card skills represent configured tools.
- MCP failure in one context does not rebuild or interrupt another context.
- Context eviction and server shutdown release agent resources.
- Existing REST query, streaming, health, and chat-history behavior remains intact.

## Configuration Boundary

`a2a_servers` remains useful after the inbound lifecycle is corrected. It configures
remote A2A agents that the current agent may call:

```yaml
a2a_servers:
  - name: graph-agent
    url: http://graph-agent:8000/
```

It does not enable `agent_factory`, and it does not resolve the inbound snapshot
failure.
