---
sidebar_position: 11
---

# A2A Agent Factory Validation

## Purpose

This test validates the runtime behavior expected from
`A2AServer(agent_factory=...)`:

1. Repeated messages in one A2A context retain conversation history.
2. A different context cannot read that history.
3. Two contexts can execute concurrently without shared-state or event-loop
   conflicts.

The test exercises the live Docker service through the A2A JSON-RPC endpoint. It
does not mock the agent, model, HTTP server, or A2A executor.

## Environment

The successful validation used:

- Strands Base Agent running in Docker.
- Ollama running on the host.
- Ollama model `gemma3:4b`.
- A2A endpoint `http://localhost:8000/`.
- Per-context `agent_factory` implementation in `strands_base_agent/server.py`.

Verify the services before running the test:

```bash
docker compose ps

curl -sS --fail http://localhost:8000/api/v1/health \
  | python3 -m json.tool

curl -sS --fail http://localhost:11434/api/tags \
  | python3 -m json.tool
```

## Test command

Run from the `strands-base-agent` repository:

```bash
python3 - <<'PY'
import json
import sys
import uuid
import urllib.request
from concurrent.futures import ThreadPoolExecutor

URL = "http://localhost:8000/"
codeword = f"NEBULA-{uuid.uuid4().hex[:12].upper()}"


def send(context_id, text):
    payload = {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "message/send",
        "params": {
            "message": {
                "messageId": str(uuid.uuid4()),
                "contextId": context_id,
                "role": "user",
                "kind": "message",
                "parts": [{"kind": "text", "text": text}],
            }
        },
    }

    request = urllib.request.Request(
        URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=180) as response:
        body = json.load(response)

    result = body.get("result", {})
    state = result.get("status", {}).get("state")
    text_parts = []

    for artifact in result.get("artifacts", []):
        for part in artifact.get("parts", []):
            if part.get("kind") == "text":
                text_parts.append(part.get("text", ""))

    return state, "".join(text_parts).strip(), body


context_a = str(uuid.uuid4())
context_b = str(uuid.uuid4())

print("\n1. SAME-CONTEXT MEMORY")
stored = send(
    context_a,
    f"Remember this secret codeword: {codeword}. Reply exactly STORED.",
)
recalled = send(
    context_a,
    "What secret codeword did I ask you to remember? Reply with only the codeword.",
)

same_context_pass = (
    stored[0] == "completed"
    and recalled[0] == "completed"
    and codeword in recalled[1]
)

print("Store response:", stored[1])
print("Recall response:", recalled[1])
print("PASS" if same_context_pass else "FAIL")

print("\n2. CROSS-CONTEXT ISOLATION")
isolated = send(
    context_b,
    "What secret codeword did another conversation ask you to remember? "
    "If you have no record, reply exactly NO_RECORD.",
)

cross_context_pass = isolated[0] == "completed" and codeword not in isolated[1]

print("Other-context response:", isolated[1])
print("PASS" if cross_context_pass else "FAIL")

print("\n3. CONCURRENT CONTEXTS")
context_c = str(uuid.uuid4())
context_d = str(uuid.uuid4())

with ThreadPoolExecutor(max_workers=2) as executor:
    future_c = executor.submit(
        send,
        context_c,
        "Reply exactly CONCURRENT_C",
    )
    future_d = executor.submit(
        send,
        context_d,
        "Reply exactly CONCURRENT_D",
    )
    result_c = future_c.result()
    result_d = future_d.result()

concurrent_pass = (
    result_c[0] == "completed"
    and result_d[0] == "completed"
    and "CONCURRENT_C" in result_c[1]
    and "CONCURRENT_D" in result_d[1]
)

print("Context C:", result_c[1])
print("Context D:", result_d[1])
print("PASS" if concurrent_pass else "FAIL")

all_passed = same_context_pass and cross_context_pass and concurrent_pass
print("\nOVERALL:", "PASS" if all_passed else "FAIL")
sys.exit(0 if all_passed else 1)
PY
```

The command exits with status `0` only when all three checks pass.

## Recorded result

The live validation completed successfully:

```text
1. SAME-CONTEXT MEMORY
Store response: STORED
Recall response: NEBULA-C86A239C7E64
PASS

2. CROSS-CONTEXT ISOLATION
Other-context response: NO_RECORD
PASS

3. CONCURRENT CONTEXTS
Context C: CONCURRENT_C
Context D: CONCURRENT_D
PASS

OVERALL: PASS
```

## Interpretation

The result demonstrates:

- The same `contextId` resolves to the same logical context agent and retains its
  in-memory conversation history.
- A different `contextId` resolves to an isolated context agent that does not
  receive the first context's history.
- Separate contexts can complete overlapping requests without the shared-agent
  snapshot failure or an event-loop conflict.
- The original `_LazyAgentProxy` `load_snapshot(None)` failure is no longer on the
  active A2A execution path.

## What this test does not prove

This is a focused functional validation. It does not test:

- session persistence across container or process restarts;
- authentication or authorization of client-supplied context IDs;
- sustained concurrency, latency, or resource limits;
- `max_contexts` eviction behavior;
- durable A2A task storage;
- MCP reconnect behavior under live network failure;
- SQLite or knowledge-graph operations.

Those concerns require separate tests and should not be inferred from this result.
