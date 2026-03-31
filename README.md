## Private Compute Network MVP

This repository contains a small MVP for the PCN architecture:

- `client` accepts raw search input and splits a query into fragments before sending work onward
- `core` contains the shared task splitting utilities, opaque identifier helpers, verified distribution logic, and crypto transport helpers
- `coordinator` maintains a persisted node registry, streams dashboard events over SSE, and distributes fragments across active nodes with retries
- `node` processes a fragment against its local shard, stays stateless, registers itself dynamically, and sends heartbeats
- `aggregator` merges partial node results into ranked search matches through its own API

## Design Rules Applied

- Full user queries are never stored.
- Nodes are stateless and compute directly from request payloads plus local shard data.
- The coordinator works only with query fragments, not the original raw query text.
- Internal task and job identifiers are opaque and do not embed fragment text.
- Code favors small modules and readable control flow over abstraction-heavy patterns.

## Phase 2 Task 1-8

Dynamic node registration, heartbeat-based lifecycle management, dynamic task distribution, shard/replica-aware verification, encrypted fragment transport, node reputation-based selection, probation-based recovery, and a live dashboard are now in place.

### Verified execution behavior

- Each fragment is executed on 2 distinct active nodes by default
- Verification now happens inside replica groups so only equivalent data is compared
- Results from different shard groups can be merged after group-local verification succeeds
- If one execution fails, another active replica in the same group is tried
- If verified responses mismatch, the fragment is retried with new nodes in that replica group
- If verification still fails, the fragment is returned as an error
- Probation nodes are never used in redundant verification pairs
- Low-risk shadow executions can be sent to probation nodes after active verification succeeds
- Duplicate execution on the same node for the same fragment is not allowed

### Encryption behavior

- Coordinator-to-node fragment transport can be encrypted with AES-256-GCM
- Node responses can be encrypted before returning to the coordinator
- Coordinator decrypts node responses before verification and merging
- Plaintext fragment data is not logged
- Encryption is controlled by `ENABLE_ENCRYPTION=true`
- Shared secret comes from `ENCRYPTION_KEY`

Encrypted payload shape:

```json
{
  "iv": "base64",
  "content": "base64",
  "tag": "base64"
}
```

## Notes

- Unknown heartbeats are ignored safely.
- Nodes send heartbeats every 10 seconds by default.
- The coordinator marks nodes inactive after 30 seconds without a heartbeat.
- `GET /nodes` returns only active nodes and includes score/task statistics for each selectable node.
- Nodes start with score `100`, gain score on verified success, and lose score on task failures.
- Nodes with score below `20` move into `probation` instead of leaving the network immediately.
- Probation nodes receive only a small shadow slice of traffic and never decide accepted results.
- Probation recovery uses a larger score boost and nodes reintegrate to `active` once their score rises above `40`.
- Repeated failures can still drive a probation node fully `inactive`.
- Selection prefers higher-score nodes first, then lower relative load.
- Fragment distribution now lives in `core/distributor.js`.
- Redundancy defaults to `2` and verification retry rounds default to `2`.
- Redundant verification assumes the nodes validating the same fragment are working from equivalent data. If nodes hold different shards, mismatches are expected unless you introduce replica groups or shard-aware verification.
- Encryption is disabled by default. When enabled, both coordinator and nodes must share the same `ENCRYPTION_KEY`.

## Reputation Config

- `SCORE_SUCCESS_INC` controls how much score a node gains on verified success. Default: `2`
- `SCORE_FAILURE_DEC` controls how much score a node loses on timeout, mismatch, or decryption failure. Default: `5`

## Probation Config

- `PROBATION_TRAFFIC_RATIO` controls the share of low-risk shadow traffic offered to probation nodes. Default: `0.1`
- `PROBATION_SUCCESS_BOOST` controls the score gain when a probation node matches a verified result. Default: `5`

## Dashboard

- Open `http://localhost:4000/dashboard` to view the network dashboard
- Dashboard APIs are protected with `Authorization: Bearer <API_KEY>`
- The browser upgrades into a secure session with `POST /dashboard/session`, then consumes `GET /dashboard/stream` over Server-Sent Events
- The dashboard shows live node health, replica-group summaries, shard-level charts, recent fragment executions, and coordinator logs without polling
- Theme presets are built in for dark and light demo modes

## Auth And Persistence

- Set `API_KEY` on the coordinator and nodes to protect `/dashboard/*` and `/register-node`
- Nodes can also use `COORDINATOR_API_KEY` if you want a separate registration secret in their environment
- Node registry state persists to `data/node-registry.json` by default
- Override the persistence location with `NODE_REGISTRY_FILE`
- Persisted records include `lastSeen`, `status`, `score`, `totalTasks`, `successfulTasks`, `failedTasks`, `shardId`, and `replicaGroup`

## Replica Setup

- For reliable shard-aware verification, run at least 2 active replicas per `replicaGroup`
- The provided `docker-compose.yml` starts two replicas for `shard-a` and two replicas for `shard-b`

