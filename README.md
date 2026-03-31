<<<<<<< HEAD
## Private Compute Network MVP

This repository contains a small MVP for the PCN architecture:

- `client` accepts raw search input and splits a query into fragments before sending work onward
- `core` contains the shared task splitting utilities and opaque identifier helpers
- `coordinator` distributes fragments across nodes without receiving the raw query string
- `node` processes a fragment against its local shard and stays stateless
- `aggregator` merges partial node results into ranked search matches through its own API

## Design Rules Applied

- Full user queries are never stored.
- Nodes are stateless and compute directly from request payloads plus local shard data.
- The coordinator works only with query fragments, not the original raw query text.
- Internal task and job identifiers are opaque and do not embed fragment text.
- Code favors small modules and readable control flow over abstraction-heavy patterns.

## Project Structure

```text
client/
core/
coordinator/
aggregator/
node/
test/
```

## Run The MVP

Install dependencies:

```powershell
npm install
```

### Local processes

Start two node instances in separate terminals:

```powershell
$env:PORT=4001
$env:NODE_ID="node-a"
$env:NODE_DATASET="shard-a"
npm run start:node
```

```powershell
$env:PORT=4002
$env:NODE_ID="node-b"
$env:NODE_DATASET="shard-b"
npm run start:node
```

Start the aggregator:

```powershell
$env:PORT=4003
npm run start:aggregator
```

Start the coordinator:

```powershell
$env:PORT=4000
$env:NODE_URLS="http://localhost:4001,http://localhost:4002"
$env:AGGREGATOR_URL="http://localhost:4003"
npm run start:coordinator
```

Start the client API:

```powershell
$env:PORT=3000
$env:COORDINATOR_URL="http://localhost:4000"
npm run start:client
```

Run a search through the client API:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:3000/search" `
  -ContentType "application/json" `
  -Body '{"query":"private compute search","limit":5}'
```

Or run the CLI client:

```powershell
$env:COORDINATOR_URL="http://localhost:4000"
npm run start:client:cli -- "private compute search"
```

### Docker Compose

Start the full stack with one command:

```powershell
docker compose up --build
```

Then send a request to the client service:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:3000/search" `
  -ContentType "application/json" `
  -Body '{"query":"private compute search","limit":5}'
```

Stop the stack:

```powershell
docker compose down
```

## API Overview

### `POST /search` on client

Request:

```json
{
  "query": "private compute search",
  "limit": 5
}
```

Response:

```json
{
  "jobId": "job-opaque-id",
  "fragmentsProcessed": 3,
  "fragments": ["private", "compute", "search"],
  "results": [
    {
      "documentId": "doc-1",
      "title": "Private compute overview",
      "score": 5,
      "matchedFragments": ["private", "compute"]
    }
  ]
}
```

## Notes

- This MVP focuses on distributed search behavior, not cryptographic privacy guarantees.
- The coordinator intentionally avoids logging or persisting request bodies.
- The client-side splitting step is where the raw query is handled.
- The coordinator can use the aggregator API when `AGGREGATOR_URL` is set, or fall back to local merging when it is not.
- The coordinator now uses opaque job and task identifiers so metadata does not reveal fragment text.
=======
# Privon — Private Compute Network (PCN)

Privon is a privacy-first distributed compute network that splits tasks across multiple nodes to prevent full data exposure and ensure secure processing.

---

## 🚀 Overview

Privon (PCN) is an experimental infrastructure project designed to:

- Protect user data during computation
- Distribute workloads across independent nodes
- Avoid centralized data processing
- Enable privacy-preserving compute systems

Instead of sending full data to a single server, Privon:

1. Splits data into fragments  
2. Distributes fragments across nodes  
3. Processes them independently  
4. Merges results into a final output  

---

## 🧠 Why Privon?

Traditional systems:
- Centralized ❌
- Data exposed ❌
- Privacy risk ❌

Privon:
- Distributed ✅
- Fragmented data ✅
- Privacy-first design ✅

---

## 🏗️ Architecture

>>>>>>> 3ab3de59642e2bce1671cb1a01d8a9a8e76b92b1
