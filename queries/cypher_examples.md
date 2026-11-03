# Example Cypher queries (read-only)

These mirror the README's starter queries and the app's Example-queries panel.
All are read-only — writes are blocked by the demo server.

## 1. Top federal awards for a company
```cypher
MATCH (t:TICKER {ticker: 'IONQ'})-[:HAS_AWARD]->(a:FEDERAL_AWARD)
RETURN a.name, a.role, a.kind, a.amount, a.agency_toptier
ORDER BY a.amount DESC
```
Best for: seeing what the government paid a company, and whether each award was
a prime award (`role: prime`) or a slice of someone else's contract
(`role: subcontract`).

## 2. What products do the awards cover?
```cypher
MATCH (t:TICKER)-[:HAS_AWARD]->(a:FEDERAL_AWARD)-[:COVERS]->(p:PRODUCT)
RETURN t.ticker, p.name, a.amount
ORDER BY a.amount DESC LIMIT 25
```
Best for: the named things (often the program/project titles) behind the money.

## 3. Prime contractors a company works under (subcontracts)
```cypher
MATCH (t:TICKER {ticker: 'INFQ'})-[:HAS_AWARD]->(a:FEDERAL_AWARD {role: 'subcontract'})
      -[:AWARDED_BY]->(prime:COMPANY)
RETURN prime.name, count(a) AS awards, sum(a.amount) AS total
ORDER BY total DESC
```
Best for: the cross-cluster hook — when your ticker is a sub-awardee, which
large prime contractor sits above it. (INFQ folds a real BAE Systems subaward
into its AWARD_INVENTORY.)

## 4. Whole graph, without the internal chunks
```cypher
MATCH (n) WHERE NOT n:Chunk AND NOT n:_Heartbeat
OPTIONAL MATCH (n)-[r]->(m) WHERE NOT m:Chunk AND NOT m:_Heartbeat
RETURN n, r, m
```
Best for: the full graph in one pass (the keepalive heartbeat node is excluded).

---

### Node & edge vocabulary
Nodes: `TICKER · FEDERAL_AWARD · PRODUCT · COMPANY · AWARD_INVENTORY · SUBSIDIARY`
Edges: `HAS_AWARD · HAS_AWARD_INVENTORY · COVERS · AWARDED_BY · SUBSIDIARY_OF`
