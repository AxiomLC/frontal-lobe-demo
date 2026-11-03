# ===============  writer.py - Neo4j read/write primitives for the demo  ===============
# A deliberately simplified, portable writer compared to the full Frontal Lobe
# pipeline:
#   - batched UNWIND upserts, all plain Cypher - NO Neo4j-specific procedures
#     (no apoc.*, no db.create.setNodeVectorProperty) - so the same code ports
#     cleanly to FalkorDB / Memgraph later.
#   - entities are MERGE'd by id and re-labelled idempotently (SET e:Label)
#   - relationships are grouped by their (closed-set) label and MERGE'd, so no
#     dynamic labels are needed in the pattern.
#   - embeddings are NOT part of this slice - the demo leg is keyless and
#     deterministic with nothing but a Neo4j connection. The full app adds a
#     vector index and embeddings.
# The cross-leg canonical merge the full app runs after every write is NOT
# here - see README's "Note on the writer".
import re

# ---- canonical-name normalisation (dedup key for the fold ladder) ----
CANONICAL_SUFFIXES = [
    "corp", "corporation", "inc", "incorporated", "llc", "ltd", "limited",
    "co", "company", "plc", "gmbh", "ag", "sa",
]


def canonical_name(name):
    """Normalize an entity name for dedup: lowercase, strip punctuation, drop
    legal suffixes from short names. A trailing parenthetical that is a
    hyphen/slash acronym token (C-DAC) is stripped; plain-word parentheticals
    (Beijing, Canada Branch) are geographic/branch tags and stay."""
    nm = (name or "").strip()
    m = re.match(r"^(.*?)\s*\(([^()]*)\)\s*$", nm)
    if m:
        alias = m.group(2).strip()
        is_alias = len(alias) >= 2 and ("-" in alias or "/" in alias)
        if is_alias and len(m.group(1).split()) >= 2:
            nm = m.group(1)
    words = [w for w in re.sub(r"[^a-z0-9 ]", " ", nm.strip().lower()).split() if w]
    if 1 < len(words) <= 3 and words[-1] in CANONICAL_SUFFIXES:
        words = words[:-1]
    return " ".join(words)


# ---- snapshot ------------------------------------------------------------
def snapshot(session):
    """Pull the existing graph into memory - the fold ladder's match targets.
    Entries: {id: {id, name, labels, props}} (embedding never pulled)."""
    entities = {}
    for r in session.run(
        "MATCH (n:__Entity__) RETURN n.id AS id, n.name AS name, "
        "labels(n) AS labels, properties(n) AS props"
    ):
        props = dict(r["props"])
        props.pop("embedding", None)
        entities[r["id"]] = {"id": r["id"], "name": r["name"], "labels": r["labels"], "props": props}
    return {"entities": entities}

# ---- one-transaction write ----------------------------------------------
_CHUNK_UPSERT = (
    "UNWIND $data AS row "
    "MERGE (c:__Node__ {id: row.id}) "
    "SET c:Chunk, c.text = row.text, c += row.props "
    "RETURN count(*)"
)


def _entity_upsert_with_label(label):
    # label comes from a closed set at build time - safe to inline.
    return (
        "UNWIND $data AS row "
        "MERGE (e:__Node__ {id: row.id}) "
        "SET e:__Entity__, e:" + label + ", e += row.props, e.name = row.name "
        "RETURN count(*)"
    )


def _rel_upsert_with_label(label):
    return (
        "UNWIND $data AS row "
        "MERGE (s:__Node__ {id: row.source_id}) "
        "MERGE (t:__Node__ {id: row.target_id}) "
        "MERGE (s)-[r:" + label + "]->(t) "
        "SET r += row.props "
        "RETURN count(*)"
    )


def write_plan(session, plan, recipe):
    """Write one ticker plan in a single transaction: chunks -> entities
    (grouped by label) -> relationships (grouped by label). Returns counts."""
    chunks = plan.get("chunks") or []
    entities = plan.get("entities") or {}
    rels = plan.get("relations") or []

    by_label = {}
    for e in entities.values():
        by_label.setdefault(e["label"], []).append(e)

    rel_by_label = {}
    for r in rels:
        rel_by_label.setdefault(r["label"], []).append(r)

    with session.begin_transaction() as tx:
        if chunks:
            tx.run(
                _CHUNK_UPSERT,
                data=[{"id": c["id"], "text": c["text"], "props": c["props"]} for c in chunks],
            )
        for label, rows in by_label.items():
            tx.run(
                _entity_upsert_with_label(label),
                data=[{"id": e["id"], "name": e["name"], "props": e["props"]} for e in rows],
            )
        for label, rows in rel_by_label.items():
            tx.run(
                _rel_upsert_with_label(label),
                data=[
                    {"source_id": r["source_id"], "target_id": r["target_id"], "props": r.get("props") or {}}
                    for r in rows
                ],
            )
    return {"chunks": len(chunks), "entities": len(entities), "relations": len(rels)}

# ---- idempotency + stale cleanup ----------------------------------------
def has_stamp(session, ticker, recipe):
    """Any node for this ticker at the current recipe stamp -> re-running the
    leg writes nothing (idempotency). Bump the recipe to force a re-fetch."""
    row = session.run(
        "MATCH (n) WHERE n.ticker = $t AND n.award_recipe_version = $ver "
        "RETURN count(n) AS c",
        t=ticker, ver=recipe,
    ).single()
    return bool(row and int(row["c"]) > 0)


def stale_cleanup(session, tickers, recipe):
    """Drop any award-leg node/chunk still stamped with an OLDER recipe. Runs
    AFTER a successful write so a failed run never loses the previous data."""
    for t in tickers:
        session.run(
            "MATCH (n) WHERE n.ticker = $t AND "
            "(n:FEDERAL_AWARD OR n:AWARD_INVENTORY OR n:SUBSIDIARY "
            " OR (n:PRODUCT AND n.source = 'USASpending') "
            " OR (n:COMPANY AND n.source = 'USASpending')) "
            "AND n.award_recipe_version <> $ver "
            "DETACH DELETE n",
            t=t, ver=recipe,
        )
        session.run(
            "MATCH (c:Chunk) WHERE c.ticker = $t AND c.source = 'USASpending' "
            "AND c.award_recipe_version <> $ver DELETE c",
            t=t, ver=recipe,
        )


# ---- reads for the ops console -------------------------------------------
def per_label_counts(session):
    """Node and relationship counts per label, excluding the keepalive
    heartbeat node, plus the totals for the usage meter."""
    labels = {}
    rels = {}
    for r in session.run(
        "MATCH (n) WHERE NOT n:_Heartbeat RETURN labels(n) AS lbls"
    ):
        for lab in r["lbls"]:
            labels[lab] = labels.get(lab, 0) + 1
    for r in session.run(
        "MATCH ()-[rel]->() "
        "WHERE NOT startNode(rel):_Heartbeat AND NOT endNode(rel):_Heartbeat "
        "RETURN type(rel) AS t"
    ):
        rels[r["t"]] = rels.get(r["t"], 0) + 1
    return {
        "node_labels": labels,
        "rel_labels": rels,
        "nodes_total": sum(labels.values()),
        "rels_total": sum(rels.values()),
    }


def graph_subgraph(session):
    """Entities + edges for the 3D map - no Chunk/MENTIONS internals, no
    heartbeat. Returns {nodes, links} for the UI."""
    nodes = []
    for r in session.run(
        "MATCH (n:__Entity__) WHERE NOT n:_Heartbeat "
        "RETURN n.id AS id, n.name AS name, labels(n) AS labels, "
        "n.ticker AS ticker, n.summary AS summary"
    ):
        aids = [l for l in (r["labels"] or []) if l not in ("__Entity__", "__Node__")]
        primary = aids[0] if aids else "NODE"
        nodes.append({
            "id": r["id"], "name": r["name"], "type": primary,
            "ticker": r["ticker"] or "", "summary": r["summary"] or "",
        })
    links = []
    for r in session.run(
        "MATCH (a:__Entity__)-[rel]->(b:__Entity__) "
        "WHERE NOT a:_Heartbeat AND NOT b:_Heartbeat "
        "RETURN a.id AS s, b.id AS t, type(rel) AS type"
    ):
        links.append({"source": r["s"], "target": r["t"], "type": r["type"]})
    return {"nodes": nodes, "links": links}


def clear_all(session):
    """Full reset - wipes the entire graph."""
    with session.begin_transaction() as tx:
        tx.run("MATCH (n) DETACH DELETE n")
    return {"status": "ok"}
