# ─────────────────────────────────────────────────────────────
# schema.py — strict typed graph schema (Beta 2.0 closed set, 9 Aug 26)
# Slice 2: term-anchored action verbs; IP family (IP/PATENT/TRADEMARK/
# INVENTOR) added; EXCLUSIVITY_CLAUSE folds into CONTRACT_TERM (a mode);
# GEOGRAPHIC_REGION dropped (jurisdiction = meta on CONTRACT/PATENT).
# ─────────────────────────────────────────────────────────────
from typing import Literal

from pydantic import BaseModel, Field

# Strict, upper-case Entity labels for Neo4j
ENTITY_TYPES = Literal[
    "TICKER",  # the tracked anchor company (our ingest universe)
    "SUBSIDIARY",  # corporate subsidiary (EX-21 list or family match)
    "COMPANY",  # partner / supplier / client — any other company
    "PERSON",  # named natural person materially relevant to a contractual fact
    "CONTRACT",  # material contract (EX-2/EX-10); kind: materials/product/IP-rights
    "DEAL_EVENT",  # announced deal w/ no contract on file (Item 1.01 / EX-99.1 / 20-F MC)
    "CONTRACT_TERM",  # query-worthy clause: money, exclusivity, restrictions
    "PRODUCT",  # the branded, named thing sold/bought in the contract
    "TECHNOLOGY",  # patented/patentable tech / IP the contract covers
    "IP",  # generic contract-side IP (patent/trademark/copyright) — folds to
    #        PATENT/TRADEMARK when the kind or number resolves
    "PATENT",  # granted patent / application (contract-minted or USPTO leg)
    "PATENT_INVENTORY",  # ONE portfolio node per ticker — every in-window
    #                  filing whose family was not featured (USPTO leg only)
    "TRADEMARK",  # trademark (contract-minted or TM leg)
    "INVENTOR",  # named inventor on a patent
    "FEDERAL_AWARD",  # federal contract/grant award (USASpending leg — kind meta: contract|grant)
    "AWARD_INVENTORY",  # ONE portfolio node per ticker — every non-featured award (USASpending leg)
]

# What the LLM may emit (v3 closed set, 9 Aug 26): the 6 contract-leg types.
# Everything else in ENTITY_TYPES is deterministic / USPTO / evidence-layer
# work — TICKER/SUBSIDIARY/CONTRACT are pipeline-built, PATENT/TRADEMARK/
# INVENTOR come from the USPTO leg. The extractor's schema uses THIS set.
LLM_ENTITY_TYPES = Literal[
    "COMPANY",
    "PERSON",
    "CONTRACT_TERM",
    "PRODUCT",
    "TECHNOLOGY",
    "IP",
]

# The 7 term-anchored action verbs the LLM may emit (v3, 9 Aug 26).
LLM_RELATION_TYPES = Literal[
    "GRANTS",
    "RESTRICTS",
    "BUYS",
    "SELLS",
    "OWNS",
    "PLEDGES",
    "OBLIGATES",
]

# Strict Relationship types connecting the entities
RELATION_TYPES = Literal[
    "HAS_CONTRACT",  # (TICKER|COMPANY|SUBSIDIARY)-[:HAS_CONTRACT]->(CONTRACT)
    "HAS_DEAL",  # (TICKER)-[:HAS_DEAL]->(DEAL_EVENT) — announced deal, no contract
    "SUBSIDIARY_OF",  # (SUBSIDIARY)-[:SUBSIDIARY_OF]->(TICKER|COMPANY)
    "HAS_TERM",  # (CONTRACT|DEAL_EVENT)-[:HAS_TERM]->(CONTRACT_TERM)
    "COVERS",  # (CONTRACT|DEAL_EVENT)-[:COVERS]->(PRODUCT | TECHNOLOGY) · (FEDERAL_AWARD)-[:COVERS]->(PRODUCT) — USASpending leg
    # Term action verbs — the term is the hub; entities branch off it.
    "GRANTS",  # gives a right/benefit (license, exclusivity, allowance)
    "RESTRICTS",  # limits (caps, non-compete, minimum liquidity)
    "BUYS",  # purchases (goods, materials, wafers…)
    "SELLS",  # sells (the counterparty view of a purchase)
    "OWNS",  # owns (IP, patents, trademarks, products)
    "PLEDGES",  # collateral / security interest / lien
    "OBLIGATES",  # imposes a duty (deliver, pay, maintain, guarantee, indemnify)
    "INVENTED_BY",  # (PATENT)-[:INVENTED_BY]->(INVENTOR)
    "LEVERAGES_TECH",  # (PATENT)-[:LEVERAGES_TECH]->(TECHNOLOGY)
    "HAS_PATENT",  # (TICKER|SUBSIDIARY|COMPANY)-[:HAS_PATENT]->(PATENT) — USPTO leg
    "HAS_PATENT_INVENTORY",  # (TICKER)-[:HAS_PATENT_INVENTORY]->(PATENT_INVENTORY) — USPTO leg
    "HAS_AWARD",  # (TICKER|SUBSIDIARY)-[:HAS_AWARD]->(FEDERAL_AWARD) — USASpending leg
    "HAS_AWARD_INVENTORY",  # (TICKER)-[:HAS_AWARD_INVENTORY]->(AWARD_INVENTORY) — USASpending leg
    "AWARDED_BY",  # (FEDERAL_AWARD)-[:AWARDED_BY]->(COMPANY) — the prime on a subaward (USASpending leg, a3)
]

# Validation Schema defining allowed connections (backwards-compat dict format:
# a relation passes if it's allowed from EITHER endpoint, so direction-flexible
# LLM output validates). The term-anchored model: CONTRACT -HAS_TERM-> TERM and
# TERM -[verb]-> entity. (9 Aug 26: closed Beta 2.0 set — HAS_TERM no longer
# hangs off COMPANIES; IN_JURISDICTION/GEOGRAPHIC_REGION are gone.)
KG_VALIDATION_SCHEMA = {
    "TICKER": ["HAS_CONTRACT", "HAS_DEAL", "HAS_PATENT", "HAS_PATENT_INVENTORY",
               "HAS_AWARD", "HAS_AWARD_INVENTORY"],
    "SUBSIDIARY": ["SUBSIDIARY_OF", "HAS_CONTRACT", "HAS_PATENT", "HAS_AWARD",
                   "GRANTS", "RESTRICTS", "BUYS", "SELLS", "OWNS", "PLEDGES", "OBLIGATES"],
    "COMPANY": ["HAS_CONTRACT", "HAS_PATENT",
                "GRANTS", "RESTRICTS", "BUYS", "SELLS", "OWNS", "PLEDGES", "OBLIGATES"],
    "PERSON": ["GRANTS", "RESTRICTS", "BUYS", "SELLS", "OWNS", "PLEDGES", "OBLIGATES"],
    "CONTRACT": ["HAS_TERM", "COVERS"],
    "DEAL_EVENT": ["HAS_TERM", "COVERS"],
    "CONTRACT_TERM": ["HAS_TERM", "GRANTS", "RESTRICTS", "BUYS", "SELLS",
                       "OWNS", "PLEDGES", "OBLIGATES"],
    "PRODUCT": ["GRANTS", "RESTRICTS", "BUYS", "SELLS", "OWNS", "PLEDGES", "OBLIGATES"],
    "TECHNOLOGY": ["GRANTS", "RESTRICTS", "BUYS", "SELLS", "OWNS", "PLEDGES",
                    "OBLIGATES", "LEVERAGES_TECH"],
    "IP": ["GRANTS", "RESTRICTS", "BUYS", "SELLS", "OWNS", "PLEDGES", "OBLIGATES"],
    "PATENT": ["INVENTED_BY", "LEVERAGES_TECH", "HAS_PATENT"],
    "PATENT_INVENTORY": ["HAS_PATENT_INVENTORY"],
    "TRADEMARK": [],
    "INVENTOR": [],
    "FEDERAL_AWARD": ["HAS_AWARD", "COVERS", "AWARDED_BY"],
    "AWARD_INVENTORY": ["HAS_AWARD_INVENTORY"],
}

# LLM-facing validation (v3): the 6 closed entities, term-anchored verbs only.
# The full KG_VALIDATION_SCHEMA stays for the fold/sweep, which may also
# handle deterministic edges (HAS_CONTRACT etc.) built outside the LLM.
LLM_VALIDATION_SCHEMA = {
    t: list(LLM_RELATION_TYPES.__args__) for t in LLM_ENTITY_TYPES.__args__
}

# EXACT (S,R,O) combo list for the extractor's strict validation (9 Aug 26):
# the dict format above is direction-flexible (a relation passes if allowed
# from EITHER endpoint), so COMPANY-[GRANTS]->PRODUCT slipped through. This
# exact list forces the term-anchored model — every LLM edge starts at a
# CONTRACT_TERM. Deterministic edges (HAS_CONTRACT, SUBSIDIARY_OF, HAS_TERM,
# COVERS, MENTIONS) are built by the fold, never by the LLM.
_LLM_VERBS = list(LLM_RELATION_TYPES.__args__)
_LLM_ENTITY_SET = list(LLM_ENTITY_TYPES.__args__)
LLM_RELATIONSHIP_COMBOS: list[tuple[str, str, str]] = [
    ("CONTRACT_TERM", verb, ent)
    for verb in _LLM_VERBS
    for ent in _LLM_ENTITY_SET
]


class SECContractMetadata(BaseModel):
    """Extracted metadata payload attached to every processed contract node."""

    cik: str = Field(description="SEC Central Index Key")
    company_name: str = Field(description="Filing company name")
    filing_date: str = Field(description="YYYY-MM-DD date of SEC filing")
    accession_number: str = Field(description="SEC unique filing identifier")
    exhibit_type: str = Field(
        default="EX-10.1", description="SEC Exhibit classification"
    )
    source_form: str = Field(
        default="8-K", description="Filing form: 8-K/6-K/10-K/20-F"
    )
    source_url: str = Field(description="Direct URL to SEC EDGAR .htm file")
