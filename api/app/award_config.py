# ===============  AWARD CONFIG - the ONLY tuning file for the USASpending leg  ===============
# Every knob that shapes the federal-award graph lives here: change a number ->
# restart the app -> done.
# Recipe version: a3 - see AWARD_RECIPE_VERSION at the bottom.
# RULE: change anything that alters what gets fetched/folded (window, cap,
# floor, name-keys, aliases) -> bump AWARD_RECIPE_VERSION -> the leg re-fetches
# on the next run (idempotency is per-stamp).
# The award leg is FULLY DETERMINISTIC - no LLM anywhere in fetch/rank/build
# (rank = obligation $; the award description IS the summary). The only
# "smart" step is UEI resolution (recipient search against the keyless
# USASpending API) - deterministic, verified live at runtime.

# ===============  1. Fetch  ===============
# 5-year window, default. Filter semantics: date_type="date_signed" (the API
# default pulls in stale awards - a 2015 contract appeared in the 5y probe).
AWARD_FETCH_WINDOW_DAYS = 1826          # 5 years

# The featured-node cap per TICKER: 4 FEDERAL_AWARD nodes + 1 AWARD_INVENTORY
# node per ticker - the same budget as the patent leg (4+1).
MAX_AWARDS_PER_TICKER = 4

# The award floor: $500K - catches SBIR Phase II + mid-size agency R&D; still
# filters the $500 placeholder rows. Applied to the search "Award Amount"
# (total obligation) before ranking.
AWARD_MIN_AMOUNT = 500_000

# The SUBAWARD floor: $100K - a subaward is a SLICE of a big prime contract,
# so the $500K prime floor would filter them all out. The sub-awardee is OUR
# ticker/Subs under a prime contractor (BAE, GD, Raytheon...) - the
# cross-cluster hook. Applied to the NET subaward amount (sum of actions per
# Sub-Award ID - one subaward = many action rows incl. negatives).
SUBAWARD_MIN_AMOUNT = 100_000

# Subaward fetch knobs - same pagination/safety shapes as the prime fetch.
SUBAWARD_MAX_RECORDS_PER_UEI = 300   # safety bound: subaward rows span many
#                                      # years (the API date filter is unreliable
#                                      # for subs - the leg post-filters by
#                                      # Sub-Award Date in Python)

# Award type groups - the API requires ONE group per call.
AWARD_TYPE_CONTRACTS = ["A", "B", "C", "D"]        # BPA call, PO, delivery order, definitive
AWARD_TYPE_GRANTS = ["02", "03", "04", "05", "F001", "F002"]  # block/formula/project/coop + F-series

# Pagination / safety / timing
AWARD_PAGE_SIZE = 100                # per page (API max)
AWARD_MAX_RECORDS_PER_UEI = 200      # safety bound on rows per UEI (pagination guard)
AWARD_API_TIMEOUT_S = 60
AWARD_FETCH_WORKERS = 4              # per-ticker detail fan-out threads
AWARD_DETAIL_PARALLEL = True         # fetch award detail for featured rows in parallel

# ===============  2. AWARD_INVENTORY portfolio chunk  ===============
# One AWARD_INVENTORY node per ticker: every in-window award NOT featured as a
# FEDERAL_AWARD node (incl. recipients that fold nowhere), ranked $ desc.
# Line cap keeps the chunk readable.
AWARD_INVENTORY_MAX_LINES = 200

# ===============  3. Per-ticker names + SAM aliases  ===============
# `names`  - the recipient-search seeds (recipient search is a CONTAINS match;
#           no wildcards needed - UEI-keyed search is the clean path).
# `aliases`- {canonical recipient name: fold target}, fired ONLY when the
#           recipient does not exact-match an existing DB entity:
#              "ticker" -> attach awards directly to the ticker anchor (the SAM
#                         legal name IS the parent under a different name -
#                         "Infleqtion Quantum, LLC", "Rigetti & Co, LLC",
#                         "LanzaTech, Inc.")
#              "sub"    -> mint a NEW SUBSIDIARY under the ticker
#                         (a real SAM-registered sub - "LanzaTech New Zealand")
# Anything else (family-prefix match vs the DB) -> NEW-SUB under that parent.
AWARD_NAME_KEYS = {
    "QUBT": {
        "names": ["Quantum Computing Inc"],
        "aliases": {},
    },
    "INFQ": {
        "names": ["Infleqtion", "Infleqtion Quantum", "ColdQuanta"],
        "aliases": {"infleqtion quantum": "ticker", "coldquanta": "ticker"},
    },
    "IONQ": {
        "names": ["IonQ"],
        "aliases": {"ionq": "ticker"},
    },
    "RGTI": {
        "names": ["Rigetti", "Rigetti & Co"],
        "aliases": {"rigetti & co": "ticker"},
    },
    "QBTS": {
        "names": ["D-Wave"],
        "aliases": {"d wave": "ticker", "d wave systems": "ticker"},
    },
    "ASML": {
        "names": ["ASML", "ASML Netherlands"],
        "aliases": {"asml": "ticker", "asml netherlands": "ticker"},
    },
    "POET": {
        "names": ["POET Technologies"],
        "aliases": {"poet technologies": "ticker"},
    },
    "ALNT": {
        "names": ["Allied Motion", "Allient"],
        "aliases": {"allied motion": "ticker", "allient": "ticker"},
    },
    "CLS": {
        "names": ["Celestica"],
        "aliases": {},
    },
    "LNZA": {
        "names": ["LanzaTech", "LanzaTech, Inc.", "LanzaTech New Zealand"],
        "aliases": {
            "lanzatech": "ticker",
            "lanzatech inc": "ticker",
            "lanzatech new zealand": "sub",
        },
    },
    "LWLG": {
        "names": ["Lightwave Logic"],
        "aliases": {"lightwave logic": "ticker"},
    },
}

# Agency full-name -> short caption abbreviation. Fallback in the leg: first
# significant words of the agency name.
AGENCY_ABBREV = {
    "Department of Defense": "DOD",
    "Department of the Air Force": "USAF",
    "Department of the Navy": "USN",
    "Department of the Army": "USA",
    "Department of Energy": "DOE",
    "Department of Homeland Security": "DHS",
    "Department of Justice": "DOJ",
    "Department of Commerce": "DOC",
    "Department of State": "DOS",
    "Department of Transportation": "DOT",
    "National Aeronautics and Space Administration": "NASA",
    "National Science Foundation": "NSF",
    "General Services Administration": "GSA",
    "Department of Health and Human Services": "HHS",
    "Department of Agriculture": "USDA",
    "Department of the Interior": "DOI",
    "Department of Labor": "DOL",
    "Department of Education": "ED",
    "Department of Veterans Affairs": "VA",
    "Department of the Treasury": "TREAS",
    "Environmental Protection Agency": "EPA",
    "National Institutes of Health": "NIH",
    "Defense Advanced Research Projects Agency": "DARPA",
    "Small Business Administration": "SBA",
}

# ===============  VERSION  ===============
# a1: first build. Fully deterministic: name keys + runtime recipient
#   resolution -> per-UEI search (contracts + grants groups, 5-yr window,
#   date_signed) -> $500K floor -> dedup by award id -> rank by obligation $ ->
#   top-4 featured FEDERAL_AWARD nodes (detail call fills description/type/
#   agency chain) -> AWARD_INVENTORY portfolio node (all non-featured) ->
#   fold ladder (exact -> SAM alias -> family-prefix -> inventory, never COMPANY)
#   -> one-tx write.
# a2: floor dropped $1M -> $500K - SBIR Phase II / mid-size agency R&D now
#   qualify.
# a3: SUBAWARDS - same leg, new fetch path (top-level "subawards": True;
#   hyphenated Sub-* fields; NO sort, NO date_type; python-side Sub-Award Date
#   window filter; net-sum aggregation per Sub-Award ID). `role` meta
#   (prime | subcontract); PRODUCT node per featured award (COVERS, junk-gated
#   description); COMPANY (prime) folded-or-minted per subcontract award
#   (AWARDED_BY) - the cross-cluster hook; SUBAWARD_MIN_AMOUNT $100K;
#   stale-stamp cleanup covers award-minted PRODUCT/COMPANY (guarded by
#   source='USASpending').
AWARD_RECIPE_VERSION = "a3"  # bump to force a full re-fetch of the leg
