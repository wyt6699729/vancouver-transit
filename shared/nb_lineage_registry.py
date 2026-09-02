"""Lineage registry — the ONLY acceptable lineage tracking method.

Never track lineage in a database table or a wiki. Every new table gets an
entry here, keyed by {domain}_{layer}.{table}.

Owner: Etienne Wang (etienne.wang@slalom.com)
Updated: 2026-09-01
"""

# TODO (Lab 9): populate from the LINEAGE spec in CLAUDE.md. Required keys per
# entry: display_name, layer, sources, source_type, feeds, grain, owner,
# notebook, sla_minutes, pii_columns, column_lineage, tags.
LINEAGE = {}
