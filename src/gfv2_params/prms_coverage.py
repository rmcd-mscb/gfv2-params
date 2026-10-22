"""Which PRMS parameters this pipeline covers, rendered for the README.

Pure: the data dictionary (`docs/HI_paramDB_DataDictionary.csv`) plus the export plan
(`gfv2_params.prms_export.plan_exports`) in, markdown out. Nothing here is hand-typed per
parameter -- the covered set is whatever the export plans, and every uncovered parameter is
grouped by a rule over the dictionary's own `Dimensions`, `Source` and `Description`
columns, so the list cannot drift from the configs or the dictionary.

"Covered" means DECLARED by the configs and exported when the `export_prms` stage has been
run on a fabric. It does not mean built on any particular fabric: the YAML cannot tell
`lulc_nlcd` (declared, never built) from `lulc_nhm_v11`. Per-fabric truth is the
`params/prms/` directory plus the Guard 4 job id.
"""

from __future__ import annotations

import re

from gfv2_params.prms_export import DIM_NDEPLVAL, ExportItem

# Ordered: first matching rule wins. The title is the README heading for that group.
RUN_CONTROLS = "Run controls and unit flags"
SCALARS = "Scalar constants"
MONTHLY = "Monthly parameters (nhru × nmonths)"
SEGMENT = "Stream-segment network"
GAGE = "Gages / points of interest"
HRU_CONSTANTS = "Per-HRU constants (NHM defaults)"
FABRIC_ATTRS = "Fabric attributes (geometry, topology, identity)"
NOT_YET = "Derived from spatial layers, not yet produced here"
UNCLASSIFIED = "UNCLASSIFIED"

GROUP_ORDER = (
    HRU_CONSTANTS, MONTHLY, NOT_YET, FABRIC_ATTRS, SEGMENT, GAGE, SCALARS, RUN_CONTROLS, UNCLASSIFIED,
)

_CONTROL_WORDS = re.compile(r"\b(flag|units|station)\b", re.I)


def classify(row: dict[str, str]) -> str:
    """The group for an UNCOVERED dictionary row. First match wins; see module docstring."""
    dim = row["Dimensions"].strip()
    source = row["Source"].strip()
    if "nmonths" in dim:
        return MONTHLY
    if dim == "nsegment":
        return SEGMENT
    if dim == "npoigages":
        return GAGE
    if dim == "one":
        return RUN_CONTROLS if _CONTROL_WORDS.search(row["Description"]) else SCALARS
    if dim == "nhru":
        if source == "Regan":
            return HRU_CONSTANTS
        if source in ("GV1.1", "HI"):
            return FABRIC_ATTRS
        return NOT_YET
    return UNCLASSIFIED


def _cell(text: str) -> str:
    """One markdown table cell: no pipes, no newlines."""
    return " ".join(str(text).split()).replace("|", "\\|")


def covered_names(items: list[ExportItem]) -> set[str]:
    return {i.prms_name for i in items}


def _written_from(item: ExportItem) -> str:
    if item.dimension == DIM_NDEPLVAL:
        return "snarea curve library (flat; not per-HRU)"
    cols = " / ".join(f"`{c}`" for c in item.source_cols)
    return f"`{item.merged_file}` ({cols})"


def render_covered(items: list[ExportItem], dictionary: dict[str, dict[str, str]]) -> str:
    lines = [
        "| Parameter | Type | Units | Dimension | Description | Written from |",
        "|---|---|---|---|---|---|",
    ]
    for item in sorted(items, key=lambda i: i.prms_name):
        row = dictionary[item.prms_name]
        lines.append(
            f"| `{item.prms_name}` | {_cell(row['Type'])} | {_cell(row['Units'])} | "
            f"{_cell(row['Dimensions'])} | {_cell(row['Description'])} | {_written_from(item)} |"
        )
    return "\n".join(lines)


def group_uncovered(
    items: list[ExportItem], dictionary: dict[str, dict[str, str]]
) -> dict[str, list[dict[str, str]]]:
    covered = covered_names(items)
    groups: dict[str, list[dict[str, str]]] = {g: [] for g in GROUP_ORDER}
    for name, row in dictionary.items():
        if name not in covered:
            groups[classify(row)].append(row)
    return {g: sorted(rows, key=lambda r: r["Parameter"]) for g, rows in groups.items() if rows}


def render_not_covered(items: list[ExportItem], dictionary: dict[str, dict[str, str]]) -> str:
    out: list[str] = []
    for group, rows in group_uncovered(items, dictionary).items():
        out += [f"**{group}** ({len(rows)})", "", "| Parameter | Units | Dimension | Description |", "|---|---|---|---|"]
        out += [
            f"| `{r['Parameter']}` | {_cell(r['Units'])} | {_cell(r['Dimensions'])} | {_cell(r['Description'])} |"
            for r in rows
        ]
        out.append("")
    return "\n".join(out).rstrip()


def render_summary(items: list[ExportItem], dictionary: dict[str, dict[str, str]]) -> str:
    """One generated line so the counts in the prose cannot go stale."""
    n_cov = len(covered_names(items))
    return (
        f"**{n_cov} of the {len(dictionary)} parameters in the data dictionary are covered**; "
        f"{len(dictionary) - n_cov} are not."
    )
