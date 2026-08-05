"""Render the three machine-derivable views of `docs/parameter_index.md` from `configs/`.

Reads `gfv2_params.params_index` -- pure YAML, no data root, no geo imports -- and
rewrites three MARKER-DELIMITED regions in `docs/parameter_index.md`:

    <!-- BEGIN GENERATED: by-process -->  ...  <!-- END GENERATED: by-process -->
    <!-- BEGIN GENERATED: by-entry -->    ...  <!-- END GENERATED: by-entry -->
    <!-- BEGIN GENERATED: by-builder --> ...  <!-- END GENERATED: by-builder -->

Everything outside those markers -- the preamble, "Reading this index", the notes
under each view, "Known gaps", "See also" -- is HAND-MAINTAINED and never touched.
That split is deliberate: the tables are a mechanical projection of the `prms:`
blocks and go stale the moment a config changes, but the prose (the TEXT_PRMS.tif
metadata investigation, the aspect measurement, the 57x worked example) is
hydrologist-reviewed judgement a generator cannot reproduce and must not clobber.

Config-entry line numbers are COMPUTED, not carried: adding a `prms:` block shifts
every entry below it, so any literal `zonal_params.yml:81` in the doc would be
wrong the moment this metadata landed.

Usage:
    python scripts/build_parameter_index.py            # rewrite in place
    python scripts/build_parameter_index.py --check    # exit 1 if it would change
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

from gfv2_params.params_index import (
    DEPSTOR_PARAMS_CONFIG,
    SNAREA_LIBRARY_CONFIG,
    ZONAL_PARAMS_CONFIG,
    DeclaredParam,
    load_declared_params,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
INDEX_PATH = _REPO_ROOT / "docs" / "parameter_index.md"

# Which top-level list of each config `iter_declared_params` draws entries from.
# `fractions:` is excluded there, and must be excluded here too: `dprst_frac` is a
# name in BOTH `fractions:` (a count CSV in merged/_intermediates/) and `ratios:`
# (the PRMS parameter), and a naive scan for `- name: dprst_frac` finds the wrong
# one -- it appears first.
_ENTRY_SECTIONS = {
    ZONAL_PARAMS_CONFIG: ("params",),
    DEPSTOR_PARAMS_CONFIG: ("means", "ratios", "constants"),
}


def _entry_locations() -> dict[str, str]:
    """Map entry name -> "<config-basename>:<line>", scanning the raw YAML text.

    A text scan rather than a YAML parse because PyYAML discards line numbers, and
    the whole point of the cite is to be clickable at a specific line.
    """
    locations: dict[str, str] = {}
    for config, sections in _ENTRY_SECTIONS.items():
        current: str | None = None
        for lineno, line in enumerate(config.read_text().splitlines(), start=1):
            top = re.match(r"^([a-z_]+):\s*$", line)
            if top:
                current = top.group(1)
                continue
            if current not in sections:
                continue
            entry = re.match(r"^\s*-\s+name:\s*(\S+)", line)
            if entry:
                locations[entry.group(1)] = f"{config.name}:{lineno}"
    # snarea_curve has no `- name:` line: the whole document is the entry.
    locations["snarea_curve"] = SNAREA_LIBRARY_CONFIG.name
    return locations


def _natural_key(col: str):
    """Sort snarea_curve_10 after snarea_curve_9, not after snarea_curve_1."""
    m = re.match(r"^(.*?)(\d+)$", col)
    return (m.group(1), int(m.group(2))) if m else (col, -1)


def _render_columns(cols: list[str]) -> str:
    """One table cell for the emitted column(s) behind a single PRMS parameter.

    Two cases collapse many columns into one row: an ALIAS group (lulc_nhm_v11's
    `retention` | `rad_trncf`, the same quantity under two names across fabric
    vintages) and a DIMENSIONED parameter (snarea_curve_0..10, one PRMS
    `snarea_curve` of extent ndeplval).
    """
    if len(cols) > 3:
        ordered = sorted(cols, key=_natural_key)
        return f"`{ordered[0]}` … `{ordered[-1]}`"
    return r" \| ".join(f"`{c}`" for c in cols)


def _process_rows(declared: list[DeclaredParam], locations: dict[str, str]):
    """(process -> rows) plus the no-process bucket, from prms.columns + prms.defects.

    A `defects` entry produces a DEFECTIVE row under every process it names, and is
    NEVER merged with a real one -- that is the whole reason `defects` is a separate
    bucket from `columns`.

    Rows are keyed per (parameter, emitting FILE) and then COLLAPSED across files
    that emit the same parameter from the same column name -- the four LULC sources
    are alternatives, not four separate parameters, and left uncollapsed they turn
    PRMSCanopy's six parameters into twenty-four rows. `_collapse` keeps the file,
    entry and builder lists in the same order so a reader can pair them positionally.
    """
    by_process: dict[str, dict[tuple, dict]] = defaultdict(dict)
    unconsumed: dict[tuple, dict] = {}

    def _add(bucket, d, col, spec, defective):
        prms_name = spec["prms"]
        key = (prms_name, d.merged_file, d.name, defective)
        row = bucket.setdefault(
            key,
            {
                "prms": prms_name,
                # Parallel-array invariant: sources[i] is one (file, entry,
                # builder) triple, so the three rendered cells line up row-wise
                # even after `_collapse` merges alternative sources.
                "sources": [
                    (
                        d.merged_file,
                        locations.get(d.name, d.name),
                        d.prms.get("builder", "—"),
                    )
                ],
                "cols": [],
                "defective": defective,
                "issue": spec.get("issue"),
            },
        )
        row["cols"].append(col)

    for d in declared:
        for col, spec in (d.prms.get("columns") or {}).items():
            processes = spec.get("processes") or []
            if not processes:
                _add(unconsumed, d, col, spec, False)
            for process in processes:
                _add(by_process[process], d, col, spec, False)
        for col, spec in (d.prms.get("defects") or {}).items():
            for process in spec.get("processes") or []:
                _add(by_process[process], d, col, spec, True)

    return (
        {p: _collapse(rows) for p, rows in by_process.items()},
        _collapse(unconsumed),
    )


def _collapse(rows: dict[tuple, dict]) -> dict[tuple, dict]:
    """Merge rows that emit the same parameter from the same column name(s)."""
    merged: dict[tuple, dict] = {}
    for _, row in sorted(rows.items()):
        key = (row["prms"], tuple(sorted(row["cols"])), row["defective"])
        if key in merged:
            for source in row["sources"]:
                if source not in merged[key]["sources"]:
                    merged[key]["sources"].append(source)
        else:
            merged[key] = row
    # Config order, not alphabetical: the four LULC alternatives then read in the
    # order they appear in zonal_params.yml, which is how an operator meets them.
    for row in merged.values():
        row["sources"].sort(key=lambda s: _entry_sort_key(s[1]))
    return merged


def _entry_sort_key(entry: str):
    """Sort a `<config>:<line>` cite by file then NUMERIC line, not string line."""
    config, _, line = entry.partition(":")
    return (config, int(line) if line.isdigit() else 0)


def _row_markdown(row: dict) -> str:
    warn = "" if row["defective"] else _rename_marker(row)
    if row["defective"]:
        issue = row.get("issue")
        link = (
            f" ([#{issue}](https://github.com/rmcd-mscb/gfv2-params/issues/{issue}))"
            if issue
            else ""
        )
        column = f"**DEFECTIVE** — {_render_columns(row['cols'])} is not this parameter{link}"
    else:
        column = _render_columns(row["cols"])
    files, entries, builders = (list(x) for x in zip(*row["sources"]))
    return (
        f"| `{row['prms']}`{warn} | {_cell(files)} | {column} "
        f"| {_cell(entries)} | {_cell(builders)} |"
    )


def _cell(values: list[str]) -> str:
    """One markdown cell. Repeats are kept so the three cells stay row-aligned."""
    return " · ".join(f"`{v}`" for v in values)


def _rename_marker(row: dict) -> str:
    """⚠️ iff the emitted column name is not the PRMS parameter name.

    A DIMENSIONED parameter is not a rename: `snarea_curve` of extent ndeplval is
    emitted as `snarea_curve_0..10`, and flagging that ⚠️ would tell a reader to
    translate a name that needs no translation. Only a genuinely different name --
    `soils` for `soil_type`, `mean` for `hru_elev` -- earns the marker.

    An ALIAS group does earn it, even though one alternative matches: lulc_nhm_v11
    declares both `retention` and `rad_trncf`, and which one is on disk depends on
    the fabric's vintage. A reader looking at gfv2 or oregon sees `retention` and
    does need to translate, so the test is "is EVERY declared column the PRMS
    name?", not "is ANY of them?".
    """
    if set(row["cols"]) == {row["prms"]}:
        return ""
    if all(re.fullmatch(rf"{re.escape(row['prms'])}_\d+", c) for c in row["cols"]):
        return ""
    return " ⚠️"


_TABLE_HEAD = (
    "| PRMS parameter | Emitted file | Column | Config entry | Builder |\n"
    "| --- | --- | --- | --- | --- |"
)


def render_by_process(declared, locations) -> str:
    by_process, unconsumed = _process_rows(declared, locations)
    out: list[str] = []

    def _section(title: str, rows: dict, count_note: str) -> None:
        out.append(f"### {title} — {count_note}")
        out.append("")
        out.append(_TABLE_HEAD)
        for _, row in sorted(rows.items(), key=lambda kv: (kv[1]["defective"], kv[0])):
            out.append(_row_markdown(row))
        out.append("")

    # Most-consumed process first; a scientist asking "which parameters feed PRMS
    # runoff?" should not have to scroll past PRMSGroundwater's single row.
    def _n_params(rows):
        return len({r["prms"] for r in rows.values() if not r["defective"]})

    for process, rows in sorted(
        by_process.items(), key=lambda kv: (-_n_params(kv[1]), kv[0])
    ):
        n = _n_params(rows)
        n_defective = len({r["prms"] for r in rows.values() if r["defective"]})
        note = f"{n} parameter{'s' if n != 1 else ''}"
        if n_defective:
            note += f" (+{n_defective} defective)"
        _section(process, rows, note)

    if unconsumed:
        n = len({r["prms"] for r in unconsumed.values()})
        _section(
            "Not consumed by any pywatershed process",
            unconsumed,
            f"{n} parameter{'s' if n != 1 else ''}",
        )

    return "\n".join(out).rstrip()


def render_by_entry(declared, locations) -> str:
    """One row per config entry: what it emits, and what is provenance."""
    out = [
        "| Config entry | Merged file | PRMS parameters | Provenance columns |",
        "| --- | --- | --- | --- |",
    ]
    # _entry_sort_key, not the raw string: "zonal_params.yml:43" sorts AFTER
    # "zonal_params.yml:389" under a string comparison, which scrambled the table
    # into 133, 169, 187, 209, 277, 331, 389, 43, 451, 81.
    for d in sorted(declared, key=lambda d: (_entry_sort_key(locations.get(d.name, "")), d.name)):
        columns = d.prms.get("columns") or {}
        defects = d.prms.get("defects") or {}
        cells = [f"`{p}`" for p in sorted({spec["prms"] for spec in columns.values()})]
        cells += [
            f"`{spec['prms']}` — **DEFECTIVE** (emitted as `{col}`)"
            for col, spec in sorted(defects.items())
        ]
        rendered = ", ".join(cells) if cells else "—"
        prov = sorted(d.prms.get("provenance") or {})
        out.append(
            f"| `{d.name}` | `{d.merged_file}` | {rendered} | "
            f"{', '.join(f'`{c}`' for c in prov) if prov else '—'} |"
        )
    return "\n".join(out)


def render_by_builder(declared) -> str:
    """One row per `prms.builder` string, with the PRMS parameters it produces."""
    good: dict[str, set[str]] = defaultdict(set)
    broken: dict[str, set[str]] = defaultdict(set)
    for d in declared:
        builder = d.prms.get("builder", "—")
        for spec in (d.prms.get("columns") or {}).values():
            good[builder].add(spec["prms"])
        for spec in (d.prms.get("defects") or {}).values():
            broken[builder].add(spec["prms"])
    out = [
        "| Builder module | PRMS parameters produced |",
        "| --- | --- |",
    ]
    for builder in sorted(set(good) | set(broken)):
        # "(DEFECTIVE)" stays OUTSIDE the backticks -- inside, it reads as part of
        # the parameter name, which is the one thing this column must not imply.
        cells = [f"`{p}`" for p in sorted(good[builder])]
        cells += [f"`{p}` **(DEFECTIVE)**" for p in sorted(broken[builder])]
        out.append(f"| `{builder}` | {', '.join(cells)} |")
    return "\n".join(out)


def _replace_region(text: str, name: str, body: str) -> str:
    begin, end = f"<!-- BEGIN GENERATED: {name} -->", f"<!-- END GENERATED: {name} -->"
    pattern = re.compile(rf"{re.escape(begin)}.*?{re.escape(end)}", re.S)
    if not pattern.search(text):
        raise SystemExit(
            f"{INDEX_PATH} has no '{name}' generated region. Expected a\n"
            f"  {begin}\n  ...\n  {end}\n"
            f"pair -- the generator only rewrites marked regions so it cannot clobber "
            f"the hand-maintained prose around them."
        )
    # `\g<0>`-free literal replacement: body may contain backslashes (`\|` escapes
    # a pipe inside a markdown table cell) and re.sub would treat them as group refs.
    return pattern.sub(lambda _m: f"{begin}\n{body}\n{end}", text)


def build(check: bool = False) -> int:
    declared = load_declared_params()
    locations = _entry_locations()

    text = INDEX_PATH.read_text()
    new = _replace_region(text, "by-process", render_by_process(declared, locations))
    new = _replace_region(new, "by-entry", render_by_entry(declared, locations))
    new = _replace_region(new, "by-builder", render_by_builder(declared))

    if new == text:
        print(f"{INDEX_PATH.relative_to(_REPO_ROOT)} is up to date.")
        return 0
    if check:
        print(
            f"{INDEX_PATH.relative_to(_REPO_ROOT)} is STALE -- re-run "
            f"`python scripts/build_parameter_index.py`.",
            file=sys.stderr,
        )
        return 1
    INDEX_PATH.write_text(new)
    print(f"Wrote {INDEX_PATH.relative_to(_REPO_ROOT)} ({len(declared)} declared entries).")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 if the index is stale instead of rewriting it",
    )
    raise SystemExit(build(check=parser.parse_args().check))


if __name__ == "__main__":
    main()
