"""Generate figures for the SNODAS snow-depletion-curve presentation deck.

Headless matplotlib. Reads the snarea pipeline's outputs for a fabric and
writes PNGs to docs/figures/snarea/<fabric>/. Re-run to refresh after the
pipeline regenerates outputs. Data-free schematics need no fabric.

Run:
    pixi run -e notebooks python scripts/render_snarea_figures.py \
        --fabric oregon --output-dir docs/figures/snarea/oregon
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402


def schematic_concept(out_path: Path) -> None:
    """Slide-2 concept: a generic snow depletion curve, plain-English axes."""
    x = np.linspace(0, 1, 200)
    y = np.clip(1 - (1 - x) ** 1.8, 0, 1)  # illustrative gradual depletion
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.plot(x, y, lw=3, color="#1f6fb4")
    ax.fill_between(x, y, color="#1f6fb4", alpha=0.12)
    ax.set_xlabel("Fraction of peak snow remaining  (melting →, right to left)")
    ax.set_ylabel("Fraction of the HRU still snow-covered")
    ax.set_title("A snow depletion curve")
    ax.set_xlim(1, 0)  # peak on the left, snow-free on the right
    ax.set_ylim(0, 1.02)
    ax.annotate("full snow cover", xy=(0.95, 0.98), fontsize=9, color="#444")
    ax.annotate("patchy, then bare", xy=(0.08, 0.06), fontsize=9, color="#444")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def schematic_pipeline(out_path: Path) -> None:
    """Slide-6 pipeline DAG: SNODAS → Stage1 → Stage2 → Stage3 → PRMS params."""
    boxes = [
        ("SNODAS\ndaily SWE (~1 km)", "#dbe9f6"),
        ("Stage 1\naggregate to HRUs\n(swe, snow-cover, sub-grid CV)", "#cfe6d4"),
        ("Stage 2\nempirical curves\n(Driscoll 2017)", "#cfe6d4"),
        ("Stage 3\nCV/lognormal library\n(Sexstone 2020)", "#cfe6d4"),
        ("PRMS / pyWatershed\nsnarea_curve, hru_deplcrv,\nsnarea_thresh", "#f6e7cf"),
    ]
    fig, ax = plt.subplots(figsize=(12, 2.6))
    ax.axis("off")
    n = len(boxes)
    slot = 1 / n
    pad = 0.012  # horizontal padding inside each slot
    box_w = slot - 2 * pad
    for i, (label, color) in enumerate(boxes):
        left = i * slot + pad
        ax.add_patch(
            plt.Rectangle(
                (left, 0.28),
                box_w,
                0.44,
                facecolor=color,
                edgecolor="#555",
                lw=1.2,
            )
        )
        ax.text(
            left + box_w / 2, 0.5, label, ha="center", va="center", fontsize=9
        )
        if i < n - 1:
            # Arrow spans the gap from this box's right edge to the next box's left edge.
            ax.annotate(
                "",
                xy=((i + 1) * slot + pad, 0.5),  # next box left edge
                xytext=(left + box_w, 0.5),  # this box right edge
                arrowprops=dict(arrowstyle="-|>", color="#555", lw=1.8),
            )
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


# Registry: name -> callable(out_path). Data-free schematics; later tasks add
# data-driven figures resolved per fabric in main().
SCHEMATICS = {
    "concept": schematic_concept,
    "pipeline": schematic_pipeline,
}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fabric", choices=["oregon", "gfv2"], default="oregon")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument(
        "--figures",
        nargs="*",
        default=None,
        help="Subset of figure names; default = all applicable.",
    )
    args = p.parse_args(argv)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    names = args.figures or list(SCHEMATICS)
    for name in names:
        if name in SCHEMATICS:
            out = args.output_dir / f"{name}.png"
            SCHEMATICS[name](out)
            print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
