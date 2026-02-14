#!/usr/bin/env python3
"""
Generate benchmark visualization plots from JSON benchmark results.

Creates publication-quality grouped bar charts comparing Avro reader performance:
- Interactive HTML for documentation (with hover details)
- Static PNG for README

Usage:
    # Default: read benchmark.json, output to benches/python/output/
    python benches/python/plot.py

    # Custom input/output
    python benches/python/plot.py --input results.json --output-dir docs/assets/

    # PNG only (skip HTML)
    python benches/python/plot.py --png-only

    # HTML only (skip PNG, no kaleido dependency needed)
    python benches/python/plot.py --html-only
"""

import argparse
import json
import sys
from pathlib import Path

import plotly.graph_objects as go

# =============================================================================
# Configuration
# =============================================================================

# Muted rainbow palette - based on Wong 2011, desaturated for a softer look
COLORS = {
    "jetliner": "#1a5a8a",  # Muted strong blue - primary
    "polars": "#2a7a5a",  # Muted teal
    "polars_avro": "#c4b840",  # Muted yellow
    "fastavro": "#c08030",  # Muted orange
    "fastavro_pandas": "#a85030",  # Muted vermillion
    "avro": "#a06080",  # Muted pink
}

# Display names for readers (cleaner labels)
READER_LABELS = {
    "jetliner": "jetliner",
    "polars": "polars",
    "polars_avro": "polars-avro",
    "fastavro": "fastavro",
    "fastavro_pandas": "fastavro + pandas",
    "avro": "apache-avro",
}

# Display names for scenarios
SCENARIO_LABELS = {
    "large_simple": "Simple\n(1M rows, 5 cols)",
    "large_wide": "Wide\n(1M rows, 100 cols)",
    "large_complex": "Complex\n(nested, arrays, maps)",
    "projection": "Projection\n(5 of 100 cols)",
}

# Preferred order for readers in legend and bars
READER_ORDER = [
    "jetliner",
    "polars",
    "polars_avro",
    "fastavro",
    "fastavro_pandas",
    "avro",
]

# Preferred order for scenarios
SCENARIO_ORDER = ["large_simple", "large_wide", "large_complex", "projection"]


# =============================================================================
# Data Loading
# =============================================================================


def load_benchmark_data(input_path: Path) -> dict:
    """Load and validate benchmark JSON data."""
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    with open(input_path) as f:
        data = json.load(f)

    if "results" not in data:
        print("Error: Invalid benchmark JSON - missing 'results' key", file=sys.stderr)
        sys.exit(1)

    return data


# =============================================================================
# Chart Creation
# =============================================================================


def create_performance_chart(data: dict) -> go.Figure:
    """Create a grouped bar chart comparing reader performance across scenarios."""
    results = data["results"]

    # Determine which scenarios and readers are present
    scenarios = [s for s in SCENARIO_ORDER if s in results]
    readers = [r for r in READER_ORDER if any(r in results[s] for s in scenarios)]

    # Build the figure
    fig = go.Figure()

    for reader in readers:
        means = []
        stdevs = []
        hover_texts = []
        scenario_labels = []

        for scenario in scenarios:
            scenario_results = results.get(scenario, {})
            reader_results = scenario_results.get(reader, {})

            if "error" in reader_results or "mean_sec" not in reader_results:
                # Reader failed or missing for this scenario
                means.append(None)
                stdevs.append(None)
                hover_texts.append(None)
            else:
                mean = reader_results["mean_sec"]
                stdev = reader_results["stdev_sec"]
                runs = reader_results.get("runs", "?")

                means.append(mean)
                stdevs.append(stdev)

                # Rich hover text
                hover_texts.append(
                    f"<b>{READER_LABELS.get(reader, reader)}</b><br>"
                    f"Mean: {mean:.3f}s<br>"
                    f"Stdev: {stdev:.3f}s<br>"
                    f"Runs: {runs}"
                )

            scenario_labels.append(SCENARIO_LABELS.get(scenario, scenario))

        # Format bar labels (1 decimal place)
        bar_labels = [f"{m:.1f}" if m is not None else "" for m in means]

        fig.add_trace(
            go.Bar(
                name=READER_LABELS.get(reader, reader),
                x=scenario_labels,
                y=means,
                text=bar_labels,
                textposition="outside",
                textfont=dict(size=8),
                error_y=dict(
                    type="data",
                    array=stdevs,
                    visible=True,
                    thickness=1.5,
                    width=3,
                ),
                marker_color=COLORS.get(reader, "#999999"),
                hovertemplate="%{customdata}<extra></extra>",
                customdata=hover_texts,
            )
        )

    # Layout configuration
    fig.update_layout(
        title=dict(
            text="<b>Avro Reader Performance Comparison</b>",
            font=dict(size=20),
            x=0.5,
            xanchor="center",
        ),
        xaxis=dict(
            title=dict(text="Benchmark Scenario", font=dict(size=14)),
            tickfont=dict(size=11),
            tickangle=0,
        ),
        yaxis=dict(
            title=dict(text="Time (seconds, log scale)", font=dict(size=14)),
            type="log",
            tickfont=dict(size=12),
            gridcolor="rgba(128, 128, 128, 0.2)",
            gridwidth=1,
            minor=dict(
                ticklen=4,
                tickcolor="rgba(128, 128, 128, 0.2)",
                gridcolor="rgba(128, 128, 128, 0.1)",
                showgrid=True,
            ),
        ),
        barmode="group",
        bargap=0.2,
        bargroupgap=0.05,
        legend=dict(
            title=dict(text="<b>Reader</b>", font=dict(size=12)),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            font=dict(size=11),
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Inter, system-ui, sans-serif"),
        margin=dict(l=60, r=40, t=120, b=80),
        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
            font_family="Inter, system-ui, sans-serif",
        ),
    )

    # Add subtle border around plot area
    fig.update_xaxes(
        showline=True,
        linewidth=1,
        linecolor="rgba(128, 128, 128, 0.3)",
        mirror=True,
    )
    fig.update_yaxes(
        showline=True,
        linewidth=1,
        linecolor="rgba(128, 128, 128, 0.3)",
        mirror=True,
    )

    return fig


def create_speedup_chart(data: dict) -> go.Figure:
    """Create a bar chart showing speedup relative to jetliner."""
    results = data["results"]

    scenarios = [s for s in SCENARIO_ORDER if s in results]
    readers = [
        r
        for r in READER_ORDER
        if r != "jetliner" and any(r in results[s] for s in scenarios)
    ]

    fig = go.Figure()

    for reader in readers:
        speedups = []
        hover_texts = []
        scenario_labels = []

        for scenario in scenarios:
            scenario_results = results.get(scenario, {})
            baseline = scenario_results.get("jetliner", {})
            reader_results = scenario_results.get(reader, {})

            if (
                "error" in reader_results
                or "mean_sec" not in reader_results
                or "error" in baseline
                or "mean_sec" not in baseline
            ):
                speedups.append(None)
                hover_texts.append(None)
            else:
                baseline_time = baseline["mean_sec"]
                reader_time = reader_results["mean_sec"]
                speedup = reader_time / baseline_time

                speedups.append(speedup)
                hover_texts.append(
                    f"<b>{READER_LABELS.get(reader, reader)}</b><br>"
                    f"Time: {reader_time:.3f}s<br>"
                    f"Baseline: {baseline_time:.3f}s<br>"
                    f"<b>{speedup:.1f}x slower</b>"
                )

            scenario_labels.append(SCENARIO_LABELS.get(scenario, scenario))

        # Format bar labels (1 decimal place with 'x' suffix)
        bar_labels = [f"{s:.1f}x" if s is not None else "" for s in speedups]

        fig.add_trace(
            go.Bar(
                name=READER_LABELS.get(reader, reader),
                x=scenario_labels,
                y=speedups,
                text=bar_labels,
                textposition="outside",
                textfont=dict(size=8),
                marker_color=COLORS.get(reader, "#999999"),
                hovertemplate="%{customdata}<extra></extra>",
                customdata=hover_texts,
            )
        )

    fig.update_layout(
        title=dict(
            text="<b>Relative Performance vs jetliner (scan)</b>",
            font=dict(size=20),
            x=0.5,
            xanchor="center",
        ),
        xaxis=dict(
            title=dict(text="Benchmark Scenario", font=dict(size=14)),
            tickfont=dict(size=11),
        ),
        yaxis=dict(
            title=dict(text="Relative time (1.0 = jetliner)", font=dict(size=14)),
            type="log",
            tickfont=dict(size=12),
            gridcolor="rgba(128, 128, 128, 0.2)",
        ),
        barmode="group",
        bargap=0.2,
        bargroupgap=0.05,
        legend=dict(
            title=dict(text="<b>Reader</b>", font=dict(size=12)),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            font=dict(size=11),
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Inter, system-ui, sans-serif"),
        margin=dict(l=60, r=40, t=120, b=80),
    )

    # Add baseline reference line at y=1
    fig.add_hline(
        y=1,
        line_dash="dash",
        line_color="rgba(0, 114, 178, 0.5)",
        line_width=2,
        annotation_text="jetliner baseline",
        annotation_position="top right",
        annotation_font_size=10,
        annotation_font_color="rgba(0, 114, 178, 0.8)",
    )

    fig.update_xaxes(
        showline=True, linewidth=1, linecolor="rgba(128, 128, 128, 0.3)", mirror=True
    )
    fig.update_yaxes(
        showline=True, linewidth=1, linecolor="rgba(128, 128, 128, 0.3)", mirror=True
    )

    return fig


# =============================================================================
# Output Generation
# =============================================================================


def save_html(fig: go.Figure, output_path: Path, title: str) -> None:
    """Save interactive HTML with embedded Plotly.js."""
    # Use CDN for smaller file size, include modebar for interactivity
    html_content = fig.to_html(
        full_html=True,
        include_plotlyjs="cdn",
        config={
            "displayModeBar": True,
            "displaylogo": False,
            "modeBarButtonsToRemove": ["lasso2d", "select2d"],
            "toImageButtonOptions": {
                "format": "png",
                "filename": output_path.stem,
                "height": 600,
                "width": 1000,
                "scale": 2,
            },
        },
    )

    # Inject custom title
    html_content = html_content.replace("<head>", f"<head><title>{title}</title>")

    output_path.write_text(html_content)
    print(f"  HTML: {output_path}")


def save_png(fig: go.Figure, output_path: Path) -> None:
    """Save static PNG image."""
    try:
        fig.write_image(
            output_path,
            width=1200,
            height=700,
            scale=2,  # 2x for retina/high-DPI
            engine="kaleido",
        )
        print(f"  PNG:  {output_path}")
    except ValueError as e:
        if "kaleido" in str(e).lower():
            print("  PNG:  SKIPPED (install 'kaleido' for static image export)")
        else:
            raise


# =============================================================================
# Main
# =============================================================================


def generate_plots(
    input_file: str = "benchmark.json",
    output_dir: str = "benches/python/output",
    html_only: bool = False,
    png_only: bool = False,
) -> None:
    """Generate benchmark plots from JSON results.

    Args:
        input_file: Path to benchmark JSON file.
        output_dir: Directory for output files.
        html_only: If True, skip PNG generation.
        png_only: If True, skip HTML generation.
    """
    input_path = Path(input_file)
    output_path = Path(output_dir)

    # Load data
    print(f"Loading benchmark data from: {input_path}")
    data = load_benchmark_data(input_path)

    # Create output directory
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate performance comparison chart
    print("\nGenerating performance comparison chart...")
    perf_fig = create_performance_chart(data)

    if not png_only:
        save_html(
            perf_fig,
            output_path / "benchmark_performance.html",
            "Avro Reader Performance Comparison",
        )
    if not html_only:
        save_png(perf_fig, output_path / "benchmark_performance.png")

    # Generate speedup chart
    print("\nGenerating speedup chart...")
    speedup_fig = create_speedup_chart(data)

    if not png_only:
        save_html(
            speedup_fig,
            output_path / "benchmark_speedup.html",
            "Performance Relative to jetliner",
        )
    if not html_only:
        save_png(speedup_fig, output_path / "benchmark_speedup.png")

    # Print metadata summary
    metadata = data.get("metadata", {})
    if metadata:
        print("\nBenchmark metadata:")
        print(f"  Timestamp: {metadata.get('timestamp', 'unknown')}")
        print(f"  Platform:  {metadata.get('platform', 'unknown')}")
        print(f"  Python:    {metadata.get('python_version', 'unknown')}")

    print(f"\nDone! Output written to: {output_path}/")


def main():
    parser = argparse.ArgumentParser(
        description="Generate benchmark visualization plots",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        default="benchmark.json",
        help="Input benchmark JSON file (default: benchmark.json)",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default="benches/python/output",
        help="Output directory for generated files (default: benches/python/output)",
    )
    parser.add_argument(
        "--html-only",
        action="store_true",
        help="Only generate HTML (skip PNG, no kaleido needed)",
    )
    parser.add_argument(
        "--png-only",
        action="store_true",
        help="Only generate PNG (skip HTML)",
    )
    args = parser.parse_args()

    generate_plots(
        input_file=args.input,
        output_dir=args.output_dir,
        html_only=args.html_only,
        png_only=args.png_only,
    )


if __name__ == "__main__":
    main()
