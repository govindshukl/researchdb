#!/usr/bin/env python3
"""
ResearchDB Demo - Interactive CLI for research workflow.
Run end-to-end fraud research with natural language queries.
"""

import sys
import os
from pathlib import Path
import argparse

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestration.graph import run_research_workflow
from src.database.connection import get_db
from src.catalog.view_catalog import ViewCatalog


def print_banner():
    """Print welcome banner."""
    print("\n" + "=" * 80)
    print("  ResearchDB - Knowledge Crystallization POC")
    print("  Multi-Agent Research System with View Hierarchy")
    print("=" * 80)
    print()


def print_help():
    """Print usage instructions."""
    print("Usage:")
    print("  python scripts/run_demo.py [options]")
    print()
    print("Options:")
    print("  -q, --query TEXT      Run with specific query")
    print("  -i, --interactive     Interactive mode (default)")
    print("  --stats               Show database statistics")
    print("  --views               List all views in catalog")
    print("  -h, --help            Show this help message")
    print()
    print("Example Queries:")
    print("  - What are the fraud patterns in the last 3 months?")
    print("  - Show merchant risk analysis by category")
    print("  - Which channels have the highest fraud rates?")
    print("  - Analyze customer transaction anomalies")
    print()


def show_stats():
    """Show database statistics."""
    print("\n" + "=" * 80)
    print("  DATABASE STATISTICS")
    print("=" * 80 + "\n")

    db_path = Path(__file__).parent.parent / 'data' / 'researchdb.db'
    db = get_db(str(db_path))

    # Table counts
    tables = ['customers', 'merchants', 'channels', 'mcc_codes', 'transactions']
    for table in tables:
        count = db.get_row_count(table)
        print(f"  {table:20s}: {count:6,} rows")

    # Fraud statistics
    fraud_query = """
        SELECT
            COUNT(*) as total_txns,
            SUM(CASE WHEN fraud_flag = 1 THEN 1 ELSE 0 END) as fraud_txns,
            ROUND(100.0 * SUM(CASE WHEN fraud_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as fraud_rate,
            ROUND(SUM(amount), 2) as total_amount,
            ROUND(AVG(amount), 2) as avg_amount
        FROM transactions
    """

    result = db.execute_query(fraud_query)
    row = dict(result[0])

    print(f"\n  Transaction Summary:")
    print(f"    Total Transactions  : {row['total_txns']:,}")
    print(f"    Fraud Transactions  : {row['fraud_txns']:,}")
    print(f"    Fraud Rate         : {row['fraud_rate']}%")
    print(f"    Total Amount       : ${row['total_amount']:,.2f}")
    print(f"    Average Amount     : ${row['avg_amount']:,.2f}")

    print()


def show_views():
    """List all views in the catalog."""
    print("\n" + "=" * 80)
    print("  VIEW CATALOG")
    print("=" * 80 + "\n")

    db_path = Path(__file__).parent.parent / 'data' / 'researchdb.db'
    db = get_db(str(db_path))
    catalog = ViewCatalog(db)

    # Get all views
    all_views = catalog.get_all_views()

    if not all_views:
        print("  No views in catalog yet.\n")
        return

    print(f"  Total Views: {len(all_views)}\n")

    # Group by layer
    by_layer = {}
    for view in all_views:
        layer = view.layer
        if layer not in by_layer:
            by_layer[layer] = []
        by_layer[layer].append(view)

    for layer in sorted(by_layer.keys()):
        views = by_layer[layer]
        print(f"  Layer {layer} ({len(views)} views):")

        for view in views:
            status_icon = "⭐" if view.status == "PROMOTED" else "📝"
            print(f"    {status_icon} {view.view_name}")
            print(f"       {view.description[:70]}...")
            print(f"       Usage: {view.usage_count} times")
            print()

    # Show promoted views
    promoted = catalog.get_all_views(status='PROMOTED')
    if promoted:
        print(f"  Promoted Views (usage >= 3): {len(promoted)}")
        for view in promoted:
            print(f"    ⭐ {view.view_name} (used {view.usage_count} times)")

    print()


def run_query(query_text: str, session_id: str = None):
    """
    Run research workflow for a query.

    Args:
        query_text: Natural language query
        session_id: Optional session ID
    """
    if not query_text or not query_text.strip():
        print("Error: Empty query provided\n")
        return

    print("\n" + "=" * 80)
    print(f"  RESEARCH SESSION")
    print("=" * 80)
    print(f"\nQuery: {query_text}\n")
    print("Running workflow...")
    print("  [1/4] Starting...")
    print("  [2/4] Explorer Agent (Layer 1 discovery)...")
    print("  [3/4] Researcher Agent (Layer 2 analysis)...")
    print("  [4/4] Generating report...\n")

    # Run workflow
    result = run_research_workflow(
        user_query=query_text,
        user_role="fraud_analyst",
        session_id=session_id
    )

    # Display results
    if result['success']:
        print("✓ Workflow completed successfully!\n")

        # Show session info
        print(f"Session ID: {result['session_id']}")

        # Show views (deduplicated)
        if result['views_created']:
            unique_views = list(dict.fromkeys(result['views_created']))  # Preserve order, remove dupes
            print(f"\nViews Created: {len(unique_views)}")
            for view in unique_views:
                print(f"  ✓ {view}")

        if result['views_used']:
            print(f"\nViews Reused: {len(result['views_used'])}")
            for view in result['views_used'][:5]:
                print(f"  ↻ {view}")

        # Show report
        print("\n" + "=" * 80)
        print("  RESEARCH REPORT")
        print("=" * 80)
        print(result['report'])
        print()

    else:
        print(f"✗ Workflow failed: {result.get('error', 'Unknown error')}\n")

        # Show partial report if available
        if result.get('report'):
            print("\nPartial Report:")
            print(result['report'])
            print()


def interactive_mode():
    """Run in interactive mode with continuous queries."""
    print("\nInteractive Mode - Enter queries (or 'exit' to quit)\n")

    while True:
        try:
            # Prompt for query
            query = input("research> ").strip()

            # Handle commands
            if query.lower() in ['exit', 'quit', 'q']:
                print("\nExiting...\n")
                break

            if query.lower() in ['help', 'h', '?']:
                print_help()
                continue

            if query.lower() == 'stats':
                show_stats()
                continue

            if query.lower() == 'views':
                show_views()
                continue

            if query.lower() == 'clear':
                os.system('clear' if os.name != 'nt' else 'cls')
                print_banner()
                continue

            if not query:
                continue

            # Run query
            run_query(query)

        except KeyboardInterrupt:
            print("\n\nInterrupted. Type 'exit' to quit.\n")
            continue
        except EOFError:
            print("\n\nExiting...\n")
            break


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="ResearchDB Demo - Multi-Agent Research System",
        add_help=False
    )

    parser.add_argument('-q', '--query', type=str, help="Run with specific query")
    parser.add_argument('-i', '--interactive', action='store_true', help="Interactive mode")
    parser.add_argument('--stats', action='store_true', help="Show database statistics")
    parser.add_argument('--views', action='store_true', help="List all views")
    parser.add_argument('-h', '--help', action='store_true', help="Show help message")

    args = parser.parse_args()

    # Print banner
    print_banner()

    # Check for API key
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        print("⚠️  WARNING: ANTHROPIC_API_KEY not set in .env file")
        print("   The workflow requires API access to run.\n")
        print("   Please add your API key to .env and try again.\n")
        return

    # Initialize database
    db_path = Path(__file__).parent.parent / 'data' / 'researchdb.db'
    if not db_path.exists():
        print(f"⚠️  WARNING: Database not found at {db_path}")
        print("   Please run: python scripts/init_db.py\n")
        return

    # Handle arguments
    if args.help:
        print_help()
        return

    if args.stats:
        show_stats()
        return

    if args.views:
        show_views()
        return

    if args.query:
        run_query(args.query)
        return

    # Default to interactive mode
    interactive_mode()


if __name__ == "__main__":
    main()
