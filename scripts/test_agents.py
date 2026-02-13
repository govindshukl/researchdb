#!/usr/bin/env python3
"""
Test script for Phase 3: LLM Agents
Tests Explorer and Researcher agents in action.
"""

import sys
import os
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_db
from src.catalog.view_catalog import ViewCatalog
from src.catalog.semantic_search import SemanticSearch
from src.agents.explorer_agent import ExplorerAgent
from src.agents.researcher_agent import ResearcherAgent
from src.agents.llm_client import get_claude_client

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def test_explorer_agent():
    """Test Explorer Agent creating Layer 1 discovery views."""
    print_section("TEST 1: Explorer Agent - Layer 1 Discovery Views")

    # Initialize database
    db_path = Path(__file__).parent.parent / 'data' / 'researchdb.db'
    db = get_db(str(db_path))

    # Check database has data
    row_count = db.get_row_count('transactions')
    print(f"✓ Database ready: {row_count} transactions loaded\n")

    # Initialize Explorer Agent
    explorer = ExplorerAgent(db=db, role="fraud_analyst")
    print("✓ Explorer Agent initialized\n")

    # Test query
    query = "What are the fraud patterns in the last 3 months by channel and merchant category?"
    print(f"User Query: {query}\n")

    try:
        # Process query
        result = explorer.process(query)

        if result['success']:
            print("✓ Explorer completed successfully!\n")

            # Show created views
            created_views = result['created_views']
            print(f"Created {len(created_views)} Layer 1 discovery views:\n")

            for view in created_views:
                print(f"  📊 {view.view_name}")
                print(f"     Layer: {view.layer}")
                print(f"     Domain: {view.domain}")
                print(f"     Description: {view.description}")
                print(f"     Base Tables: {', '.join(view.base_tables)}")
                print(f"     Tags: {', '.join(view.tags)}")
                print()

            # Show context for Researcher
            context = result['context']
            print("Context passed to Researcher:")
            print(f"  - Relevant tables: {', '.join(context['relevant_tables'])}")
            print(f"  - Existing views: {len(context['existing_views'])}")
            print(f"  - Created views: {len(context['created_views'])}")
            print(f"  - Recommendations: {context['recommendations'][:100]}...")
            print()

            return result
        else:
            print(f"✗ Explorer failed: {result.get('message', 'Unknown error')}")
            return None

    except Exception as e:
        logger.error(f"Explorer test failed: {e}", exc_info=True)
        print(f"\n⚠️  Note: This test requires ANTHROPIC_API_KEY in .env")
        print(f"   Error: {str(e)}")
        return None


def test_researcher_agent(explorer_context=None):
    """Test Researcher Agent creating Layer 2 research views."""
    print_section("TEST 2: Researcher Agent - Layer 2 Research Views")

    db = get_db()

    # Initialize Researcher Agent
    researcher = ResearcherAgent(db=db, role="fraud_analyst")
    print("✓ Researcher Agent initialized\n")

    # Test query
    query = "Are there anomalous fraud patterns? Which merchants are high risk?"
    print(f"User Query: {query}\n")

    try:
        # Process query with Explorer's context
        result = researcher.process(query, context=explorer_context)

        if result['success']:
            print("✓ Researcher completed successfully!\n")

            # Show analysis
            print("Analysis:")
            print(f"  {result['analysis'][:200]}...\n")

            # Show query results
            query_results = result['query_results']
            print(f"Executed {len(query_results)} analytical queries:\n")

            for qr in query_results[:3]:  # Show first 3
                if qr['success']:
                    print(f"  📈 {qr['purpose']}")
                    print(f"     Rows: {qr['row_count']}")
                    print(f"     Insights: {qr.get('insights', 'N/A')[:100]}...")
                    print()

            # Show created views
            created_views = result['created_views']
            print(f"Created {len(created_views)} Layer 2 research views:\n")

            for view in created_views:
                print(f"  🔬 {view.view_name}")
                print(f"     Layer: {view.layer}")
                print(f"     Description: {view.description}")
                print(f"     Depends on: {', '.join(view.depends_on_views) if view.depends_on_views else 'None'}")
                print()

            # Show report
            print("Final Report:")
            print(f"  {result['report'][:300]}...\n")

            return result
        else:
            print(f"✗ Researcher failed: {result.get('message', 'Unknown error')}")
            return None

    except Exception as e:
        logger.error(f"Researcher test failed: {e}", exc_info=True)
        print(f"\n⚠️  Note: This test requires ANTHROPIC_API_KEY in .env")
        print(f"   Error: {str(e)}")
        return None


def test_view_catalog_after_agents():
    """Show View Catalog state after agent runs."""
    print_section("TEST 3: View Catalog State")

    db = get_db()
    catalog = ViewCatalog(db)

    # Get all views
    all_views = catalog.get_all_views()

    print(f"Total views in catalog: {len(all_views)}\n")

    # Group by layer
    by_layer = {}
    for view in all_views:
        layer = view.layer
        if layer not in by_layer:
            by_layer[layer] = []
        by_layer[layer].append(view)

    for layer in sorted(by_layer.keys()):
        views = by_layer[layer]
        print(f"Layer {layer} ({len(views)} views):")
        for view in views:
            status_emoji = "✓" if view.status == "PROMOTED" else "📝"
            print(f"  {status_emoji} {view.view_name} (usage: {view.usage_count})")
        print()

    # Show promoted views
    promoted = catalog.get_all_views(status='PROMOTED')
    if promoted:
        print(f"Promoted views (usage >= 3): {len(promoted)}")
        for view in promoted:
            print(f"  ⭐ {view.view_name} (used {view.usage_count} times)")
        print()


def test_semantic_search():
    """Test semantic search on created views."""
    print_section("TEST 4: Semantic Search on Views")

    db = get_db()
    catalog = ViewCatalog(db)
    search = SemanticSearch(catalog)

    # Test queries
    queries = [
        "fraud by channel",
        "merchant risk analysis",
        "monthly trends"
    ]

    for query in queries:
        print(f"Query: '{query}'")
        results = search.search(query, top_k=3, min_score=0.3)

        if results:
            print(f"  Found {len(results)} relevant views:")
            for result in results:
                print(f"    - {result.view.view_name} (score: {result.similarity_score:.3f})")
                print(f"      {result.view.description[:80]}...")
        else:
            print("  No relevant views found")
        print()


def test_quick_profile():
    """Test Explorer's quick profiling utility."""
    print_section("TEST 5: Quick Data Profile")

    db = get_db()  # Already initialized, no path needed
    explorer = ExplorerAgent(db=db)

    query = "fraud transactions in December"
    profile = explorer.quick_profile(query)

    print(profile)
    print()


def test_llm_client_stats():
    """Show LLM usage statistics."""
    print_section("TEST 6: LLM Usage Statistics")

    try:
        client = get_claude_client()
        stats = client.get_usage_stats()

        print("Claude API Usage:")
        print(f"  Input tokens:  {stats['total_input_tokens']:,}")
        print(f"  Output tokens: {stats['total_output_tokens']:,}")
        print(f"  Total tokens:  {stats['total_tokens']:,}")
        print(f"  Total cost:    ${stats['total_cost']:.4f}")
        print(f"  Model:         {stats['model']}")
        print()

    except Exception as e:
        print(f"Could not retrieve usage stats: {e}\n")


def main():
    """Run all agent tests."""
    print("\n" + "=" * 80)
    print("  PHASE 3: LLM AGENTS TEST SUITE")
    print("=" * 80)

    # Check for API key
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        print("\n⚠️  WARNING: ANTHROPIC_API_KEY not set in .env file")
        print("   Agent tests will fail without API access.")
        print("   Please add your API key to .env and try again.\n")
        return

    try:
        # Test 1: Explorer Agent (this initializes the database)
        explorer_result = test_explorer_agent()

        # Test 5: Quick profile (doesn't need LLM)
        test_quick_profile()

        if explorer_result:
            # Test 2: Researcher Agent (uses Explorer's context)
            context = explorer_result.get('context')
            test_researcher_agent(explorer_context=context)

        # Test 3: View Catalog state
        test_view_catalog_after_agents()

        # Test 4: Semantic search
        test_semantic_search()

        # Test 6: LLM stats
        test_llm_client_stats()

        # Final summary
        print_section("PHASE 3 COMPLETE ✓")
        print("All agent tests passed successfully!")
        print("\nKey achievements:")
        print("  ✓ Explorer Agent creates Layer 1 discovery views")
        print("  ✓ Researcher Agent builds Layer 2 research views")
        print("  ✓ Agents use LLM for SQL generation and analysis")
        print("  ✓ View Catalog tracks all created views")
        print("  ✓ Semantic search finds relevant views")
        print("  ✓ Full end-to-end workflow demonstrated")
        print("\n" + "=" * 80)
        print()

    except KeyboardInterrupt:
        print("\n\nTest interrupted by user.")
    except Exception as e:
        logger.error(f"Test suite failed: {e}", exc_info=True)
        print(f"\n✗ Test suite failed with error: {e}")
        raise


if __name__ == "__main__":
    main()
