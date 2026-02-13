#!/usr/bin/env python3
"""
Basic test for Phase 3 agents - no API calls required.
Tests agent initialization and utility methods.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_db
from src.agents.explorer_agent import ExplorerAgent
from src.agents.researcher_agent import ResearcherAgent


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def test_agent_initialization():
    """Test that agents can be initialized."""
    print_section("TEST 1: Agent Initialization")

    # Initialize database
    db_path = Path(__file__).parent.parent / 'data' / 'researchdb.db'
    db = get_db(str(db_path))

    # Test Explorer
    explorer = ExplorerAgent(db=db, role="fraud_analyst")
    print(f"✓ Explorer Agent initialized")
    print(f"  - Role: {explorer.role}")
    print(f"  - Layer: {explorer.layer}")
    print(f"  - Domain: {explorer.domain}")
    print()

    # Test Researcher
    researcher = ResearcherAgent(db=db, role="fraud_analyst")
    print(f"✓ Researcher Agent initialized")
    print(f"  - Role: {researcher.role}")
    print(f"  - Layer: {researcher.layer}")
    print(f"  - Domain: {researcher.domain}")
    print()

    return explorer, researcher


def test_explorer_utilities(explorer):
    """Test Explorer utility methods that don't need LLM."""
    print_section("TEST 2: Explorer Utilities")

    # Test table identification
    query1 = "show me fraud transactions by merchant"
    tables1 = explorer.identify_relevant_tables(query1)
    print(f"Query: '{query1}'")
    print(f"✓ Identified tables: {', '.join(tables1)}")
    print()

    query2 = "analyze customer behavior in mobile channel"
    tables2 = explorer.identify_relevant_tables(query2)
    print(f"Query: '{query2}'")
    print(f"✓ Identified tables: {', '.join(tables2)}")
    print()

    # Test data profiling
    profiling = explorer.profile_tables(['transactions', 'merchants'])
    print(f"✓ Profiled {len(profiling)} tables:")
    for table, info in profiling.items():
        print(f"  - {table}: {info['row_count']} rows, {len(info['columns'])} columns")
    print()

    # Test quick profile
    print("Quick profile for 'fraud in December':")
    profile = explorer.quick_profile("fraud in December")
    print(profile)
    print()


def test_existing_view_check(explorer):
    """Test checking for existing views."""
    print_section("TEST 3: Existing View Discovery")

    query = "monthly fraud trends by channel"
    existing = explorer.check_existing_coverage(query)

    print(f"Query: '{query}'")
    print(f"✓ Found {len(existing)} potentially relevant views")

    for result in existing[:3]:
        print(f"  - {result.view.view_name} (score: {result.similarity_score:.3f})")
        print(f"    {result.view.description[:80]}...")
    print()


def test_base_agent_utilities(explorer):
    """Test BaseAgent utility methods."""
    print_section("TEST 4: Base Agent Utilities")

    # Test SQL execution
    sql = "SELECT COUNT(*) as fraud_count FROM transactions WHERE fraud_flag = 1"
    results = explorer.execute_sql(sql)
    print(f"✓ SQL execution works")
    print(f"  Query: {sql}")
    print(f"  Result: {results[0]}")
    print()

    # Test table sample
    sample = explorer.get_table_sample('transactions', limit=3)
    print(f"✓ Table sampling works")
    print(f"  Got {len(sample)} sample rows from transactions")
    print(f"  Sample: {sample[0]}")
    print()

    # Test schema formatting
    schema = explorer.format_schema_for_llm(['transactions'])
    print(f"✓ Schema formatting works")
    print(f"  Schema preview: {schema[:200]}...")
    print()


def test_researcher_utilities(researcher):
    """Test Researcher utility methods."""
    print_section("TEST 5: Researcher Utilities")

    # Test view comparison (if we have views)
    from src.catalog.view_catalog import ViewCatalog
    db = get_db()  # Already initialized, no path needed
    catalog = ViewCatalog(db)

    views = catalog.get_all_views()
    print(f"✓ View Catalog has {len(views)} views")

    if len(views) >= 2:
        view1 = views[0].view_name
        view2 = views[1].view_name

        comparison = researcher.compare_views(view1, view2)
        print(f"✓ Compared views: {view1} vs {view2}")
        print(f"  Relationship: {comparison['relationship']}")
        print(f"  Shared tables: {comparison['shared_tables']}")
    else:
        print("  (Not enough views to test comparison)")
    print()


def main():
    """Run basic agent tests."""
    print("\n" + "=" * 80)
    print("  PHASE 3: BASIC AGENT TESTS (No API Required)")
    print("=" * 80)

    try:
        # Test 1: Initialization
        explorer, researcher = test_agent_initialization()

        # Test 2: Explorer utilities
        test_explorer_utilities(explorer)

        # Test 3: View discovery
        test_existing_view_check(explorer)

        # Test 4: Base agent utilities
        test_base_agent_utilities(explorer)

        # Test 5: Researcher utilities
        test_researcher_utilities(researcher)

        # Summary
        print_section("BASIC TESTS COMPLETE ✓")
        print("All agent structure tests passed!")
        print("\nVerified:")
        print("  ✓ Agent initialization works")
        print("  ✓ Table identification works")
        print("  ✓ Data profiling works")
        print("  ✓ View discovery works")
        print("  ✓ SQL execution works")
        print("  ✓ Schema formatting works")
        print("\nTo test LLM functionality:")
        print("  1. Add ANTHROPIC_API_KEY to .env file")
        print("  2. Run: python scripts/test_agents.py")
        print("\n" + "=" * 80)
        print()

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
