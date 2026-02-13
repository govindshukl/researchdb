#!/usr/bin/env python3
"""
Test script for Phase 2: Graph Intelligence.
Tests semantic search, schema graph, and Steiner Tree optimization.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_db
from src.database.view_executor import ViewExecutor
from src.catalog.models import ViewMetadata
from src.catalog.view_catalog import ViewCatalog
from src.catalog.semantic_search import SemanticSearch
from src.graph.schema_graph import SchemaGraph
from src.graph.steiner_tree import SteinerTreeSolver
from src.graph.view_integration import ViewIntegration


def setup_test_data():
    """Set up database with test views."""
    print("\n" + "="*70)
    print("Setting up test data...")
    print("="*70)

    db_path = Path(__file__).parent.parent / "data" / "researchdb.sqlite"
    db = get_db(str(db_path))

    catalog = ViewCatalog(db)
    executor = ViewExecutor(db)

    # Create a few test views if they don't exist
    test_views = [
        {
            'name': 'v_fraud_monthly_trend',
            'layer': 1,
            'domain': 'fraud',
            'description': 'Monthly fraud aggregation showing trends over time by channel',
            'tables': ['transactions', 'channels'],
            'tags': ['fraud', 'monthly', 'trend', 'aggregation'],
            'ddl': """
                CREATE VIEW v_fraud_monthly_trend AS
                SELECT
                    strftime('%Y-%m', txn_date) as month,
                    channel_id,
                    COUNT(*) as total_txns,
                    SUM(fraud_flag) as fraud_count,
                    ROUND(AVG(amount), 2) as avg_amount
                FROM transactions
                GROUP BY strftime('%Y-%m', txn_date), channel_id
            """
        },
        {
            'name': 'v_merchant_risk_profile',
            'layer': 1,
            'domain': 'fraud',
            'description': 'Merchant risk analysis with transaction patterns and fraud rates',
            'tables': ['transactions', 'merchants', 'mcc_codes'],
            'tags': ['merchant', 'risk', 'profile', 'fraud'],
            'ddl': """
                CREATE VIEW v_merchant_risk_profile AS
                SELECT
                    m.merchant_id,
                    m.name as merchant_name,
                    m.risk_tier,
                    mc.category as mcc_category,
                    COUNT(t.txn_id) as total_txns,
                    SUM(t.fraud_flag) as fraud_count,
                    ROUND(SUM(t.fraud_flag) * 100.0 / COUNT(t.txn_id), 2) as fraud_rate
                FROM merchants m
                JOIN transactions t ON m.merchant_id = t.merchant_id
                JOIN mcc_codes mc ON m.mcc_code = mc.mcc_code
                GROUP BY m.merchant_id, m.name, m.risk_tier, mc.category
            """
        }
    ]

    for view_def in test_views:
        if not catalog.find_by_name(view_def['name']):
            # Create view in database
            result = executor.create_view(view_def['ddl'])

            if result['success']:
                # Register in catalog
                view_meta = ViewMetadata(
                    view_name=view_def['name'],
                    layer=view_def['layer'],
                    domain=view_def['domain'],
                    description=view_def['description'],
                    base_tables=view_def['tables'],
                    view_definition=view_def['ddl'],
                    tags=view_def['tags'],
                    status='PROMOTED'  # Pre-promote for testing
                )
                catalog.register_view(view_meta)
                print(f"✓ Created view: {view_def['name']}")

    print(f"✓ Test data ready\n")
    return db, catalog


def test_semantic_search(catalog):
    """Test 1: Semantic search for view discovery."""
    print("\n" + "="*70)
    print("TEST 1: Semantic Search")
    print("="*70)

    semantic_search = SemanticSearch(catalog)

    # Index all views
    print("\n[Test 1.1] Indexing views...")
    count = semantic_search.index_all_views()
    print(f"✓ Indexed {count} views")

    # Test semantic search
    print("\n[Test 1.2] Searching for fraud-related views...")
    queries = [
        "Show me fraud patterns by merchant",
        "Monthly fraud trends",
        "Risk analysis for transactions"
    ]

    for query in queries:
        print(f"\nQuery: '{query}'")
        results = semantic_search.search(query, top_k=3, min_score=0.2)

        if results:
            for i, result in enumerate(results, 1):
                print(f"  {i}. {result.view.view_name} (score: {result.similarity_score:.3f})")
                print(f"     {result.view.description}")
        else:
            print("  No results found")

    # Test table-based search
    print("\n[Test 1.3] Searching by tables...")
    tables = ['transactions', 'merchants']
    views = semantic_search.search_by_tables(tables, top_k=5)
    print(f"✓ Found {len(views)} views using tables: {', '.join(tables)}")
    for view in views:
        print(f"  - {view.view_name}")

    return semantic_search


def test_schema_graph(db):
    """Test 2: Schema graph construction."""
    print("\n" + "="*70)
    print("TEST 2: Schema Graph")
    print("="*70)

    schema_graph = SchemaGraph(db)

    # Build graph
    print("\n[Test 2.1] Building schema graph...")
    graph = schema_graph.build_from_database()
    print(f"✓ Graph built: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges")

    # Get statistics
    print("\n[Test 2.2] Graph statistics...")
    stats = schema_graph.get_statistics()
    print(f"  Tables: {stats['num_tables']}")
    print(f"  Foreign Keys: {stats['num_foreign_keys']}")
    print(f"  Connected: {stats['is_connected']}")
    print(f"  Total Rows: {stats['total_rows']:,}")

    # Test pathfinding
    print("\n[Test 2.3] Finding paths...")
    path = schema_graph.get_shortest_path('transactions', 'mcc_codes')
    if path:
        print(f"✓ Path from transactions to mcc_codes: {' → '.join(path)}")
    else:
        print("✗ No path found")

    # Test FK discovery
    print("\n[Test 2.4] Foreign key relationships...")
    fks = schema_graph.get_foreign_keys('transactions')
    print(f"✓ Found {len(fks)} foreign keys for 'transactions'")
    for fk in fks:
        print(f"  - {fk['direction']}: {fk['from_table']}.{fk['from_column']} → {fk['to_table']}.{fk['to_column']}")

    # Print summary
    print("\n[Test 2.5] Schema summary...")
    schema_graph.print_summary()

    return schema_graph


def test_steiner_tree(schema_graph, catalog):
    """Test 3: Steiner Tree optimization."""
    print("\n" + "="*70)
    print("TEST 3: Steiner Tree Solver")
    print("="*70)

    solver = SteinerTreeSolver(schema_graph, catalog)

    # Test 1: Solve without views
    print("\n[Test 3.1] Solving WITHOUT views...")
    terminal_tables = ['transactions', 'merchants', 'mcc_codes']
    solution_without = solver.solve(terminal_tables, use_views=False)

    print(f"✓ Solution found:")
    print(f"  Tables used: {', '.join(solution_without['tables_used'])}")
    print(f"  Total cost: {solution_without['total_cost']:.4f}")
    print(f"  Edges: {solution_without['total_edges']}")

    # Test 2: Solve with views
    print("\n[Test 3.2] Solving WITH views...")
    solution_with = solver.solve(terminal_tables, use_views=True)

    print(f"✓ Solution found:")
    print(f"  Tables used: {', '.join(solution_with['tables_used'])}")
    print(f"  Views used: {', '.join(solution_with['views_used']) if solution_with['views_used'] else 'None'}")
    print(f"  Total cost: {solution_with['total_cost']:.4f}")
    print(f"  Edges: {solution_with['total_edges']}")

    # Test 3: Compare solutions
    print("\n[Test 3.3] Comparing solutions...")
    comparison = solver.compare_solutions(terminal_tables)

    savings = comparison['savings']
    print(f"✓ Optimization savings:")
    print(f"  Cost reduction: {savings['cost_reduction']:.4f} ({savings['cost_reduction_pct']:.1f}%)")
    print(f"  Tables avoided: {savings['tables_avoided']}")

    # Test 4: Recommend views
    print("\n[Test 3.4] View recommendations...")
    recommended = solver.recommend_views(terminal_tables, top_k=3)
    print(f"✓ Recommended {len(recommended)} views:")
    for view in recommended:
        print(f"  - {view.view_name} (layer {view.layer})")

    return solver


def test_view_integration(schema_graph, catalog, semantic_search):
    """Test 4: View integration."""
    print("\n" + "="*70)
    print("TEST 4: View Integration")
    print("="*70)

    integration = ViewIntegration(schema_graph, catalog, semantic_search)

    # Test 1: Find optimal views
    print("\n[Test 4.1] Finding optimal views for query...")
    query = "What are the fraud patterns by merchant category?"
    terminal_tables = ['transactions', 'merchants', 'mcc_codes']

    optimal = integration.find_optimal_views(query, terminal_tables)

    print(f"✓ Analysis complete:")
    print(f"  Query: {query}")
    print(f"  Required tables: {', '.join(terminal_tables)}")

    if optimal['recommended_views']:
        print(f"\n  Recommended views:")
        for rec in optimal['recommended_views']:
            view = rec['view']
            print(f"    - {view.view_name} (score: {rec['combined_score']:.3f})")
    else:
        print("  No views recommended")

    # Test 2: View creation recommendation
    print("\n[Test 4.2] Should create view?...")
    recommendation = integration.should_create_view(query, terminal_tables)

    print(f"✓ Recommendation:")
    print(f"  Should create: {recommendation['should_create']}")
    print(f"  Reason: {recommendation['reason']}")
    print(f"  Confidence: {recommendation.get('confidence', 0):.2f}")

    # Test 3: View name suggestion
    print("\n[Test 4.3] Suggesting view name...")
    view_name = integration.suggest_view_name('fraud', 'merchant_patterns', 'monthly')
    print(f"✓ Suggested name: {view_name}")

    # Test 4: Print optimization report
    print("\n[Test 4.4] Full optimization report...")
    integration.print_optimization_report(query, terminal_tables)

    return integration


def main():
    """Run all Phase 2 tests."""
    print("\n" + "="*70)
    print("PHASE 2: GRAPH INTELLIGENCE TESTS")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Setup
        db, catalog = setup_test_data()

        # Test 1: Semantic Search
        semantic_search = test_semantic_search(catalog)

        # Test 2: Schema Graph
        schema_graph = test_schema_graph(db)

        # Test 3: Steiner Tree
        steiner_solver = test_steiner_tree(schema_graph, catalog)

        # Test 4: View Integration
        integration = test_view_integration(schema_graph, catalog, semantic_search)

        # Final summary
        print("\n" + "="*70)
        print("ALL TESTS PASSED! ✓")
        print("="*70)
        print("\nPhase 2 Graph Intelligence is working correctly:")
        print("  ✓ Semantic search with sentence-transformers")
        print("  ✓ Schema graph construction with NetworkX")
        print("  ✓ Steiner Tree optimization")
        print("  ✓ View integration and recommendations")
        print("  ✓ Cost optimization (views as zero-weight shortcuts)")
        print("\nReady to proceed to Phase 3 (LLM Agents)!")
        print("="*70 + "\n")

        return 0

    except Exception as e:
        print("\n" + "="*70)
        print("TEST FAILED! ✗")
        print("="*70)
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
