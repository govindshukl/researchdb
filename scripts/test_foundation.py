#!/usr/bin/env python3
"""
Test script to verify Phase 1 foundation components.
Tests database, view catalog, and view executor.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.schema import SchemaManager, initialize_database
from src.database.view_executor import ViewExecutor
from src.catalog.models import ViewMetadata
from src.catalog.view_catalog import ViewCatalog


def test_database_initialization():
    """Test 1: Database initialization and schema."""
    print("\n" + "="*70)
    print("TEST 1: Database Initialization")
    print("="*70)

    db_path = Path(__file__).parent.parent / "data" / "researchdb.sqlite"

    # Remove existing database for clean test
    if db_path.exists():
        db_path.unlink()
        print("✓ Removed existing database")

    # Initialize database
    print("Initializing database...")
    db = initialize_database(str(db_path))
    print("✓ Database initialized")

    # Verify schema
    schema_manager = SchemaManager(db)
    assert schema_manager.verify_schema(), "Schema verification failed"
    print("✓ Schema verification passed")

    # Get statistics
    stats = schema_manager.get_statistics()
    print(f"✓ Database has {stats['total_tables']} tables")

    return db


def test_sample_data_generation(db):
    """Test 2: Sample data generation."""
    print("\n" + "="*70)
    print("TEST 2: Sample Data Generation")
    print("="*70)

    from generate_sample_data import SampleDataGenerator

    generator = SampleDataGenerator(db)

    print("Generating sample data...")
    generator.generate_all(
        num_customers=50,   # Smaller for quick test
        num_merchants=30,
        num_transactions=200
    )
    print("✓ Sample data generated")

    # Verify data
    txn_count = db.get_row_count('transactions')
    customer_count = db.get_row_count('customers')
    merchant_count = db.get_row_count('merchants')

    print(f"✓ {customer_count} customers created")
    print(f"✓ {merchant_count} merchants created")
    print(f"✓ {txn_count} transactions created")

    # Check fraud distribution
    fraud_query = """
        SELECT
            SUM(fraud_flag) as fraud_count,
            COUNT(*) as total_count
        FROM transactions
    """
    result = db.execute_query(fraud_query)[0]
    fraud_rate = result['fraud_count'] / result['total_count'] * 100
    print(f"✓ Fraud rate: {fraud_rate:.1f}%")

    return True


def test_view_executor(db):
    """Test 3: View executor and DDL validation."""
    print("\n" + "="*70)
    print("TEST 3: View Executor")
    print("="*70)

    executor = ViewExecutor(db)

    # Test 1: Valid view DDL
    print("\n[Test 3.1] Creating valid view...")
    valid_ddl = """
        CREATE VIEW v_test_fraud_summary AS
        SELECT
            COUNT(*) as total_txns,
            SUM(fraud_flag) as fraud_count,
            AVG(amount) as avg_amount
        FROM transactions
    """

    result = executor.create_view(valid_ddl)
    assert result['success'], f"Failed to create view: {result['errors']}"
    print(f"✓ View created: {result['view_name']}")

    # Test 2: Query the view
    print("\n[Test 3.2] Querying view...")
    rows = executor.query_view('v_test_fraud_summary')
    assert len(rows) > 0, "View query returned no results"
    print(f"✓ View query successful: {dict(rows[0])}")

    # Test 3: Invalid view DDL (should fail validation)
    print("\n[Test 3.3] Testing validation (should reject bad DDL)...")
    invalid_ddl = """
        CREATE VIEW invalid_view AS
        SELECT * FROM transactions; DROP TABLE transactions;
    """

    validation = executor.validate_view_ddl(invalid_ddl)
    assert not validation['valid'], "Validation should have failed for dangerous SQL"
    print(f"✓ Validation correctly rejected dangerous SQL: {validation['errors']}")

    # Test 4: Get view definition
    print("\n[Test 3.4] Retrieving view definition...")
    definition = executor.get_view_definition('v_test_fraud_summary')
    assert definition is not None, "Failed to get view definition"
    print(f"✓ View definition retrieved")

    return True


def test_view_catalog(db):
    """Test 4: View catalog operations."""
    print("\n" + "="*70)
    print("TEST 4: View Catalog")
    print("="*70)

    catalog = ViewCatalog(db)

    # Test 1: Register a view
    print("\n[Test 4.1] Registering view in catalog...")
    view1 = ViewMetadata(
        view_name='v_fraud_monthly_trend',
        layer=1,
        domain='fraud',
        description='Monthly fraud aggregation by channel',
        base_tables=['transactions', 'channels'],
        depends_on_views=[],
        created_by_session='test-session-001',
        created_by_role='fraud_analyst',
        created_by_query='What is happening with fraud?',
        view_definition="""
            CREATE VIEW v_fraud_monthly_trend AS
            SELECT
                strftime('%Y-%m', txn_date) as month,
                COUNT(*) as total_txns,
                SUM(fraud_flag) as fraud_count
            FROM transactions
            GROUP BY strftime('%Y-%m', txn_date)
        """,
        tags=['fraud', 'monthly', 'trend', 'channel']
    )

    registered = catalog.register_view(view1)
    assert registered.view_id is not None, "View registration failed"
    print(f"✓ View registered: {registered.view_name} (ID: {registered.view_id})")

    # Test 2: Find view by name
    print("\n[Test 4.2] Finding view by name...")
    found = catalog.find_by_name('v_fraud_monthly_trend')
    assert found is not None, "Failed to find view by name"
    assert found.view_name == view1.view_name, "View name mismatch"
    print(f"✓ Found view: {found.view_name}")

    # Test 3: Register another view (Layer 2)
    print("\n[Test 4.3] Registering Layer 2 view...")
    view2 = ViewMetadata(
        view_name='v_fraud_anomaly_detection',
        layer=2,
        domain='fraud',
        description='Fraud anomaly detection with z-scores',
        base_tables=['transactions'],
        depends_on_views=['v_fraud_monthly_trend'],
        created_by_session='test-session-001',
        view_definition="""
            CREATE VIEW v_fraud_anomaly_detection AS
            SELECT
                month,
                fraud_count,
                AVG(fraud_count) OVER () as avg_fraud,
                fraud_count - AVG(fraud_count) OVER () as deviation
            FROM v_fraud_monthly_trend
        """,
        tags=['fraud', 'anomaly', 'detection']
    )

    registered2 = catalog.register_view(view2)
    print(f"✓ Layer 2 view registered: {registered2.view_name}")

    # Test 4: Find by domain
    print("\n[Test 4.4] Finding views by domain...")
    fraud_views = catalog.find_by_domain('fraud')
    assert len(fraud_views) >= 2, "Should find at least 2 fraud views"
    print(f"✓ Found {len(fraud_views)} fraud views")

    # Test 5: Increment usage
    print("\n[Test 4.5] Testing usage tracking...")
    for i in range(4):  # Increment 4 times to trigger auto-promotion
        catalog.increment_usage('v_fraud_monthly_trend')

    updated = catalog.find_by_name('v_fraud_monthly_trend')
    assert updated.usage_count == 4, "Usage count not incremented"
    assert updated.status == 'PROMOTED', "View should be auto-promoted after 3 uses"
    print(f"✓ Usage tracked: {updated.usage_count} times")
    print(f"✓ Auto-promoted to: {updated.status}")

    # Test 6: Get statistics
    print("\n[Test 4.6] Getting catalog statistics...")
    stats = catalog.get_statistics()
    print(f"✓ Total views: {stats.total_views}")
    print(f"✓ By layer: {dict(stats.by_layer)}")
    print(f"✓ By domain: {dict(stats.by_domain)}")
    print(f"✓ Total usage: {stats.total_usage}")

    # Test 7: View lineage
    print("\n[Test 4.7] Getting view lineage...")
    lineage = catalog.get_view_lineage('v_fraud_anomaly_detection')
    assert len(lineage['upstream_views']) == 1, "Should have 1 upstream dependency"
    assert lineage['upstream_views'][0].view_name == 'v_fraud_monthly_trend', "Wrong upstream view"
    print(f"✓ Lineage tracked correctly")
    print(f"  Base tables: {lineage['base_tables']}")
    print(f"  Upstream: {[v.view_name for v in lineage['upstream_views']]}")
    print(f"  Depth: {lineage['total_depth']}")

    # Test 8: Print catalog
    print("\n[Test 4.8] Printing catalog...")
    catalog.print_catalog()

    return True


def test_integration():
    """Test 5: End-to-end integration test."""
    print("\n" + "="*70)
    print("TEST 5: Integration Test")
    print("="*70)

    db_path = Path(__file__).parent.parent / "data" / "researchdb.sqlite"
    db = initialize_database(str(db_path))

    executor = ViewExecutor(db)
    catalog = ViewCatalog(db)

    # Scenario: Create view via executor, then register in catalog
    print("\n[Test 5.1] Creating view via executor...")
    ddl = """
        CREATE VIEW v_test_channel_stats AS
        SELECT
            channel_id,
            COUNT(*) as txn_count,
            SUM(fraud_flag) as fraud_count,
            AVG(amount) as avg_amount
        FROM transactions
        GROUP BY channel_id
    """

    result = executor.create_view(ddl)
    assert result['success'], "View creation failed"
    print(f"✓ View created in database: {result['view_name']}")

    # Register in catalog
    print("\n[Test 5.2] Registering view in catalog...")
    view_meta = ViewMetadata(
        view_name='v_test_channel_stats',
        layer=1,
        domain='fraud',
        description='Channel statistics for fraud analysis',
        base_tables=['transactions'],
        view_definition=ddl,
        tags=['channel', 'statistics']
    )

    catalog.register_view(view_meta)
    print("✓ View registered in catalog")

    # Query the view
    print("\n[Test 5.3] Querying view...")
    results = executor.query_view('v_test_channel_stats')
    print(f"✓ Query returned {len(results)} rows")
    for row in results[:3]:  # Print first 3
        print(f"  Channel {row['channel_id']}: {row['txn_count']} txns, {row['fraud_count']} fraud")

    # Increment usage
    print("\n[Test 5.4] Simulating usage...")
    catalog.increment_usage('v_test_channel_stats')
    view = catalog.find_by_name('v_test_channel_stats')
    print(f"✓ Usage count: {view.usage_count}")

    return True


def main():
    """Run all tests."""
    print("\n" + "="*70)
    print("RESEARCHDB FOUNDATION TESTS")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Test 1: Database initialization
        db = test_database_initialization()

        # Test 2: Sample data generation
        test_sample_data_generation(db)

        # Test 3: View executor
        test_view_executor(db)

        # Test 4: View catalog
        test_view_catalog(db)

        # Test 5: Integration
        test_integration()

        # Final summary
        print("\n" + "="*70)
        print("ALL TESTS PASSED! ✓")
        print("="*70)
        print("\nPhase 1 Foundation is working correctly:")
        print("  ✓ Database initialization and schema management")
        print("  ✓ Sample data generation with realistic fraud data")
        print("  ✓ View DDL execution with security validation")
        print("  ✓ View catalog with CRUD operations")
        print("  ✓ Usage tracking and auto-promotion")
        print("  ✓ View lineage and dependency tracking")
        print("\nReady to proceed to Phase 2 (Graph Intelligence)!")
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
