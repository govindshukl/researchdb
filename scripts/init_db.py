#!/usr/bin/env python3
"""
Initialize the ResearchDB database with schema and sample data.

Usage:
    python scripts/init_db.py [--db-path PATH] [--reset]

Options:
    --db-path PATH  Path to database file (default: data/researchdb.sqlite)
    --reset         Reset database before initialization (drops all tables/views)
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.schema import SchemaManager, initialize_database
from src.database.connection import get_db
from generate_sample_data import SampleDataGenerator


def main():
    """Main initialization function."""
    # Parse arguments
    parser = argparse.ArgumentParser(description="Initialize ResearchDB database")
    parser.add_argument(
        '--db-path',
        type=str,
        default=None,
        help='Path to database file (default: data/researchdb.sqlite)'
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Reset database before initialization'
    )
    parser.add_argument(
        '--no-data',
        action='store_true',
        help='Skip sample data generation (schema only)'
    )

    args = parser.parse_args()

    # Determine database path
    if args.db_path:
        db_path = Path(args.db_path)
    else:
        db_path = Path(__file__).parent.parent / "data" / "researchdb.sqlite"

    # Ensure data directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print("\n" + "="*70)
    print("ResearchDB Initialization")
    print("="*70)
    print(f"\nDatabase: {db_path}")
    print(f"Reset: {args.reset}")
    print(f"Generate Data: {not args.no_data}")
    print("="*70 + "\n")

    # Handle reset if requested
    if args.reset and db_path.exists():
        print("⚠️  Resetting database (all data will be lost)...")
        db = get_db(str(db_path))
        schema_manager = SchemaManager(db)
        schema_manager.reset_database()
        print("✓ Database reset complete\n")

    # Initialize database and schema
    print("Initializing database schema...")
    try:
        db = initialize_database(str(db_path))
        print("✓ Database schema initialized\n")
    except Exception as e:
        print(f"✗ Database initialization failed: {e}")
        sys.exit(1)

    # Generate sample data (unless --no-data)
    if not args.no_data:
        print("Generating sample data...")
        try:
            generator = SampleDataGenerator(db)
            generator.generate_all(
                num_customers=200,
                num_merchants=100,
                num_transactions=1000
            )
            generator.print_statistics()
            print("✓ Sample data generated\n")
        except Exception as e:
            print(f"✗ Sample data generation failed: {e}")
            sys.exit(1)
    else:
        print("⊘ Skipping sample data generation\n")

    # Final verification
    print("Verifying database...")
    schema_manager = SchemaManager(db)
    if schema_manager.verify_schema():
        print("✓ Database verification passed\n")
    else:
        print("✗ Database verification failed\n")
        sys.exit(1)

    # Print final statistics
    stats = schema_manager.get_statistics()
    print("="*70)
    print("Database Ready!")
    print("="*70)
    print(f"\nTables: {stats['total_tables']}")
    print(f"Views:  {stats['total_views']}")
    print(f"Rows:   {stats['total_rows']:,}\n")

    print("Next steps:")
    print("  1. Set ANTHROPIC_API_KEY in .env file")
    print("  2. Run demo: python scripts/run_demo.py")
    print("  3. Or explore interactively: python -i scripts/init_db.py\n")


if __name__ == "__main__":
    main()
