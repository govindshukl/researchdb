"""
Database schema initialization and management.
"""

import logging
from pathlib import Path
from typing import Optional

from .connection import DatabaseConnection, get_db

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages database schema initialization and migrations."""

    def __init__(self, db: Optional[DatabaseConnection] = None):
        """
        Initialize schema manager.

        Args:
            db: DatabaseConnection instance (uses global if None)
        """
        self.db = db or get_db()

    def initialize_schema(self, schema_file: Optional[str] = None) -> None:
        """
        Initialize database schema from SQL file.

        Args:
            schema_file: Path to schema SQL file. Defaults to data/sample_data.sql
        """
        if schema_file is None:
            # Default to project's schema file
            project_root = Path(__file__).parent.parent.parent
            schema_file = project_root / "data" / "sample_data.sql"

        logger.info(f"Initializing database schema from: {schema_file}")

        try:
            self.db.execute_script_file(str(schema_file))
            logger.info("Database schema initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            raise

    def verify_schema(self) -> bool:
        """
        Verify that all required tables exist.

        Returns:
            True if schema is valid, False otherwise
        """
        required_tables = [
            'customers',
            'mcc_codes',
            'merchants',
            'channels',
            'transactions',
            'view_catalog',
            'table_statistics',
            'research_sessions'
        ]

        logger.info("Verifying database schema...")

        missing_tables = []
        for table in required_tables:
            if not self.db.table_exists(table):
                missing_tables.append(table)
                logger.warning(f"Missing table: {table}")

        if missing_tables:
            logger.error(f"Schema validation failed. Missing tables: {missing_tables}")
            return False

        logger.info("Schema validation passed")
        return True

    def get_schema_info(self) -> dict:
        """
        Get comprehensive schema information.

        Returns:
            Dict with table and view information
        """
        tables = self.db.get_all_tables()
        views = self.db.get_all_views()

        table_info = {}
        for table in tables:
            columns = self.db.get_table_info(table)
            foreign_keys = self.db.get_foreign_keys(table)
            row_count = self.db.get_row_count(table)

            table_info[table] = {
                'columns': [
                    {
                        'name': col['name'],
                        'type': col['type'],
                        'notnull': bool(col['notnull']),
                        'pk': bool(col['pk'])
                    }
                    for col in columns
                ],
                'foreign_keys': [
                    {
                        'column': fk['from'],
                        'references_table': fk['table'],
                        'references_column': fk['to']
                    }
                    for fk in foreign_keys
                ],
                'row_count': row_count
            }

        return {
            'tables': table_info,
            'views': views,
            'total_tables': len(tables),
            'total_views': len(views)
        }

    def drop_all_views(self) -> int:
        """
        Drop all views from the database.
        Useful for cleanup in tests or reinit.

        Returns:
            Number of views dropped
        """
        views = self.db.get_all_views()
        count = 0

        with self.db.get_connection() as conn:
            for view in views:
                try:
                    conn.execute(f"DROP VIEW IF EXISTS {view}")
                    logger.debug(f"Dropped view: {view}")
                    count += 1
                except Exception as e:
                    logger.warning(f"Failed to drop view {view}: {e}")
            conn.commit()

        logger.info(f"Dropped {count} views")
        return count

    def reset_database(self) -> None:
        """
        Reset database by dropping all tables and views.
        WARNING: This destroys all data!
        """
        logger.warning("Resetting database - ALL DATA WILL BE LOST")

        with self.db.get_connection() as conn:
            # Drop all views first
            views = self.db.get_all_views()
            for view in views:
                conn.execute(f"DROP VIEW IF EXISTS {view}")

            # Drop all tables
            tables = self.db.get_all_tables()
            for table in tables:
                conn.execute(f"DROP TABLE IF EXISTS {table}")

            conn.commit()

        logger.info("Database reset complete")

    def get_statistics(self) -> dict:
        """
        Get database statistics.

        Returns:
            Dict with database statistics
        """
        tables = self.db.get_all_tables()
        total_rows = sum(self.db.get_row_count(table) for table in tables)

        return {
            'total_tables': len(tables),
            'total_views': len(self.db.get_all_views()),
            'total_rows': total_rows,
            'tables': {
                table: self.db.get_row_count(table)
                for table in tables
            }
        }


def initialize_database(db_path: str, schema_file: Optional[str] = None) -> DatabaseConnection:
    """
    Convenience function to initialize a new database.

    Args:
        db_path: Path to database file
        schema_file: Path to schema SQL file (optional)

    Returns:
        Initialized DatabaseConnection instance
    """
    # Create database connection
    db = DatabaseConnection(db_path)

    # Initialize schema
    schema_manager = SchemaManager(db)
    schema_manager.initialize_schema(schema_file)

    # Verify schema
    if not schema_manager.verify_schema():
        raise RuntimeError("Schema verification failed")

    # Log statistics
    stats = schema_manager.get_statistics()
    logger.info(f"Database initialized: {stats['total_tables']} tables, {stats['total_rows']} rows")

    return db
