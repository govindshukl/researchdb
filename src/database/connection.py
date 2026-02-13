"""
Database connection manager for SQLite with context manager pattern.
"""

import sqlite3
import logging
from pathlib import Path
from typing import Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """
    Singleton database connection manager with context manager support.

    Usage:
        db = DatabaseConnection('path/to/database.sqlite')
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
    """

    _instance: Optional['DatabaseConnection'] = None
    _db_path: Optional[Path] = None

    def __new__(cls, db_path: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, db_path: Optional[str] = None):
        if db_path and not hasattr(self, '_initialized'):
            self._db_path = Path(db_path)
            self._initialized = True
            logger.info(f"Database connection manager initialized: {self._db_path}")

    @property
    def db_path(self) -> Path:
        """Get the database path."""
        if self._db_path is None:
            raise RuntimeError("Database path not set. Initialize with db_path parameter.")
        return self._db_path

    @contextmanager
    def get_connection(self):
        """
        Get a database connection with automatic cleanup.

        Yields:
            sqlite3.Connection: Database connection

        Example:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM table")
                results = cursor.fetchall()
        """
        conn = None
        try:
            # Ensure parent directory exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            # Create connection
            conn = sqlite3.connect(str(self.db_path))

            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")

            # Return rows as Row objects (dict-like access)
            conn.row_factory = sqlite3.Row

            logger.debug(f"Database connection opened: {self.db_path}")
            yield conn

        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            if conn:
                conn.rollback()
            raise

        finally:
            if conn:
                conn.close()
                logger.debug("Database connection closed")

    def execute_script(self, script: str) -> None:
        """
        Execute a SQL script (multiple statements).

        Args:
            script: SQL script string
        """
        with self.get_connection() as conn:
            try:
                conn.executescript(script)
                conn.commit()
                logger.info("SQL script executed successfully")
            except sqlite3.Error as e:
                logger.error(f"Error executing script: {e}")
                raise

    def execute_script_file(self, script_path: str) -> None:
        """
        Execute a SQL script from a file.

        Args:
            script_path: Path to SQL file
        """
        script_file = Path(script_path)
        if not script_file.exists():
            raise FileNotFoundError(f"SQL script not found: {script_path}")

        script = script_file.read_text()
        self.execute_script(script)
        logger.info(f"SQL script file executed: {script_path}")

    def execute_query(self, query: str, params: Optional[tuple] = None) -> list:
        """
        Execute a SELECT query and return all results.

        Args:
            query: SQL query string
            params: Query parameters (optional)

        Returns:
            List of Row objects (dict-like access)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            logger.debug(f"Query executed, returned {len(results)} rows")
            return results

    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """
        Execute an INSERT/UPDATE/DELETE query.

        Args:
            query: SQL query string
            params: Query parameters (optional)

        Returns:
            Number of rows affected
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            rows_affected = cursor.rowcount
            logger.debug(f"Update executed, {rows_affected} rows affected")
            return rows_affected

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        query = """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?
        """
        results = self.execute_query(query, (table_name,))
        return len(results) > 0

    def view_exists(self, view_name: str) -> bool:
        """
        Check if a view exists in the database.

        Args:
            view_name: Name of the view

        Returns:
            True if view exists, False otherwise
        """
        query = """
            SELECT name FROM sqlite_master
            WHERE type='view' AND name=?
        """
        results = self.execute_query(query, (view_name,))
        return len(results) > 0

    def get_table_info(self, table_name: str) -> list:
        """
        Get column information for a table.

        Args:
            table_name: Name of the table

        Returns:
            List of column info dicts
        """
        query = f"PRAGMA table_info({table_name})"
        return self.execute_query(query)

    def get_foreign_keys(self, table_name: str) -> list:
        """
        Get foreign key relationships for a table.

        Args:
            table_name: Name of the table

        Returns:
            List of foreign key info dicts
        """
        query = f"PRAGMA foreign_key_list({table_name})"
        return self.execute_query(query)

    def get_all_tables(self) -> list[str]:
        """
        Get list of all tables in the database.

        Returns:
            List of table names
        """
        query = """
            SELECT name FROM sqlite_master
            WHERE type='table'
            ORDER BY name
        """
        results = self.execute_query(query)
        return [row['name'] for row in results]

    def get_all_views(self) -> list[str]:
        """
        Get list of all views in the database.

        Returns:
            List of view names
        """
        query = """
            SELECT name FROM sqlite_master
            WHERE type='view'
            ORDER BY name
        """
        results = self.execute_query(query)
        return [row['name'] for row in results]

    def get_row_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table.

        Args:
            table_name: Name of the table

        Returns:
            Number of rows
        """
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(query)
        return result[0]['count'] if result else 0


# Global instance
_db_instance: Optional[DatabaseConnection] = None


def get_db(db_path: Optional[str] = None) -> DatabaseConnection:
    """
    Get the global database instance.

    Args:
        db_path: Path to database file (required on first call)

    Returns:
        DatabaseConnection instance
    """
    global _db_instance
    if _db_instance is None:
        if db_path is None:
            raise RuntimeError("Database path required on first call to get_db()")
        _db_instance = DatabaseConnection(db_path)
    return _db_instance
