"""
View DDL executor - creates and manages SQL views on behalf of agents.
"""

import logging
import re
from typing import Optional, Dict

from .connection import DatabaseConnection, get_db

logger = logging.getLogger(__name__)


class ViewExecutor:
    """
    Executes and validates view DDL statements.
    This module is called by agents to create views safely.
    """

    # Validation rules for view DDL
    ALLOWED_STATEMENTS = ['CREATE VIEW', 'CREATE OR REPLACE VIEW']
    BLOCKED_PATTERNS = [
        r'\bORDER\s+BY\b(?!.*\bTOP\b)',  # ORDER BY without TOP
        r'\bINTO\b',                      # SELECT INTO
        r'\bOPTION\b',                    # Query hints
        r'\bEXEC\b',                      # No EXEC calls
        r'\bEXECUTE\b',                   # No EXECUTE
        r'\bDROP\b',                      # No DROP statements
        r'\bTRUNCATE\b',                  # No TRUNCATE
        r'\bDELETE\b',                    # No DELETE in view def
        r'\bUPDATE\b',                    # No UPDATE in view def
        r'\bINSERT\b',                    # No INSERT in view def
    ]

    def __init__(self, db: Optional[DatabaseConnection] = None):
        """
        Initialize view executor.

        Args:
            db: DatabaseConnection instance (uses global if None)
        """
        self.db = db or get_db()

    def validate_view_ddl(self, ddl: str) -> Dict[str, any]:
        """
        Validate view DDL before execution.

        Args:
            ddl: CREATE VIEW statement

        Returns:
            Dict with validation result:
                {
                    'valid': bool,
                    'errors': List[str],
                    'view_name': str (if parseable)
                }
        """
        errors = []
        view_name = None

        # Check if it starts with allowed statement
        ddl_upper = ddl.upper().strip()
        if not any(ddl_upper.startswith(stmt) for stmt in self.ALLOWED_STATEMENTS):
            errors.append(f"DDL must start with one of: {self.ALLOWED_STATEMENTS}")

        # Extract view name
        view_name_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)', ddl, re.IGNORECASE)
        if view_name_match:
            view_name = view_name_match.group(1)
        else:
            errors.append("Could not extract view name from DDL")

        # Check for blocked patterns
        for pattern in self.BLOCKED_PATTERNS:
            if re.search(pattern, ddl, re.IGNORECASE):
                errors.append(f"Blocked pattern found: {pattern}")

        # Check view naming convention
        if view_name and not view_name.startswith('v_'):
            errors.append(f"View name must start with 'v_': {view_name}")

        # Check for SQL injection patterns
        dangerous_patterns = [
            r';\s*DROP',
            r';\s*DELETE',
            r'--.*DROP',
            r'/\*.*DROP.*\*/',
        ]
        for pattern in dangerous_patterns:
            if re.search(pattern, ddl, re.IGNORECASE):
                errors.append(f"Dangerous SQL pattern detected: {pattern}")

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'view_name': view_name
        }

    def create_view(self, ddl: str, force: bool = False) -> Dict[str, any]:
        """
        Create a view from DDL statement.

        Args:
            ddl: CREATE VIEW statement
            force: If True, skip validation (dangerous!)

        Returns:
            Dict with execution result:
                {
                    'success': bool,
                    'view_name': str,
                    'message': str,
                    'errors': List[str]
                }
        """
        # Validate DDL
        if not force:
            validation = self.validate_view_ddl(ddl)
            if not validation['valid']:
                logger.error(f"View DDL validation failed: {validation['errors']}")
                return {
                    'success': False,
                    'view_name': validation.get('view_name'),
                    'message': 'Validation failed',
                    'errors': validation['errors']
                }

        view_name = validation.get('view_name') if not force else 'unknown'

        # Execute DDL
        try:
            self.db.execute_update(ddl)
            logger.info(f"View created successfully: {view_name}")
            return {
                'success': True,
                'view_name': view_name,
                'message': f'View {view_name} created successfully',
                'errors': []
            }
        except Exception as e:
            logger.error(f"Failed to create view {view_name}: {e}")
            return {
                'success': False,
                'view_name': view_name,
                'message': str(e),
                'errors': [str(e)]
            }

    def drop_view(self, view_name: str) -> bool:
        """
        Drop a view.

        Args:
            view_name: Name of the view to drop

        Returns:
            True if successful, False otherwise
        """
        try:
            self.db.execute_update(f"DROP VIEW IF EXISTS {view_name}")
            logger.info(f"View dropped: {view_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to drop view {view_name}: {e}")
            return False

    def get_view_definition(self, view_name: str) -> Optional[str]:
        """
        Get the DDL definition of a view.

        Args:
            view_name: Name of the view

        Returns:
            View DDL string, or None if view doesn't exist
        """
        query = """
            SELECT sql FROM sqlite_master
            WHERE type='view' AND name=?
        """
        results = self.db.execute_query(query, (view_name,))

        if results:
            return results[0]['sql']
        return None

    def query_view(self, view_name: str, limit: Optional[int] = None) -> list:
        """
        Execute a SELECT query on a view.

        Args:
            view_name: Name of the view
            limit: Optional row limit

        Returns:
            List of Row objects
        """
        if not self.db.view_exists(view_name):
            raise ValueError(f"View does not exist: {view_name}")

        query = f"SELECT * FROM {view_name}"
        if limit:
            query += f" LIMIT {limit}"

        return self.db.execute_query(query)

    def test_view(self, ddl: str) -> Dict[str, any]:
        """
        Test a view DDL by creating and immediately dropping it.
        Useful for validation without side effects.

        Args:
            ddl: CREATE VIEW statement

        Returns:
            Dict with test result
        """
        # Extract view name
        validation = self.validate_view_ddl(ddl)
        if not validation['valid']:
            return {
                'success': False,
                'message': 'Validation failed',
                'errors': validation['errors']
            }

        view_name = validation['view_name']

        # Try creating the view
        result = self.create_view(ddl)
        if not result['success']:
            return result

        # Drop the view immediately
        self.drop_view(view_name)

        return {
            'success': True,
            'message': f'View {view_name} is valid (test passed)',
            'errors': []
        }

    def get_view_columns(self, view_name: str) -> list:
        """
        Get column information for a view.

        Args:
            view_name: Name of the view

        Returns:
            List of column names
        """
        if not self.db.view_exists(view_name):
            raise ValueError(f"View does not exist: {view_name}")

        # Query with LIMIT 0 to get column info without data
        query = f"SELECT * FROM {view_name} LIMIT 0"
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return [description[0] for description in cursor.description]
