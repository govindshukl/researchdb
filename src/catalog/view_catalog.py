"""
View Catalog - Registry and CRUD operations for views.
This is the central registry that makes views discoverable and tracks their lifecycle.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

from .models import ViewMetadata, ViewStatistics
from ..database.connection import DatabaseConnection, get_db

logger = logging.getLogger(__name__)


class ViewCatalog:
    """
    Central registry for views with CRUD operations.
    Manages view lifecycle, usage tracking, and discovery.
    """

    def __init__(self, db: Optional[DatabaseConnection] = None):
        """
        Initialize view catalog.

        Args:
            db: DatabaseConnection instance (uses global if None)
        """
        self.db = db or get_db()

    def register_view(self, view: ViewMetadata) -> ViewMetadata:
        """
        Register a new view in the catalog.

        Args:
            view: ViewMetadata instance

        Returns:
            ViewMetadata with populated view_id

        Raises:
            ValueError: If view_name already exists
        """
        # Check if view already exists
        existing = self.find_by_name(view.view_name)
        if existing:
            raise ValueError(f"View already exists: {view.view_name}")

        # Set created_date if not provided
        if not view.created_date:
            view.created_date = datetime.now()

        # Set last_validated if not provided
        if not view.last_validated:
            view.last_validated = datetime.now()

        # Convert to database dict
        db_data = view.to_db_dict()

        # Build INSERT query
        columns = [k for k in db_data.keys() if k != 'view_id' and db_data[k] is not None]
        placeholders = ', '.join(['?' for _ in columns])
        column_names = ', '.join(columns)

        query = f"""
            INSERT INTO view_catalog ({column_names})
            VALUES ({placeholders})
        """

        values = tuple(db_data[col] for col in columns)

        # Execute insert
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, values)
            conn.commit()
            view_id = cursor.lastrowid

        logger.info(f"View registered: {view.view_name} (ID: {view_id})")

        # Retrieve and return the full view
        return self.find_by_id(view_id)

    def find_by_id(self, view_id: int) -> Optional[ViewMetadata]:
        """
        Find a view by its ID.

        Args:
            view_id: View ID

        Returns:
            ViewMetadata if found, None otherwise
        """
        query = "SELECT * FROM view_catalog WHERE view_id = ?"
        results = self.db.execute_query(query, (view_id,))

        if results:
            return ViewMetadata.from_db_row(dict(results[0]))
        return None

    def find_by_name(self, view_name: str) -> Optional[ViewMetadata]:
        """
        Find a view by its name.

        Args:
            view_name: View name

        Returns:
            ViewMetadata if found, None otherwise
        """
        query = "SELECT * FROM view_catalog WHERE view_name = ?"
        results = self.db.execute_query(query, (view_name,))

        if results:
            return ViewMetadata.from_db_row(dict(results[0]))
        return None

    def find_by_domain(self, domain: str, layer: Optional[int] = None) -> List[ViewMetadata]:
        """
        Find views by domain and optionally layer.

        Args:
            domain: Business domain
            layer: View layer (optional)

        Returns:
            List of ViewMetadata
        """
        if layer:
            query = "SELECT * FROM view_catalog WHERE domain = ? AND layer = ? ORDER BY usage_count DESC"
            results = self.db.execute_query(query, (domain, layer))
        else:
            query = "SELECT * FROM view_catalog WHERE domain = ? ORDER BY usage_count DESC"
            results = self.db.execute_query(query, (domain,))

        return [ViewMetadata.from_db_row(dict(row)) for row in results]

    def get_all_views(self, layer: Optional[int] = None, status: Optional[str] = None) -> List[ViewMetadata]:
        """
        Get all views, optionally filtered by layer and/or status.

        Args:
            layer: View layer (optional)
            status: View status (optional)

        Returns:
            List of ViewMetadata
        """
        query = "SELECT * FROM view_catalog WHERE 1=1"
        params = []

        if layer:
            query += " AND layer = ?"
            params.append(layer)

        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY usage_count DESC, created_date DESC"

        results = self.db.execute_query(query, tuple(params) if params else None)
        return [ViewMetadata.from_db_row(dict(row)) for row in results]

    def increment_usage(self, view_name: str) -> bool:
        """
        Increment usage count and update last_used timestamp.

        Args:
            view_name: View name

        Returns:
            True if successful, False otherwise
        """
        query = """
            UPDATE view_catalog
            SET usage_count = usage_count + 1,
                last_used = ?
            WHERE view_name = ?
        """

        try:
            rows_affected = self.db.execute_update(query, (datetime.now().isoformat(), view_name))
            if rows_affected > 0:
                logger.debug(f"Incremented usage for view: {view_name}")

                # Check if auto-promotion threshold reached
                view = self.find_by_name(view_name)
                if view and view.usage_count >= 3 and view.status == 'DRAFT':
                    self.promote_view(view_name)

                return True
            return False
        except Exception as e:
            logger.error(f"Failed to increment usage for {view_name}: {e}")
            return False

    def promote_view(self, view_name: str) -> bool:
        """
        Promote a view from DRAFT to PROMOTED status.

        Args:
            view_name: View name

        Returns:
            True if successful, False otherwise
        """
        query = """
            UPDATE view_catalog
            SET status = 'PROMOTED',
                promoted_date = ?
            WHERE view_name = ? AND status = 'DRAFT'
        """

        try:
            rows_affected = self.db.execute_update(query, (datetime.now().isoformat(), view_name))
            if rows_affected > 0:
                logger.info(f"View promoted: {view_name}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to promote view {view_name}: {e}")
            return False

    def update_view(self, view_name: str, updates: Dict[str, Any]) -> bool:
        """
        Update specific fields of a view.

        Args:
            view_name: View name
            updates: Dict of field names and new values

        Returns:
            True if successful, False otherwise
        """
        if not updates:
            return False

        # Build UPDATE query
        set_clauses = ', '.join([f"{key} = ?" for key in updates.keys()])
        query = f"UPDATE view_catalog SET {set_clauses} WHERE view_name = ?"

        values = tuple(list(updates.values()) + [view_name])

        try:
            rows_affected = self.db.execute_update(query, values)
            if rows_affected > 0:
                logger.debug(f"View updated: {view_name}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to update view {view_name}: {e}")
            return False

    def delete_view(self, view_name: str) -> bool:
        """
        Delete a view from the catalog (soft delete by setting status to ARCHIVED).

        Args:
            view_name: View name

        Returns:
            True if successful, False otherwise
        """
        return self.update_view(view_name, {'status': 'ARCHIVED'})

    def get_statistics(self) -> ViewStatistics:
        """
        Get catalog statistics.

        Returns:
            ViewStatistics object
        """
        all_views = self.get_all_views()

        stats = ViewStatistics(
            total_views=len(all_views),
            by_layer={},
            by_domain={},
            by_status={},
            total_usage=0
        )

        # Count by layer, domain, status
        for view in all_views:
            stats.by_layer[view.layer] = stats.by_layer.get(view.layer, 0) + 1
            stats.by_domain[view.domain] = stats.by_domain.get(view.domain, 0) + 1
            stats.by_status[view.status] = stats.by_status.get(view.status, 0) + 1
            stats.total_usage += view.usage_count

        # Find most used view
        if all_views:
            stats.most_used = max(all_views, key=lambda v: v.usage_count)

        return stats

    def find_by_base_tables(self, table_names: List[str]) -> List[ViewMetadata]:
        """
        Find views that use any of the specified base tables.

        Args:
            table_names: List of table names

        Returns:
            List of ViewMetadata
        """
        all_views = self.get_all_views()
        matching_views = []

        for view in all_views:
            # Check if any of the view's base tables match
            if any(table in view.base_tables for table in table_names):
                matching_views.append(view)

        return matching_views

    def get_view_lineage(self, view_name: str) -> Dict[str, Any]:
        """
        Get the complete lineage of a view (upstream and downstream dependencies).

        Args:
            view_name: View name

        Returns:
            Dict with lineage information
        """
        view = self.find_by_name(view_name)
        if not view:
            return {}

        # Get upstream dependencies (views this view depends on)
        upstream = []
        for dep_view_name in view.depends_on_views:
            dep_view = self.find_by_name(dep_view_name)
            if dep_view:
                upstream.append(dep_view)

        # Get downstream dependencies (views that depend on this view)
        downstream = []
        for user_view_name in view.used_by_views:
            user_view = self.find_by_name(user_view_name)
            if user_view:
                downstream.append(user_view)

        return {
            'view': view,
            'base_tables': view.base_tables,
            'upstream_views': upstream,
            'downstream_views': downstream,
            'total_depth': self._calculate_depth(view)
        }

    def _calculate_depth(self, view: ViewMetadata, visited: Optional[set] = None) -> int:
        """
        Calculate the depth of a view in the dependency tree.

        Args:
            view: ViewMetadata
            visited: Set of visited view names (to prevent cycles)

        Returns:
            Maximum depth
        """
        if visited is None:
            visited = set()

        if view.view_name in visited:
            return 0  # Cycle detected, stop recursion

        visited.add(view.view_name)

        if not view.depends_on_views:
            return 0  # Leaf view

        max_depth = 0
        for dep_view_name in view.depends_on_views:
            dep_view = self.find_by_name(dep_view_name)
            if dep_view:
                depth = self._calculate_depth(dep_view, visited.copy())
                max_depth = max(max_depth, depth + 1)

        return max_depth

    def print_catalog(self, layer: Optional[int] = None):
        """
        Print a formatted catalog listing.

        Args:
            layer: Filter by layer (optional)
        """
        views = self.get_all_views(layer=layer)

        print("\n" + "="*80)
        print("View Catalog")
        print("="*80)

        if not views:
            print("No views in catalog")
            print("="*80 + "\n")
            return

        for view in views:
            print(f"\n[{view.view_name}]")
            print(f"  Layer: {view.layer} | Domain: {view.domain} | Status: {view.status}")
            print(f"  Usage: {view.usage_count} times")
            if view.description:
                print(f"  Description: {view.description}")
            print(f"  Base Tables: {', '.join(view.base_tables)}")
            if view.depends_on_views:
                print(f"  Depends On: {', '.join(view.depends_on_views)}")

        print("\n" + "="*80)

        # Print statistics
        stats = self.get_statistics()
        print(f"\nTotal Views: {stats.total_views}")
        print(f"By Layer: {dict(stats.by_layer)}")
        print(f"By Domain: {dict(stats.by_domain)}")
        print(f"By Status: {dict(stats.by_status)}")
        print(f"Total Usage: {stats.total_usage}")

        if stats.most_used:
            print(f"\nMost Used: {stats.most_used.view_name} ({stats.most_used.usage_count} times)")

        print("="*80 + "\n")
