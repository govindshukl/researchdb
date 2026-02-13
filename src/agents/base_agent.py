"""
Base agent class with shared utilities for Explorer and Researcher agents.
"""

import logging
from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod

from .llm_client import ClaudeClient, get_claude_client
from ..database.connection import DatabaseConnection, get_db
from ..database.view_executor import ViewExecutor
from ..catalog.view_catalog import ViewCatalog
from ..catalog.semantic_search import SemanticSearch
from ..graph.schema_graph import SchemaGraph
from ..graph.view_integration import ViewIntegration

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """
    Base class for research agents.
    Provides common utilities for database access, LLM calls, and view management.
    """

    def __init__(
        self,
        db: Optional[DatabaseConnection] = None,
        llm_client: Optional[ClaudeClient] = None,
        role: str = "analyst"
    ):
        """
        Initialize base agent.

        Args:
            db: DatabaseConnection instance
            llm_client: ClaudeClient instance
            role: User role for this agent
        """
        self.db = db or get_db()
        self.llm_client = llm_client or get_claude_client()
        self.role = role

        # Initialize components
        self.view_executor = ViewExecutor(self.db)
        self.catalog = ViewCatalog(self.db)
        self.semantic_search = SemanticSearch(self.catalog)
        self.schema_graph = SchemaGraph(self.db)
        self.view_integration = ViewIntegration(
            self.schema_graph,
            self.catalog,
            self.semantic_search
        )

        # Build schema graph
        if not self.schema_graph._built:
            self.schema_graph.build_from_database()

        logger.info(f"{self.__class__.__name__} initialized")

    @abstractmethod
    def process(self, query: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Process a user query.
        Must be implemented by subclasses.

        Args:
            query: User's natural language query
            context: Optional context from previous steps

        Returns:
            Dict with processing results
        """
        pass

    def execute_sql(self, sql: str, params: Optional[tuple] = None) -> List[Dict]:
        """
        Execute a SQL query and return results.

        Args:
            sql: SQL query
            params: Query parameters (optional)

        Returns:
            List of result dicts
        """
        try:
            results = self.db.execute_query(sql, params)
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            raise

    def get_table_sample(self, table_name: str, limit: int = 5) -> List[Dict]:
        """
        Get sample rows from a table.

        Args:
            table_name: Name of the table
            limit: Number of rows to return

        Returns:
            List of sample rows
        """
        sql = f"SELECT * FROM {table_name} LIMIT {limit}"
        return self.execute_sql(sql)

    def get_table_schema(self, table_name: str) -> List[Dict]:
        """
        Get schema information for a table.

        Args:
            table_name: Name of the table

        Returns:
            List of column info dicts
        """
        return self.db.get_table_info(table_name)

    def format_schema_for_llm(self, tables: List[str]) -> str:
        """
        Format table schemas for LLM context.

        Args:
            tables: List of table names

        Returns:
            Formatted schema description
        """
        schema_parts = []

        for table in tables:
            if not self.db.table_exists(table):
                continue

            columns = self.get_table_schema(table)
            row_count = self.db.get_row_count(table)
            sample = self.get_table_sample(table, limit=3)

            schema_parts.append(f"Table: {table}")
            schema_parts.append(f"Rows: {row_count:,}")
            schema_parts.append("Columns:")

            for col in columns:
                null_status = "NOT NULL" if col['notnull'] else "NULL"
                pk_status = "PRIMARY KEY" if col['pk'] else ""
                schema_parts.append(
                    f"  - {col['name']}: {col['type']} {null_status} {pk_status}".strip()
                )

            if sample:
                schema_parts.append("\nSample Data:")
                for i, row in enumerate(sample, 1):
                    schema_parts.append(f"  Row {i}: {dict(row)}")

            schema_parts.append("")  # Blank line

        return "\n".join(schema_parts)

    def call_llm(
        self,
        user_message: str,
        system_prompt: str,
        context: Optional[str] = None,
        temperature: float = 0.7
    ) -> str:
        """
        Make an LLM call with proper formatting.

        Args:
            user_message: User's message
            system_prompt: System prompt
            context: Additional context (optional)
            temperature: Sampling temperature

        Returns:
            LLM response text
        """
        messages = self.llm_client.format_messages(
            user_message=user_message,
            context=context
        )

        response = self.llm_client.chat_completion(
            messages=messages,
            system_prompt=system_prompt,
            temperature=temperature
        )

        return response['content']

    def extract_sql_from_response(self, response: str) -> Optional[str]:
        """
        Extract SQL query from LLM response.
        Looks for SQL in code blocks or CREATE VIEW statements.

        Args:
            response: LLM response text

        Returns:
            Extracted SQL or None
        """
        # Look for SQL code blocks
        if "```sql" in response:
            start = response.find("```sql") + 6
            end = response.find("```", start)
            if end != -1:
                return response[start:end].strip()

        # Look for CREATE VIEW statements
        if "CREATE VIEW" in response.upper():
            # Extract from CREATE VIEW to the end or next major section
            start = response.upper().find("CREATE VIEW")
            sql = response[start:].split("\n\n")[0]  # Take first paragraph
            return sql.strip()

        return None

    def validate_and_execute_view(
        self,
        view_ddl: str,
        view_name: str,
        layer: int,
        domain: str,
        description: str,
        base_tables: List[str],
        tags: List[str]
    ) -> Dict[str, Any]:
        """
        Validate, execute, and register a view.

        Args:
            view_ddl: CREATE VIEW statement
            view_name: Name of the view
            layer: View layer (1, 2, or 3)
            domain: Business domain
            description: View description
            base_tables: List of base tables
            tags: Semantic tags

        Returns:
            Dict with result
        """
        # Validate DDL
        validation = self.view_executor.validate_view_ddl(view_ddl)

        if not validation['valid']:
            return {
                'success': False,
                'view_name': view_name,
                'errors': validation['errors']
            }

        # Execute view creation
        result = self.view_executor.create_view(view_ddl)

        if not result['success']:
            return result

        # Register in catalog
        try:
            from ..catalog.models import ViewMetadata
            view_meta = ViewMetadata(
                view_name=view_name,
                layer=layer,
                domain=domain,
                description=description,
                base_tables=base_tables,
                view_definition=view_ddl,
                tags=tags,
                created_by_role=self.role,
                status='DRAFT'
            )

            self.catalog.register_view(view_meta)

            logger.info(f"View created and registered: {view_name}")

            return {
                'success': True,
                'view_name': view_name,
                'message': f'View {view_name} created successfully',
                'view': view_meta
            }

        except Exception as e:
            logger.error(f"Failed to register view: {e}")
            # Try to drop the view since registration failed
            self.view_executor.drop_view(view_name)

            return {
                'success': False,
                'view_name': view_name,
                'errors': [str(e)]
            }

    def get_relevant_views(self, query: str, tables: List[str]) -> List[Any]:
        """
        Find relevant existing views for a query.

        Args:
            query: User's query
            tables: Required tables

        Returns:
            List of ViewSearchResult
        """
        return self.semantic_search.suggest_views_for_query(
            query,
            tables=tables,
            top_k=5
        )

    def log_activity(self, activity_type: str, details: Dict[str, Any]):
        """
        Log agent activity for observability.

        Args:
            activity_type: Type of activity
            details: Activity details
        """
        logger.info(f"[{self.__class__.__name__}] {activity_type}: {details}")
