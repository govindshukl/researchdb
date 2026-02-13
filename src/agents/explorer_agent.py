"""
Explorer Agent - Data reconnaissance and Layer 1 view creation.
Profiles data, discovers patterns, and creates discovery views.
"""

import logging
import json
from typing import Dict, Any, Optional, List

from .base_agent import BaseAgent

logger = logging.getLogger(__name__)


EXPLORER_SYSTEM_PROMPT = """You are an Explorer Agent for data reconnaissance. Your job is to:

1. Analyze user queries to identify key entities and data needs
2. Profile relevant tables with exploratory SQL queries
3. Create Layer 1 discovery views that capture useful aggregations
4. Provide clear context for deeper analysis

When creating views:
- Use naming convention: v_{domain}_{concept}_{granularity}
- Example: v_fraud_monthly_trend, v_merchant_risk_profile
- Keep views simple (1-2 table joins, basic aggregations)
- Focus on GROUP BY aggregations, statistical baselines, dimension summaries
- Add clear descriptions for the View Catalog

SQL Guidelines - CRITICAL:
- Use SQLite syntax (not T-SQL)
- Use strftime() for date formatting
- Use ROUND() for decimal precision
- Always include meaningful column aliases
- ONLY use columns that exist in the schema provided
- Common columns: txn_id, customer_id, merchant_id, channel_id, amount, txn_date, fraud_flag, fraud_score, status
- NEVER invent columns unless shown in schema context
- Test queries are valid before creating views

Output Format:
Return a JSON object with:
{
  "analysis": "Your understanding of the query and data needs",
  "views_to_create": [
    {
      "name": "v_domain_concept_granularity",
      "description": "Business-level description",
      "tables": ["table1", "table2"],
      "tags": ["tag1", "tag2"],
      "sql": "CREATE VIEW v_... AS SELECT ..."
    }
  ],
  "recommendations": "What the Researcher should focus on next"
}
"""


class ExplorerAgent(BaseAgent):
    """
    Explorer Agent for data profiling and Layer 1 view creation.
    Creates discovery views that serve as building blocks for research.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.layer = 1  # Explorer creates Layer 1 views
        self.domain = "fraud"  # Default domain for POC

    def process(self, query: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Process user query through exploration.

        Args:
            query: User's natural language query
            context: Optional context

        Returns:
            Dict with exploration results
        """
        logger.info(f"Explorer processing query: {query}")

        # Step 1: Check for existing views
        existing_views = self.check_existing_coverage(query)

        if existing_views:
            logger.info(f"Found {len(existing_views)} existing views that may be relevant")

        # Step 2: Identify relevant tables
        relevant_tables = self.identify_relevant_tables(query)

        # Step 3: Profile the data
        profiling_results = self.profile_tables(relevant_tables)

        # Step 4: Use LLM to determine what views to create
        view_recommendations = self.recommend_views(
            query,
            relevant_tables,
            profiling_results,
            existing_views
        )

        # Step 5: Create recommended views
        created_views = []
        for view_rec in view_recommendations.get('views_to_create', []):
            result = self.create_discovery_view(view_rec)
            if result['success']:
                created_views.append(result['view'])

        # Step 6: Build context for Researcher
        explorer_context = {
            'query': query,
            'relevant_tables': relevant_tables,
            'existing_views': [v.view.view_name for v in existing_views] if existing_views else [],
            'created_views': [v.view_name for v in created_views],
            'profiling_summary': profiling_results,
            'recommendations': view_recommendations.get('recommendations', ''),
            'analysis': view_recommendations.get('analysis', '')
        }

        logger.info(f"Explorer created {len(created_views)} views")

        return {
            'success': True,
            'created_views': created_views,
            'existing_views': existing_views,
            'context': explorer_context,
            'message': f"Exploration complete: {len(created_views)} views created"
        }

    def check_existing_coverage(self, query: str) -> List[Any]:
        """
        Check if existing views already cover this query.

        Args:
            query: User's query

        Returns:
            List of relevant ViewSearchResult
        """
        results = self.semantic_search.search(
            query,
            top_k=5,
            min_score=0.5,
            domain=self.domain
        )

        return results

    def identify_relevant_tables(self, query: str) -> List[str]:
        """
        Identify which tables are relevant for the query.
        Uses simple keyword matching for POC (could use LLM for production).

        Args:
            query: User's query

        Returns:
            List of table names
        """
        query_lower = query.lower()

        # Domain-specific table mapping
        table_keywords = {
            'transactions': ['transaction', 'fraud', 'payment', 'amount', 'txn'],
            'merchants': ['merchant', 'seller', 'vendor', 'store'],
            'customers': ['customer', 'user', 'account', 'buyer'],
            'channels': ['channel', 'atm', 'pos', 'online', 'mobile'],
            'mcc_codes': ['category', 'mcc', 'merchant category', 'industry']
        }

        relevant = set()

        for table, keywords in table_keywords.items():
            if any(kw in query_lower for kw in keywords):
                relevant.add(table)

        # Always include transactions for fraud domain
        if 'fraud' in query_lower or 'transaction' in query_lower:
            relevant.add('transactions')

        return list(relevant) if relevant else ['transactions']

    def profile_tables(self, tables: List[str]) -> Dict[str, Any]:
        """
        Profile tables to understand data distribution.

        Args:
            tables: List of table names

        Returns:
            Dict with profiling results
        """
        profiling = {}

        for table in tables:
            try:
                # Get row count
                row_count = self.db.get_row_count(table)

                # Get sample data
                sample = self.get_table_sample(table, limit=5)

                # Get column info
                columns = self.get_table_schema(table)

                profiling[table] = {
                    'row_count': row_count,
                    'columns': [col['name'] for col in columns],
                    'sample': sample
                }

            except Exception as e:
                logger.warning(f"Failed to profile table {table}: {e}")

        return profiling

    def recommend_views(
        self,
        query: str,
        tables: List[str],
        profiling: Dict[str, Any],
        existing_views: List[Any]
    ) -> Dict[str, Any]:
        """
        Use LLM to recommend which views to create.

        Args:
            query: User's query
            tables: Relevant tables
            profiling: Profiling results
            existing_views: Existing views

        Returns:
            Dict with view recommendations
        """
        # Build context for LLM
        schema_context = self.format_schema_for_llm(tables)

        existing_views_desc = ""
        if existing_views:
            existing_views_desc = "\n\nExisting Views:\n"
            for result in existing_views:
                view = result.view
                existing_views_desc += f"- {view.view_name}: {view.description}\n"

        context = f"""Database Schema:
{schema_context}
{existing_views_desc}

User Query: {query}

Task: Analyze the query and recommend Layer 1 discovery views to create.
Focus on monthly/daily aggregations, statistical baselines, and dimension summaries.
"""

        # Call LLM
        try:
            response = self.call_llm(
                user_message="Based on the context above, what Layer 1 discovery views should I create? Return JSON format as specified.",
                system_prompt=EXPLORER_SYSTEM_PROMPT,
                context=context,
                temperature=0.3  # Lower temperature for more structured output
            )

            # Parse JSON response
            # Extract JSON from markdown code blocks if present
            if "```json" in response:
                start = response.find("```json") + 7
                end = response.find("```", start)
                json_str = response[start:end].strip()
            elif "```" in response:
                start = response.find("```") + 3
                end = response.find("```", start)
                json_str = response[start:end].strip()
            else:
                json_str = response

            recommendations = json.loads(json_str)

            return recommendations

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM JSON response: {e}")
            logger.error(f"Response was: {response[:500]}...")

            # Try to extract partial information from the response
            analysis_text = "LLM response could not be fully parsed. "
            recommendations_text = "Based on the data profiling, the system suggests creating discovery views for the relevant tables. "

            # Try to extract analysis section
            if '"analysis"' in response:
                try:
                    analysis_start = response.find('"analysis":') + 11
                    analysis_text += response[analysis_start:analysis_start+500].split('"')[1]
                except:
                    pass

            # Try to extract recommendations
            if '"recommendations"' in response:
                try:
                    rec_start = response.find('"recommendations":') + 18
                    recommendations_text = response[rec_start:rec_start+500].split('"')[1]
                except:
                    pass

            # Return fallback with any extracted info
            return {
                'analysis': analysis_text,
                'views_to_create': [],
                'recommendations': recommendations_text
            }

    def create_discovery_view(self, view_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a Layer 1 discovery view from specification.

        Args:
            view_spec: View specification dict with name, description, tables, tags, sql

        Returns:
            Dict with creation result
        """
        view_name = view_spec.get('name')
        description = view_spec.get('description', '')
        tables = view_spec.get('tables', [])
        tags = view_spec.get('tags', [])
        sql = view_spec.get('sql', '')

        if not view_name or not sql:
            return {
                'success': False,
                'view_name': view_name,
                'errors': ['Missing view name or SQL']
            }

        logger.info(f"Creating discovery view: {view_name}")

        # Validate and execute
        result = self.validate_and_execute_view(
            view_ddl=sql,
            view_name=view_name,
            layer=self.layer,
            domain=self.domain,
            description=description,
            base_tables=tables,
            tags=tags
        )

        return result

    def quick_profile(self, query: str) -> str:
        """
        Quick profiling summary for a query.
        Useful for debugging or interactive exploration.

        Args:
            query: User's query

        Returns:
            Formatted profiling summary
        """
        tables = self.identify_relevant_tables(query)
        profiling = self.profile_tables(tables)

        summary = ["Quick Data Profile:", "=" * 50]

        for table, info in profiling.items():
            summary.append(f"\nTable: {table}")
            summary.append(f"  Rows: {info['row_count']:,}")
            summary.append(f"  Columns: {', '.join(info['columns'])}")

            if info['sample']:
                summary.append("  Sample:")
                for row in info['sample'][:2]:  # Just 2 rows
                    summary.append(f"    {row}")

        return "\n".join(summary)
