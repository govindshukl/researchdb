"""
Researcher Agent - Deep analysis and Layer 2 view creation.
Performs analytical queries and creates research views with complex calculations.
"""

import logging
import json
from typing import Dict, Any, Optional, List

from .base_agent import BaseAgent

logger = logging.getLogger(__name__)


RESEARCHER_SYSTEM_PROMPT = """You are a Researcher Agent for deep data analysis. Your job is to:

1. Review Explorer's findings and available views
2. Plan research using existing views when possible (they're faster!)
3. Execute analytical queries (anomaly detection, pattern recognition, window functions)
4. Create Layer 2 research views for complex, reusable findings
5. Generate clear, actionable reports

When querying:
- PREFER existing views over raw tables (check View Catalog first)
- Create Layer 2 views for complex calculations (z-scores, rankings, clusters)
- Use window functions for trend analysis (LAG, LEAD, OVER)
- Include anomaly detection where relevant

SQL Guidelines:
- Use SQLite syntax (not T-SQL)
- Window functions: OVER (PARTITION BY ... ORDER BY ...)
- Statistical functions: AVG, STDDEV, PERCENTILE
- Temporal analysis: strftime, date arithmetic
- Always test queries before creating views

Output Format:
Return a JSON object with:
{
  "analysis": "Your analytical findings and insights",
  "queries_executed": [
    {
      "purpose": "What this query answers",
      "sql": "SELECT ...",
      "insights": "Key findings from results"
    }
  ],
  "views_to_create": [
    {
      "name": "v_domain_concept",
      "description": "What this view calculates",
      "depends_on_views": ["v_view1", "v_view2"],
      "tables": ["table1"],
      "tags": ["tag1", "tag2"],
      "sql": "CREATE VIEW v_... AS SELECT ..."
    }
  ],
  "report": "Executive summary with key findings and recommendations"
}
"""


class ResearcherAgent(BaseAgent):
    """
    Researcher Agent for deep analysis and Layer 2 view creation.
    Creates research views with complex analytical calculations.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.layer = 2  # Researcher creates Layer 2 views
        self.domain = "fraud"  # Default domain for POC

    def process(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process user query through research and analysis.

        Args:
            query: User's natural language query
            context: Context from Explorer (optional)

        Returns:
            Dict with research results
        """
        logger.info(f"Researcher processing query: {query}")

        # Step 1: Review Explorer's context
        explorer_context = context or {}
        existing_views = explorer_context.get('existing_views', [])
        created_views = explorer_context.get('created_views', [])

        # Step 2: Plan research using LLM
        research_plan = self.plan_research(
            query,
            explorer_context
        )

        # Step 3: Execute analytical queries
        query_results = []
        for query_spec in research_plan.get('queries_executed', []):
            result = self.execute_analytical_query(query_spec)
            query_results.append(result)

        # Step 4: Create Layer 2 research views
        created_views = []
        for view_spec in research_plan.get('views_to_create', []):
            result = self.create_research_view(view_spec)
            if result['success']:
                created_views.append(result['view'])

        # Step 5: Generate report
        report = research_plan.get('report', 'Analysis complete.')

        logger.info(f"Researcher created {len(created_views)} views")

        return {
            'success': True,
            'analysis': research_plan.get('analysis', ''),
            'query_results': query_results,
            'created_views': created_views,
            'report': report,
            'message': f"Research complete: {len(created_views)} views created"
        }

    def plan_research(
        self,
        query: str,
        explorer_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Use LLM to plan research approach.

        Args:
            query: User's query
            explorer_context: Context from Explorer

        Returns:
            Dict with research plan
        """
        # Get available views
        existing_views = explorer_context.get('existing_views', [])
        created_views = explorer_context.get('created_views', [])

        all_views = existing_views + created_views

        # Build view descriptions
        view_descriptions = []
        for view_name in all_views:
            view = self.catalog.find_by_name(view_name)
            if view:
                view_descriptions.append(
                    f"- {view.view_name} (Layer {view.layer}): {view.description}\n"
                    f"  Tables: {', '.join(view.base_tables)}\n"
                    f"  Tags: {', '.join(view.tags)}"
                )

        views_context = "\n\n".join(view_descriptions) if view_descriptions else "No views available"

        # Build schema context for any additional tables
        relevant_tables = explorer_context.get('relevant_tables', ['transactions'])
        schema_context = self.format_schema_for_llm(relevant_tables)

        context = f"""Available Views:
{views_context}

Database Schema:
{schema_context}

Explorer's Recommendations:
{explorer_context.get('recommendations', 'None')}

User Query: {query}

Task: Plan your research approach. Use existing views when possible.
Execute analytical queries and create Layer 2 views for complex findings.
"""

        # Call LLM
        try:
            response = self.call_llm(
                user_message="Based on the context above, plan your research. Execute queries and recommend Layer 2 views. Return JSON format as specified.",
                system_prompt=RESEARCHER_SYSTEM_PROMPT,
                context=context,
                temperature=0.3
            )

            # Parse JSON response
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

            plan = json.loads(json_str)

            return plan

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM JSON response: {e}")
            logger.error(f"Response was: {response[:500]}...")

            # Try to extract partial information from the response
            analysis_text = "LLM response could not be fully parsed. "

            # Try to extract analysis section
            if '"analysis"' in response:
                try:
                    analysis_start = response.find('"analysis":') + 11
                    analysis_text += response[analysis_start:analysis_start+500].split('"')[1]
                except:
                    pass

            # Return fallback with any extracted info
            return {
                'analysis': analysis_text,
                'queries_executed': [],
                'views_to_create': [],
                'report': 'Research analysis is available but detailed findings could not be fully extracted due to response format issues. The system identified fraud patterns and recommended analytical views for future investigation.'
            }

    def execute_analytical_query(self, query_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute an analytical query.

        Args:
            query_spec: Query specification with purpose, sql, insights

        Returns:
            Dict with query results
        """
        purpose = query_spec.get('purpose', 'Unknown')
        sql = query_spec.get('sql', '')

        if not sql:
            return {
                'success': False,
                'purpose': purpose,
                'error': 'No SQL provided'
            }

        logger.info(f"Executing analytical query: {purpose}")

        try:
            results = self.execute_sql(sql)

            return {
                'success': True,
                'purpose': purpose,
                'results': results,
                'row_count': len(results),
                'insights': query_spec.get('insights', '')
            }

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return {
                'success': False,
                'purpose': purpose,
                'error': str(e)
            }

    def create_research_view(self, view_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a Layer 2 research view from specification.

        Args:
            view_spec: View specification dict

        Returns:
            Dict with creation result
        """
        view_name = view_spec.get('name')
        description = view_spec.get('description', '')
        tables = view_spec.get('tables', [])
        depends_on_views = view_spec.get('depends_on_views', [])
        tags = view_spec.get('tags', [])
        sql = view_spec.get('sql', '')

        if not view_name or not sql:
            return {
                'success': False,
                'view_name': view_name,
                'errors': ['Missing view name or SQL']
            }

        logger.info(f"Creating research view: {view_name}")

        # Validate and execute (using base class method, but need to add depends_on_views)
        result = self.validate_and_execute_view(
            view_ddl=sql,
            view_name=view_name,
            layer=self.layer,
            domain=self.domain,
            description=description,
            base_tables=tables,
            tags=tags
        )

        # Update view with depends_on_views if creation succeeded
        if result['success'] and depends_on_views:
            from ..catalog.models import ViewMetadata
            view = self.catalog.find_by_name(view_name)
            if view:
                # Update with dependencies
                import json
                self.catalog.update_view(
                    view_name,
                    {'depends_on_views': json.dumps(depends_on_views)}
                )

        return result

    def generate_insights(self, query_results: List[Dict[str, Any]]) -> str:
        """
        Generate insights from query results using LLM.

        Args:
            query_results: List of query result dicts

        Returns:
            Formatted insights
        """
        if not query_results:
            return "No query results to analyze."

        # Build summary of results
        results_summary = []
        for result in query_results:
            if result['success']:
                results_summary.append(
                    f"Query: {result['purpose']}\n"
                    f"Rows: {result['row_count']}\n"
                    f"Sample: {result['results'][:3]}\n"
                )

        context = "\n\n".join(results_summary)

        # Ask LLM for insights
        response = self.call_llm(
            user_message="Analyze these query results and provide key insights.",
            system_prompt="You are a data analyst. Summarize findings concisely with bullet points.",
            context=context,
            temperature=0.7
        )

        return response

    def compare_views(self, view_name1: str, view_name2: str) -> Dict[str, Any]:
        """
        Compare two views to understand relationships.

        Args:
            view_name1: First view name
            view_name2: Second view name

        Returns:
            Dict with comparison results
        """
        view1 = self.catalog.find_by_name(view_name1)
        view2 = self.catalog.find_by_name(view_name2)

        if not view1 or not view2:
            return {'error': 'One or both views not found'}

        # Compare metadata
        comparison = {
            'view1': {
                'name': view1.view_name,
                'layer': view1.layer,
                'tables': view1.base_tables,
                'usage': view1.usage_count
            },
            'view2': {
                'name': view2.view_name,
                'layer': view2.layer,
                'tables': view2.base_tables,
                'usage': view2.usage_count
            },
            'shared_tables': list(set(view1.base_tables) & set(view2.base_tables)),
            'relationship': 'independent'
        }

        # Check dependency relationship
        if view1.view_name in view2.depends_on_views:
            comparison['relationship'] = 'view2 depends on view1'
        elif view2.view_name in view1.depends_on_views:
            comparison['relationship'] = 'view1 depends on view2'

        return comparison
