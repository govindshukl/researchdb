"""
View integration utilities for schema graph and Steiner Tree.
Provides helper functions to work with views in the graph context.
"""

import logging
from typing import List, Dict, Any, Optional

from .schema_graph import SchemaGraph
from .steiner_tree import SteinerTreeSolver
from ..catalog.view_catalog import ViewCatalog
from ..catalog.semantic_search import SemanticSearch
from ..catalog.models import ViewMetadata

logger = logging.getLogger(__name__)


class ViewIntegration:
    """
    Integrates views with schema graph and query planning.
    Helps agents discover and utilize views for optimal query execution.
    """

    def __init__(
        self,
        schema_graph: SchemaGraph,
        catalog: ViewCatalog,
        semantic_search: Optional[SemanticSearch] = None
    ):
        """
        Initialize view integration.

        Args:
            schema_graph: SchemaGraph instance
            catalog: ViewCatalog instance
            semantic_search: SemanticSearch instance (optional)
        """
        self.schema_graph = schema_graph
        self.catalog = catalog
        self.semantic_search = semantic_search or SemanticSearch(catalog)
        self.steiner_solver = SteinerTreeSolver(schema_graph, catalog)

    def find_optimal_views(
        self,
        query: str,
        terminal_tables: List[str]
    ) -> Dict[str, Any]:
        """
        Find optimal views to use for a query.
        Combines semantic search with Steiner Tree optimization.

        Args:
            query: Natural language query
            terminal_tables: Required tables

        Returns:
            Dict with recommended views and optimization metrics
        """
        logger.info(f"Finding optimal views for query: '{query}'")

        # Step 1: Semantic search for relevant views
        semantic_results = self.semantic_search.suggest_views_for_query(
            query,
            tables=terminal_tables,
            top_k=5
        )

        # Step 2: Get Steiner Tree recommendations
        steiner_recommendations = self.steiner_solver.recommend_views(
            terminal_tables,
            top_k=5
        )

        # Step 3: Combine recommendations (union of both approaches)
        all_recommended = {}

        for result in semantic_results:
            view = result.view
            all_recommended[view.view_name] = {
                'view': view,
                'semantic_score': result.similarity_score,
                'steiner_score': 0,
                'source': 'semantic'
            }

        for view in steiner_recommendations:
            if view.view_name in all_recommended:
                all_recommended[view.view_name]['steiner_score'] = 1
                all_recommended[view.view_name]['source'] = 'both'
            else:
                all_recommended[view.view_name] = {
                    'view': view,
                    'semantic_score': 0,
                    'steiner_score': 1,
                    'source': 'steiner'
                }

        # Step 4: Calculate combined scores
        ranked_views = []
        for view_name, data in all_recommended.items():
            # Combined score: semantic + steiner + usage bonus
            combined_score = (
                data['semantic_score'] * 0.5 +
                data['steiner_score'] * 0.3 +
                (data['view'].usage_count / 100) * 0.2  # Usage bonus
            )

            ranked_views.append({
                'view': data['view'],
                'combined_score': combined_score,
                'semantic_score': data['semantic_score'],
                'steiner_score': data['steiner_score'],
                'source': data['source']
            })

        # Sort by combined score
        ranked_views.sort(key=lambda x: x['combined_score'], reverse=True)

        # Step 5: Get Steiner Tree comparison
        comparison = self.steiner_solver.compare_solutions(terminal_tables)

        return {
            'recommended_views': ranked_views[:3],  # Top 3
            'all_candidates': ranked_views,
            'steiner_comparison': comparison,
            'terminal_tables': terminal_tables,
            'query': query
        }

    def should_create_view(
        self,
        query: str,
        terminal_tables: List[str],
        complexity_threshold: int = 3
    ) -> Dict[str, Any]:
        """
        Determine if a new view should be created for this query.

        Args:
            query: Natural language query
            terminal_tables: Required tables
            complexity_threshold: Min number of tables to consider view creation

        Returns:
            Dict with recommendation and reasoning
        """
        # Don't create views for simple queries
        if len(terminal_tables) < complexity_threshold:
            return {
                'should_create': False,
                'reason': f'Query too simple ({len(terminal_tables)} tables < {complexity_threshold} threshold)',
                'confidence': 0.0
            }

        # Check if similar views already exist
        optimal_views = self.find_optimal_views(query, terminal_tables)

        if optimal_views['recommended_views']:
            best_view = optimal_views['recommended_views'][0]

            # If there's a highly relevant view, don't create new one
            if best_view['combined_score'] > 0.7:
                return {
                    'should_create': False,
                    'reason': f"Existing view '{best_view['view'].view_name}' already covers this (score: {best_view['combined_score']:.2f})",
                    'existing_view': best_view['view'],
                    'confidence': best_view['combined_score']
                }

        # Check Steiner Tree complexity
        steiner_solution = self.steiner_solver.solve(terminal_tables, use_views=False)

        # Create view if:
        # 1. Multiple tables involved
        # 2. No highly relevant existing view
        # 3. High join cost
        confidence = min(
            len(terminal_tables) / 10,  # More tables = higher confidence
            steiner_solution['total_cost'] / 5  # Higher cost = higher confidence
        )

        return {
            'should_create': True,
            'reason': f"Complex query ({len(terminal_tables)} tables, cost {steiner_solution['total_cost']:.2f}) with no existing coverage",
            'confidence': min(confidence, 1.0),
            'suggested_layer': 1,  # Discovery view
            'base_tables': terminal_tables
        }

    def suggest_view_name(
        self,
        domain: str,
        concept: str,
        granularity: Optional[str] = None
    ) -> str:
        """
        Generate a view name following naming convention.

        Args:
            domain: Business domain
            concept: Main concept
            granularity: Time/aggregation granularity (optional)

        Returns:
            Suggested view name
        """
        parts = ['v', domain, concept]

        if granularity:
            parts.append(granularity)

        view_name = '_'.join(parts)

        # Ensure uniqueness
        counter = 1
        original_name = view_name

        while self.catalog.find_by_name(view_name):
            view_name = f"{original_name}_{counter}"
            counter += 1

        return view_name

    def get_view_impact_analysis(self, view_name: str) -> Dict[str, Any]:
        """
        Analyze the impact of a view on query optimization.
        Shows how many queries would benefit from this view.

        Args:
            view_name: Name of the view

        Returns:
            Dict with impact analysis
        """
        view = self.catalog.find_by_name(view_name)
        if not view:
            return {'error': f'View not found: {view_name}'}

        # Get view lineage
        lineage = self.catalog.get_view_lineage(view_name)

        # Find queries that could use this view
        # (queries that need any of the view's base tables)
        potential_queries = []

        # Get all views that could benefit from this view
        for candidate in self.catalog.get_all_views():
            if candidate.view_name == view_name:
                continue

            # Check if candidate uses any of this view's tables
            overlap = set(candidate.base_tables) & set(view.base_tables)

            if overlap:
                potential_queries.append({
                    'view': candidate.view_name,
                    'tables_overlap': list(overlap),
                    'potential_savings': len(overlap)
                })

        return {
            'view': view,
            'usage_count': view.usage_count,
            'base_tables': view.base_tables,
            'downstream_views': len(lineage['downstream_views']),
            'potential_beneficiaries': len(potential_queries),
            'impact_score': view.usage_count + len(potential_queries) * 2,
            'recommendations': potential_queries[:5]  # Top 5
        }

    def refresh_view_graph_cache(self):
        """
        Refresh the cached unified graph with latest views.
        Call this after creating/deleting views.
        """
        self.steiner_solver.unified_graph = None
        logger.info("View graph cache refreshed")

    def print_optimization_report(
        self,
        query: str,
        terminal_tables: List[str]
    ):
        """
        Print a comprehensive optimization report.

        Args:
            query: Natural language query
            terminal_tables: Required tables
        """
        print("\n" + "="*70)
        print("View Optimization Report")
        print("="*70)

        print(f"\nQuery: {query}")
        print(f"Required Tables: {', '.join(terminal_tables)}")

        # Find optimal views
        optimal = self.find_optimal_views(query, terminal_tables)

        print("\n" + "-"*70)
        print("Recommended Views:")
        print("-"*70)

        if optimal['recommended_views']:
            for i, rec in enumerate(optimal['recommended_views'], 1):
                view = rec['view']
                print(f"\n{i}. {view.view_name}")
                print(f"   Layer: {view.layer} | Domain: {view.domain}")
                print(f"   Score: {rec['combined_score']:.3f} (semantic: {rec['semantic_score']:.3f}, steiner: {rec['steiner_score']:.3f})")
                print(f"   Usage: {view.usage_count} times")
                print(f"   Base Tables: {', '.join(view.base_tables)}")
        else:
            print("  No existing views found - consider creating one!")

        # Steiner Tree comparison
        print("\n" + "-"*70)
        print("Steiner Tree Optimization:")
        print("-"*70)

        comparison = optimal['steiner_comparison']

        print(f"\nWithout Views:")
        print(f"  Tables: {', '.join(comparison['without_views']['tables'])}")
        print(f"  Cost: {comparison['without_views']['cost']:.4f}")

        print(f"\nWith Views:")
        print(f"  Tables: {', '.join(comparison['with_views']['tables'])}")
        if comparison['with_views']['views']:
            print(f"  Views: {', '.join(comparison['with_views']['views'])}")
        print(f"  Cost: {comparison['with_views']['cost']:.4f}")

        savings = comparison['savings']
        print(f"\nSavings:")
        print(f"  Cost Reduction: {savings['cost_reduction']:.4f} ({savings['cost_reduction_pct']:.1f}%)")
        print(f"  Tables Avoided: {savings['tables_avoided']}")

        # Creation recommendation
        print("\n" + "-"*70)
        print("View Creation Recommendation:")
        print("-"*70)

        recommendation = self.should_create_view(query, terminal_tables)

        print(f"\nShould Create: {recommendation['should_create']}")
        print(f"Reason: {recommendation['reason']}")
        print(f"Confidence: {recommendation.get('confidence', 0):.2f}")

        print("\n" + "="*70 + "\n")
