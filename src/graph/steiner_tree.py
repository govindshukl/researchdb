"""
Steiner Tree solver with view integration.
Finds the minimal set of tables/views needed to answer a query.
Views act as shortcut edges with zero weight, making them preferred over raw table joins.
"""

import logging
import networkx as nx
from typing import List, Dict, Any, Optional, Set

from .schema_graph import SchemaGraph
from ..catalog.view_catalog import ViewCatalog
from ..catalog.models import ViewMetadata

logger = logging.getLogger(__name__)


class SteinerTreeSolver:
    """
    Solves the Steiner Tree problem for database query optimization.
    Finds the minimum cost subgraph connecting required tables.
    """

    def __init__(
        self,
        schema_graph: SchemaGraph,
        catalog: Optional[ViewCatalog] = None
    ):
        """
        Initialize Steiner Tree solver.

        Args:
            schema_graph: SchemaGraph instance
            catalog: ViewCatalog instance (optional, for view integration)
        """
        self.schema_graph = schema_graph
        self.catalog = catalog
        self.unified_graph = None

    def solve(
        self,
        terminal_tables: List[str],
        use_views: bool = False
    ) -> Dict[str, Any]:
        """
        Solve Steiner Tree problem for given terminal tables.

        Args:
            terminal_tables: List of required table names
            use_views: Whether to consider views as shortcuts

        Returns:
            Dict with solution details:
                - subgraph: NetworkX graph
                - tables_used: List of table names
                - views_used: List of view names (if use_views=True)
                - total_cost: Total weight
                - path_description: Human-readable description
        """
        logger.info(f"Solving Steiner Tree for tables: {terminal_tables}")

        # Validate terminal tables exist
        missing = [t for t in terminal_tables if t not in self.schema_graph.graph]
        if missing:
            raise ValueError(f"Tables not in schema: {missing}")

        if len(terminal_tables) == 0:
            return self._empty_result()

        if len(terminal_tables) == 1:
            return self._single_table_result(terminal_tables[0])

        # Build unified graph (with or without views)
        if use_views and self.catalog:
            graph = self._build_unified_graph()
        else:
            graph = self.schema_graph.graph.to_undirected()

        # Handle disconnected graphs
        if not nx.is_connected(graph):
            logger.warning("Graph is disconnected, working with largest connected component")

            # Get largest connected component
            components = list(nx.connected_components(graph))
            largest_component = max(components, key=len)

            # Create subgraph with only largest component
            graph = graph.subgraph(largest_component).copy()

            # Filter terminals to those in this component
            valid_terminals = [t for t in terminal_tables if t in graph]

            if len(valid_terminals) < len(terminal_tables):
                removed = set(terminal_tables) - set(valid_terminals)
                logger.warning(
                    f"Removed {len(removed)} terminals not in connected component: {removed}"
                )

            if len(valid_terminals) == 0:
                return self._empty_result()

            if len(valid_terminals) == 1:
                return self._single_table_result(valid_terminals[0])

            terminal_tables = valid_terminals

        # Run Steiner Tree approximation
        try:
            steiner_tree = nx.algorithms.approximation.steiner_tree(
                graph,
                terminal_tables,
                weight='weight'
            )

            # Analyze solution
            return self._analyze_solution(steiner_tree, terminal_tables, use_views)

        except Exception as e:
            logger.error(f"Steiner Tree solve failed: {e}")
            raise

    def _build_unified_graph(self) -> nx.Graph:
        """
        Build unified graph with both tables and views.
        Views are added as nodes with zero-weight edges to their base tables.

        Returns:
            NetworkX undirected graph
        """
        logger.debug("Building unified graph with views...")

        # Start with schema graph (undirected)
        graph = self.schema_graph.graph.to_undirected()

        # Get all views from catalog
        views = self.catalog.get_all_views(status='PROMOTED')  # Only use promoted views
        views.extend(self.catalog.get_all_views(status='MATERIALIZED'))

        logger.debug(f"Found {len(views)} promoted/materialized views")

        # Add view nodes and edges
        for view in views:
            # Add view as a node
            graph.add_node(
                view.view_name,
                type='view',
                layer=view.layer,
                domain=view.domain,
                usage_count=view.usage_count,
                base_tables=view.base_tables
            )

            # Add zero-weight edges from view to its base tables
            for table in view.base_tables:
                if table in graph:
                    graph.add_edge(
                        view.view_name,
                        table,
                        type='view_dependency',
                        weight=0.0  # Views are FREE shortcuts!
                    )

        self.unified_graph = graph
        logger.debug(f"Unified graph: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges")

        return graph

    def _analyze_solution(
        self,
        steiner_tree: nx.Graph,
        terminal_tables: List[str],
        used_views: bool
    ) -> Dict[str, Any]:
        """
        Analyze the Steiner Tree solution.

        Args:
            steiner_tree: Solved Steiner Tree
            terminal_tables: Original terminal tables
            used_views: Whether views were considered

        Returns:
            Dict with analysis results
        """
        # Separate nodes into tables and views
        tables_in_solution = []
        views_in_solution = []

        for node in steiner_tree.nodes():
            node_data = steiner_tree.nodes[node]
            if node_data.get('type') == 'view':
                views_in_solution.append(node)
            else:
                tables_in_solution.append(node)

        # Calculate total cost
        total_cost = sum(
            steiner_tree[u][v].get('weight', 1.0)
            for u, v in steiner_tree.edges()
        )

        # Build path description
        path_desc = self._build_path_description(
            steiner_tree,
            terminal_tables,
            tables_in_solution,
            views_in_solution
        )

        result = {
            'subgraph': steiner_tree,
            'terminal_tables': terminal_tables,
            'tables_used': tables_in_solution,
            'views_used': views_in_solution,
            'total_nodes': len(steiner_tree.nodes()),
            'total_edges': len(steiner_tree.edges()),
            'total_cost': round(total_cost, 4),
            'path_description': path_desc,
            'used_views': used_views
        }

        logger.info(
            f"Steiner Tree solution: {len(tables_in_solution)} tables, "
            f"{len(views_in_solution)} views, cost={total_cost:.4f}"
        )

        return result

    def _build_path_description(
        self,
        tree: nx.Graph,
        terminals: List[str],
        tables: List[str],
        views: List[str]
    ) -> str:
        """Build human-readable description of the solution path."""
        lines = []

        lines.append(f"Required tables: {', '.join(terminals)}")

        if views:
            lines.append(f"Views used as shortcuts: {', '.join(views)}")
            # Show what each view covers
            for view_name in views:
                view_data = tree.nodes[view_name]
                base_tables = view_data.get('base_tables', [])
                covered = [t for t in base_tables if t in terminals]
                if covered:
                    lines.append(f"  - {view_name} covers: {', '.join(covered)}")

        # Show additional steiner nodes (tables needed for joins but not in terminals)
        steiner_nodes = [t for t in tables if t not in terminals]
        if steiner_nodes:
            lines.append(f"Additional tables for joins: {', '.join(steiner_nodes)}")

        return "\n".join(lines)

    def _empty_result(self) -> Dict[str, Any]:
        """Return result for empty terminal set."""
        return {
            'subgraph': nx.Graph(),
            'terminal_tables': [],
            'tables_used': [],
            'views_used': [],
            'total_nodes': 0,
            'total_edges': 0,
            'total_cost': 0.0,
            'path_description': 'No tables specified',
            'used_views': False
        }

    def _single_table_result(self, table: str) -> Dict[str, Any]:
        """Return result for single table (no joins needed)."""
        graph = nx.Graph()
        graph.add_node(table)

        return {
            'subgraph': graph,
            'terminal_tables': [table],
            'tables_used': [table],
            'views_used': [],
            'total_nodes': 1,
            'total_edges': 0,
            'total_cost': 0.0,
            'path_description': f'Single table: {table}',
            'used_views': False
        }

    def compare_solutions(
        self,
        terminal_tables: List[str]
    ) -> Dict[str, Any]:
        """
        Compare Steiner Tree solutions with and without views.
        Shows the optimization benefit of using views.

        Args:
            terminal_tables: List of required tables

        Returns:
            Dict with comparison results
        """
        logger.info("Comparing solutions with/without views...")

        # Solve without views
        solution_without = self.solve(terminal_tables, use_views=False)

        # Solve with views (if catalog available)
        if self.catalog:
            solution_with = self.solve(terminal_tables, use_views=True)
        else:
            solution_with = solution_without

        # Calculate savings
        cost_savings = solution_without['total_cost'] - solution_with['total_cost']
        tables_avoided = len(solution_without['tables_used']) - len(solution_with['tables_used'])

        comparison = {
            'without_views': {
                'tables': solution_without['tables_used'],
                'cost': solution_without['total_cost'],
                'edges': solution_without['total_edges']
            },
            'with_views': {
                'tables': solution_with['tables_used'],
                'views': solution_with['views_used'],
                'cost': solution_with['total_cost'],
                'edges': solution_with['total_edges']
            },
            'savings': {
                'cost_reduction': round(cost_savings, 4),
                'cost_reduction_pct': round(
                    cost_savings / solution_without['total_cost'] * 100, 2
                ) if solution_without['total_cost'] > 0 else 0,
                'tables_avoided': tables_avoided
            }
        }

        logger.info(
            f"Comparison: {cost_savings:.4f} cost saved, "
            f"{tables_avoided} tables avoided by using views"
        )

        return comparison

    def recommend_views(
        self,
        terminal_tables: List[str],
        top_k: int = 3
    ) -> List[ViewMetadata]:
        """
        Recommend views that would be useful for the given terminal tables.
        This helps agents decide which views to create.

        Args:
            terminal_tables: Required tables
            top_k: Number of recommendations

        Returns:
            List of ViewMetadata (existing views that cover these tables)
        """
        if not self.catalog:
            return []

        # Find views that cover any of the terminal tables
        relevant_views = self.catalog.find_by_base_tables(terminal_tables)

        # Score views by coverage
        scored_views = []
        for view in relevant_views:
            # How many terminal tables does this view cover?
            coverage = len(set(view.base_tables) & set(terminal_tables))
            # Bonus for higher usage (proven useful)
            score = coverage * 10 + view.usage_count

            scored_views.append((score, view))

        # Sort by score and return top-k
        scored_views.sort(key=lambda x: x[0], reverse=True)
        return [view for score, view in scored_views[:top_k]]

    def visualize_comparison(
        self,
        terminal_tables: List[str],
        output_file: Optional[str] = None
    ):
        """
        Visualize side-by-side comparison of solutions with/without views.

        Args:
            terminal_tables: Required tables
            output_file: Path to save image (optional)
        """
        import matplotlib.pyplot as plt

        comparison = self.compare_solutions(terminal_tables)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

        # Solve for visualization
        sol_without = self.solve(terminal_tables, use_views=False)
        sol_with = self.solve(terminal_tables, use_views=True)

        # Draw without views
        pos1 = nx.spring_layout(sol_without['subgraph'], k=2)
        nx.draw(
            sol_without['subgraph'],
            pos1,
            ax=ax1,
            with_labels=True,
            node_color='lightblue',
            node_size=1500,
            font_size=10,
            font_weight='bold'
        )
        ax1.set_title(
            f"Without Views\n"
            f"Tables: {len(sol_without['tables_used'])}, "
            f"Cost: {sol_without['total_cost']:.2f}",
            fontsize=14
        )

        # Draw with views
        pos2 = nx.spring_layout(sol_with['subgraph'], k=2)

        # Color nodes differently (views vs tables)
        node_colors = []
        for node in sol_with['subgraph'].nodes():
            if node in sol_with['views_used']:
                node_colors.append('lightgreen')  # Views are green
            else:
                node_colors.append('lightblue')   # Tables are blue

        nx.draw(
            sol_with['subgraph'],
            pos2,
            ax=ax2,
            with_labels=True,
            node_color=node_colors,
            node_size=1500,
            font_size=10,
            font_weight='bold'
        )
        ax2.set_title(
            f"With Views\n"
            f"Tables: {len(sol_with['tables_used'])}, "
            f"Views: {len(sol_with['views_used'])}, "
            f"Cost: {sol_with['total_cost']:.2f}",
            fontsize=14
        )

        plt.suptitle(
            f"Steiner Tree Comparison\n"
            f"Cost Reduction: {comparison['savings']['cost_reduction_pct']:.1f}%",
            fontsize=16,
            fontweight='bold'
        )

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            logger.info(f"Comparison visualization saved to: {output_file}")
        else:
            plt.show()
