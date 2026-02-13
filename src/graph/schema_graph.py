"""
Schema graph construction using NetworkX.
Builds a graph representation of database tables and foreign key relationships.
"""

import logging
import networkx as nx
from typing import List, Dict, Any, Optional, Tuple

from ..database.connection import DatabaseConnection, get_db

logger = logging.getLogger(__name__)


class SchemaGraph:
    """
    Represents the database schema as a directed graph.
    Nodes are tables, edges are foreign key relationships.
    """

    def __init__(self, db: Optional[DatabaseConnection] = None):
        """
        Initialize schema graph.

        Args:
            db: DatabaseConnection instance (uses global if None)
        """
        self.db = db or get_db()
        self.graph = nx.DiGraph()
        self._built = False

    def build_from_database(self) -> nx.DiGraph:
        """
        Build graph from database schema by discovering tables and foreign keys.

        Returns:
            NetworkX DiGraph
        """
        logger.info("Building schema graph from database...")

        # Clear existing graph
        self.graph.clear()

        # Get all tables
        tables = self.db.get_all_tables()
        logger.debug(f"Found {len(tables)} tables")

        # Add table nodes
        for table in tables:
            row_count = self.db.get_row_count(table)
            columns = self.db.get_table_info(table)

            self.graph.add_node(
                table,
                type='table',
                row_count=row_count,
                columns=[col['name'] for col in columns],
                column_count=len(columns)
            )

        # Add foreign key edges
        edge_count = 0
        for table in tables:
            fks = self.db.get_foreign_keys(table)

            for fk in fks:
                # Foreign key goes FROM this table TO referenced table
                from_table = table
                to_table = fk['table']
                from_column = fk['from']
                to_column = fk['to']

                # Calculate edge weight (based on table sizes)
                # Larger tables = higher cost to join
                from_row_count = self.graph.nodes[from_table]['row_count']
                to_row_count = self.graph.nodes[to_table]['row_count']

                # Weight formula: normalized by max table size
                max_rows = max(self.graph.nodes[n]['row_count'] for n in self.graph.nodes)
                weight = (from_row_count + to_row_count) / (2 * max_rows) if max_rows > 0 else 1.0

                self.graph.add_edge(
                    from_table,
                    to_table,
                    type='fk',
                    from_column=from_column,
                    to_column=to_column,
                    weight=weight
                )

                edge_count += 1

        self._built = True
        logger.info(f"Schema graph built: {len(tables)} nodes, {edge_count} edges")

        return self.graph

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a table node.

        Args:
            table_name: Name of the table

        Returns:
            Dict with table metadata
        """
        if table_name not in self.graph:
            raise ValueError(f"Table not in graph: {table_name}")

        node_data = self.graph.nodes[table_name]
        return {
            'name': table_name,
            'type': node_data.get('type'),
            'row_count': node_data.get('row_count'),
            'columns': node_data.get('columns', []),
            'column_count': node_data.get('column_count')
        }

    def get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get all foreign key relationships for a table.

        Args:
            table_name: Name of the table

        Returns:
            List of foreign key info dicts
        """
        if table_name not in self.graph:
            return []

        fks = []

        # Outgoing edges (this table references other tables)
        for successor in self.graph.successors(table_name):
            edge_data = self.graph[table_name][successor]
            if edge_data.get('type') == 'fk':
                fks.append({
                    'from_table': table_name,
                    'to_table': successor,
                    'from_column': edge_data.get('from_column'),
                    'to_column': edge_data.get('to_column'),
                    'direction': 'outgoing'
                })

        # Incoming edges (other tables reference this table)
        for predecessor in self.graph.predecessors(table_name):
            edge_data = self.graph[predecessor][table_name]
            if edge_data.get('type') == 'fk':
                fks.append({
                    'from_table': predecessor,
                    'to_table': table_name,
                    'from_column': edge_data.get('from_column'),
                    'to_column': edge_data.get('to_column'),
                    'direction': 'incoming'
                })

        return fks

    def get_shortest_path(self, source: str, target: str) -> Optional[List[str]]:
        """
        Find shortest path between two tables.

        Args:
            source: Source table
            target: Target table

        Returns:
            List of table names in path, or None if no path exists
        """
        # Convert to undirected for pathfinding (FK can be traversed both ways)
        undirected = self.graph.to_undirected()

        try:
            path = nx.shortest_path(undirected, source, target)
            return path
        except nx.NetworkXNoPath:
            logger.warning(f"No path found between {source} and {target}")
            return None

    def get_subgraph(self, tables: List[str]) -> nx.DiGraph:
        """
        Extract a subgraph containing only specified tables and their connections.

        Args:
            tables: List of table names

        Returns:
            NetworkX DiGraph subgraph
        """
        # Get all nodes that exist in the graph
        valid_tables = [t for t in tables if t in self.graph]

        if not valid_tables:
            return nx.DiGraph()

        # Create subgraph
        subgraph = self.graph.subgraph(valid_tables).copy()

        return subgraph

    def get_connected_tables(
        self,
        table_name: str,
        max_depth: int = 2
    ) -> List[str]:
        """
        Get all tables connected to a given table within max_depth hops.

        Args:
            table_name: Starting table
            max_depth: Maximum distance

        Returns:
            List of connected table names
        """
        if table_name not in self.graph:
            return []

        # Convert to undirected for traversal
        undirected = self.graph.to_undirected()

        # BFS to find all reachable tables
        connected = set()
        visited = set()
        queue = [(table_name, 0)]

        while queue:
            current, depth = queue.pop(0)

            if current in visited or depth > max_depth:
                continue

            visited.add(current)
            connected.add(current)

            # Add neighbors
            for neighbor in undirected.neighbors(current):
                if neighbor not in visited:
                    queue.append((neighbor, depth + 1))

        connected.discard(table_name)  # Remove starting table
        return list(connected)

    def calculate_join_cost(self, table1: str, table2: str) -> float:
        """
        Calculate the cost of joining two tables.

        Args:
            table1: First table
            table2: Second table

        Returns:
            Join cost (lower is better)
        """
        if table1 not in self.graph or table2 not in self.graph:
            return float('inf')

        # If directly connected, use edge weight
        if self.graph.has_edge(table1, table2):
            return self.graph[table1][table2].get('weight', 1.0)

        if self.graph.has_edge(table2, table1):
            return self.graph[table2][table1].get('weight', 1.0)

        # Otherwise, find shortest path and sum weights
        undirected = self.graph.to_undirected()

        try:
            path = nx.shortest_path(undirected, table1, table2)
            total_cost = 0.0

            for i in range(len(path) - 1):
                from_node = path[i]
                to_node = path[i + 1]

                # Get weight from either direction
                if self.graph.has_edge(from_node, to_node):
                    total_cost += self.graph[from_node][to_node].get('weight', 1.0)
                elif self.graph.has_edge(to_node, from_node):
                    total_cost += self.graph[to_node][from_node].get('weight', 1.0)
                else:
                    total_cost += 1.0

            return total_cost

        except nx.NetworkXNoPath:
            return float('inf')

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get graph statistics.

        Returns:
            Dict with statistics
        """
        if not self._built:
            return {'error': 'Graph not built yet'}

        return {
            'num_tables': self.graph.number_of_nodes(),
            'num_foreign_keys': self.graph.number_of_edges(),
            'is_connected': nx.is_weakly_connected(self.graph),
            'num_components': nx.number_weakly_connected_components(self.graph),
            'avg_degree': sum(dict(self.graph.degree()).values()) / self.graph.number_of_nodes()
            if self.graph.number_of_nodes() > 0 else 0,
            'total_rows': sum(
                self.graph.nodes[n].get('row_count', 0)
                for n in self.graph.nodes
            )
        }

    def visualize(self, output_file: Optional[str] = None):
        """
        Visualize the schema graph using matplotlib.

        Args:
            output_file: Path to save image (optional)
        """
        import matplotlib.pyplot as plt

        if not self._built:
            logger.warning("Graph not built yet")
            return

        plt.figure(figsize=(14, 10))

        # Use spring layout for better visualization
        pos = nx.spring_layout(self.graph, k=2, iterations=50)

        # Draw nodes
        node_sizes = [
            self.graph.nodes[n].get('row_count', 100) * 10
            for n in self.graph.nodes
        ]

        nx.draw_networkx_nodes(
            self.graph,
            pos,
            node_size=node_sizes,
            node_color='lightblue',
            alpha=0.7
        )

        # Draw edges
        nx.draw_networkx_edges(
            self.graph,
            pos,
            edge_color='gray',
            arrows=True,
            arrowsize=20,
            alpha=0.5
        )

        # Draw labels
        nx.draw_networkx_labels(
            self.graph,
            pos,
            font_size=10,
            font_weight='bold'
        )

        plt.title("Database Schema Graph", fontsize=16)
        plt.axis('off')
        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            logger.info(f"Graph visualization saved to: {output_file}")
        else:
            plt.show()

    def print_summary(self):
        """Print a text summary of the schema graph."""
        stats = self.get_statistics()

        print("\n" + "="*70)
        print("Schema Graph Summary")
        print("="*70)
        print(f"Tables: {stats['num_tables']}")
        print(f"Foreign Keys: {stats['num_foreign_keys']}")
        print(f"Connected: {stats['is_connected']}")
        print(f"Total Rows: {stats['total_rows']:,}")
        print(f"Average Degree: {stats['avg_degree']:.2f}")
        print("="*70)

        # Print tables sorted by row count
        print("\nTables by Size:")
        tables_by_size = sorted(
            [(n, self.graph.nodes[n].get('row_count', 0)) for n in self.graph.nodes],
            key=lambda x: x[1],
            reverse=True
        )

        for table, count in tables_by_size:
            print(f"  {table:30s}: {count:>10,} rows")

        print("="*70 + "\n")
