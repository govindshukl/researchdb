"""
Semantic search for view discovery using sentence-transformers.
Enables natural language queries to find relevant views in the catalog.
"""

import logging
import numpy as np
from typing import List, Optional, Dict, Any
from sentence_transformers import SentenceTransformer

from .models import ViewMetadata, ViewSearchResult
from .view_catalog import ViewCatalog

logger = logging.getLogger(__name__)


class SemanticSearch:
    """
    Semantic search engine for view discovery.
    Uses sentence-transformers to embed view descriptions and find similar views.
    """

    def __init__(
        self,
        catalog: ViewCatalog,
        model_name: str = "all-MiniLM-L6-v2"
    ):
        """
        Initialize semantic search engine.

        Args:
            catalog: ViewCatalog instance
            model_name: Sentence-transformers model name
        """
        self.catalog = catalog
        self.model_name = model_name
        self.model = None
        self.embeddings_cache: Dict[str, np.ndarray] = {}

        logger.info(f"Semantic search initialized with model: {model_name}")

    def _load_model(self):
        """Lazy load the sentence-transformers model."""
        if self.model is None:
            logger.info(f"Loading sentence-transformers model: {self.model_name}")
            self.model = SentenceTransformer(self.model_name)
            logger.info("Model loaded successfully")

    def embed_text(self, text: str) -> np.ndarray:
        """
        Generate embedding for a text string.

        Args:
            text: Input text

        Returns:
            Embedding vector (numpy array)
        """
        self._load_model()
        embedding = self.model.encode(text, convert_to_numpy=True)
        return embedding

    def embed_view(self, view: ViewMetadata) -> np.ndarray:
        """
        Generate embedding for a view.
        Combines description, tags, and domain for richer representation.

        Args:
            view: ViewMetadata instance

        Returns:
            Embedding vector
        """
        # Check cache first
        if view.view_name in self.embeddings_cache:
            return self.embeddings_cache[view.view_name]

        # Build composite text for embedding
        text_parts = []

        if view.description:
            text_parts.append(view.description)

        # Add domain context
        text_parts.append(f"domain: {view.domain}")

        # Add layer context
        layer_name = {1: "discovery", 2: "research", 3: "compound"}
        text_parts.append(f"layer: {layer_name.get(view.layer, 'unknown')}")

        # Add tags
        if view.tags:
            text_parts.append(f"tags: {', '.join(view.tags)}")

        # Add base tables for context
        if view.base_tables:
            text_parts.append(f"tables: {', '.join(view.base_tables)}")

        composite_text = " | ".join(text_parts)

        # Generate embedding
        embedding = self.embed_text(composite_text)

        # Cache it
        self.embeddings_cache[view.view_name] = embedding

        return embedding

    def index_all_views(self) -> int:
        """
        Index all views in the catalog by generating embeddings.

        Returns:
            Number of views indexed
        """
        logger.info("Indexing all views in catalog...")

        views = self.catalog.get_all_views()
        count = 0

        for view in views:
            self.embed_view(view)
            count += 1

        logger.info(f"Indexed {count} views")
        return count

    def cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """
        Calculate cosine similarity between two vectors.

        Args:
            a: First vector
            b: Second vector

        Returns:
            Similarity score (0-1)
        """
        dot_product = np.dot(a, b)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return float(dot_product / (norm_a * norm_b))

    def search(
        self,
        query: str,
        top_k: int = 5,
        min_score: float = 0.3,
        domain: Optional[str] = None,
        layer: Optional[int] = None
    ) -> List[ViewSearchResult]:
        """
        Search for views semantically similar to the query.

        Args:
            query: Natural language query
            top_k: Number of results to return
            min_score: Minimum similarity score threshold
            domain: Filter by domain (optional)
            layer: Filter by layer (optional)

        Returns:
            List of ViewSearchResult sorted by similarity
        """
        logger.debug(f"Semantic search query: '{query}' (top_k={top_k}, min_score={min_score})")

        # Get query embedding
        query_embedding = self.embed_text(query)

        # Get candidate views
        if domain or layer:
            if domain and layer:
                candidates = self.catalog.find_by_domain(domain, layer)
            elif domain:
                candidates = self.catalog.find_by_domain(domain)
            else:
                candidates = self.catalog.get_all_views(layer=layer)
        else:
            candidates = self.catalog.get_all_views()

        if not candidates:
            logger.warning("No candidate views found")
            return []

        # Calculate similarities
        results = []
        for view in candidates:
            view_embedding = self.embed_view(view)
            similarity = self.cosine_similarity(query_embedding, view_embedding)

            if similarity >= min_score:
                results.append(ViewSearchResult(
                    view=view,
                    similarity_score=similarity
                ))

        # Sort by similarity (descending)
        results.sort(key=lambda x: x.similarity_score, reverse=True)

        # Return top-k
        top_results = results[:top_k]

        logger.info(f"Found {len(top_results)} matching views (from {len(candidates)} candidates)")

        return top_results

    def find_similar_views(
        self,
        view_name: str,
        top_k: int = 5,
        min_score: float = 0.5
    ) -> List[ViewSearchResult]:
        """
        Find views similar to a given view.
        Useful for discovering related views.

        Args:
            view_name: Name of the reference view
            top_k: Number of results to return
            min_score: Minimum similarity score

        Returns:
            List of ViewSearchResult
        """
        # Get reference view
        ref_view = self.catalog.find_by_name(view_name)
        if not ref_view:
            logger.warning(f"View not found: {view_name}")
            return []

        # Get reference embedding
        ref_embedding = self.embed_view(ref_view)

        # Get all views
        all_views = self.catalog.get_all_views()

        # Calculate similarities
        results = []
        for view in all_views:
            if view.view_name == view_name:
                continue  # Skip self

            view_embedding = self.embed_view(view)
            similarity = self.cosine_similarity(ref_embedding, view_embedding)

            if similarity >= min_score:
                results.append(ViewSearchResult(
                    view=view,
                    similarity_score=similarity
                ))

        # Sort and return top-k
        results.sort(key=lambda x: x.similarity_score, reverse=True)
        return results[:top_k]

    def search_by_tables(
        self,
        table_names: List[str],
        top_k: int = 5
    ) -> List[ViewMetadata]:
        """
        Find views that use any of the specified tables.
        This is exact match, not semantic.

        Args:
            table_names: List of table names
            top_k: Number of results to return

        Returns:
            List of ViewMetadata
        """
        views = self.catalog.find_by_base_tables(table_names)

        # Sort by usage count (more used = more valuable)
        views.sort(key=lambda v: v.usage_count, reverse=True)

        return views[:top_k]

    def suggest_views_for_query(
        self,
        query: str,
        tables: Optional[List[str]] = None,
        top_k: int = 3
    ) -> List[ViewSearchResult]:
        """
        Suggest relevant views for a user query.
        Combines semantic search with table-based filtering.

        Args:
            query: User's natural language query
            tables: Tables mentioned in query (optional)
            top_k: Number of suggestions

        Returns:
            List of ViewSearchResult
        """
        logger.info(f"Suggesting views for query: '{query}'")

        # If tables are provided, filter by those first
        if tables:
            # Get views that use these tables
            table_views = self.search_by_tables(tables, top_k=10)

            # If we have table-specific views, do semantic search within them
            if table_views:
                # Create temporary list for semantic ranking
                results = []
                query_embedding = self.embed_text(query)

                for view in table_views:
                    view_embedding = self.embed_view(view)
                    similarity = self.cosine_similarity(query_embedding, view_embedding)
                    results.append(ViewSearchResult(
                        view=view,
                        similarity_score=similarity
                    ))

                results.sort(key=lambda x: x.similarity_score, reverse=True)
                return results[:top_k]

        # Otherwise, do full semantic search
        return self.search(query, top_k=top_k, min_score=0.2)

    def clear_cache(self):
        """Clear the embeddings cache."""
        self.embeddings_cache.clear()
        logger.info("Embeddings cache cleared")

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the embeddings cache.

        Returns:
            Dict with cache statistics
        """
        return {
            'cached_views': len(self.embeddings_cache),
            'model_loaded': self.model is not None,
            'model_name': self.model_name
        }
