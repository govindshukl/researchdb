"""
Pydantic models for View Catalog metadata.
"""

import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator


class ViewMetadata(BaseModel):
    """
    Metadata model for a view in the catalog.
    Corresponds to the view_catalog table schema.
    """

    view_id: Optional[int] = Field(None, description="Auto-generated ID")
    view_name: str = Field(..., description="Unique view name (e.g., v_fraud_monthly_trend)")
    layer: int = Field(..., ge=1, le=3, description="View layer (1=Discovery, 2=Research, 3=Compound)")
    domain: str = Field(..., description="Business domain (fraud, compliance, customer, etc.)")
    description: Optional[str] = Field(None, description="Business-level description")

    # Lineage
    base_tables: List[str] = Field(default_factory=list, description="Base tables used by this view")
    depends_on_views: List[str] = Field(default_factory=list, description="Views this view depends on")
    used_by_views: List[str] = Field(default_factory=list, description="Views that depend on this view")
    steiner_subgraph: Optional[Dict[str, Any]] = Field(None, description="Original Steiner Tree subgraph")

    # Creation context
    created_by_session: Optional[str] = Field(None, description="Session ID that created this view")
    created_by_role: str = Field(default="fraud_analyst", description="User role that created this view")
    created_by_query: Optional[str] = Field(None, description="Original user query that led to this view")
    created_date: Optional[datetime] = Field(None, description="Creation timestamp")

    # Lifecycle
    status: str = Field(default="DRAFT", description="View status")
    promoted_date: Optional[datetime] = Field(None, description="When view was promoted")
    materialized_date: Optional[datetime] = Field(None, description="When view was materialized")

    # Usage tracking
    usage_count: int = Field(default=0, description="Number of times view was queried")
    last_used: Optional[datetime] = Field(None, description="Last usage timestamp")
    avg_query_time_ms: Optional[float] = Field(None, description="Average query time in milliseconds")

    # Freshness
    freshness_type: str = Field(default="LIVE", description="Freshness type (LIVE, STATIC, SCHEDULED)")
    last_validated: Optional[datetime] = Field(None, description="Last validation timestamp")
    is_valid: bool = Field(default=True, description="Whether view is currently valid")

    # Semantic search
    embedding_id: Optional[str] = Field(None, description="Reference to embedding vector")
    tags: List[str] = Field(default_factory=list, description="Semantic tags for discovery")

    # DDL
    view_definition: str = Field(..., description="Full CREATE VIEW statement")

    # Governance
    approved_by: Optional[str] = Field(None, description="Approver name/ID")
    approval_date: Optional[datetime] = Field(None, description="Approval timestamp")
    review_notes: Optional[str] = Field(None, description="Review notes")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

    @field_validator('view_name')
    def validate_view_name(cls, v):
        """Ensure view name starts with v_"""
        if not v.startswith('v_'):
            raise ValueError(f"View name must start with 'v_': {v}")
        return v

    @field_validator('layer')
    def validate_layer(cls, v):
        """Validate layer is 1, 2, or 3"""
        if v not in [1, 2, 3]:
            raise ValueError(f"Layer must be 1, 2, or 3: {v}")
        return v

    @field_validator('domain')
    def validate_domain(cls, v):
        """Validate domain is one of the allowed values"""
        allowed_domains = ['fraud', 'compliance', 'customer', 'merchant', 'transaction', 'risk']
        if v not in allowed_domains:
            raise ValueError(f"Domain must be one of {allowed_domains}: {v}")
        return v

    @field_validator('status')
    def validate_status(cls, v):
        """Validate status is one of the allowed values"""
        allowed_statuses = ['DRAFT', 'PROMOTED', 'MATERIALIZED', 'STALE', 'ARCHIVED']
        if v not in allowed_statuses:
            raise ValueError(f"Status must be one of {allowed_statuses}: {v}")
        return v

    @field_validator('freshness_type')
    def validate_freshness_type(cls, v):
        """Validate freshness type is one of the allowed values"""
        allowed_types = ['LIVE', 'STATIC', 'SCHEDULED']
        if v not in allowed_types:
            raise ValueError(f"Freshness type must be one of {allowed_types}: {v}")
        return v

    def to_db_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for database insertion.
        Serializes lists and dicts to JSON strings.
        """
        data = self.model_dump()

        # Convert lists to JSON
        data['base_tables'] = json.dumps(data['base_tables'])
        data['depends_on_views'] = json.dumps(data['depends_on_views'])
        data['used_by_views'] = json.dumps(data['used_by_views'])
        data['tags'] = json.dumps(data['tags'])

        # Convert steiner_subgraph to JSON
        if data['steiner_subgraph']:
            data['steiner_subgraph'] = json.dumps(data['steiner_subgraph'])

        # Convert datetimes to strings
        for key in ['created_date', 'promoted_date', 'materialized_date', 'last_used', 'last_validated', 'approval_date']:
            if data[key]:
                data[key] = data[key].isoformat()

        # Convert is_valid to int (SQLite doesn't have boolean)
        data['is_valid'] = 1 if data['is_valid'] else 0

        return data

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ViewMetadata':
        """
        Create ViewMetadata from database row.
        Deserializes JSON strings back to lists/dicts.
        """
        data = dict(row)

        # Parse JSON fields
        data['base_tables'] = json.loads(data['base_tables']) if data.get('base_tables') else []
        data['depends_on_views'] = json.loads(data['depends_on_views']) if data.get('depends_on_views') else []
        data['used_by_views'] = json.loads(data['used_by_views']) if data.get('used_by_views') else []
        data['tags'] = json.loads(data['tags']) if data.get('tags') else []

        if data.get('steiner_subgraph'):
            data['steiner_subgraph'] = json.loads(data['steiner_subgraph'])

        # Convert string timestamps to datetime
        for key in ['created_date', 'promoted_date', 'materialized_date', 'last_used', 'last_validated', 'approval_date']:
            if data.get(key):
                data[key] = datetime.fromisoformat(data[key])

        # Convert int to bool
        data['is_valid'] = bool(data.get('is_valid', 1))

        return cls(**data)

    def get_summary(self) -> str:
        """Get a one-line summary of the view."""
        return (
            f"{self.view_name} (L{self.layer}/{self.domain}) - "
            f"{self.status} - used {self.usage_count} times - "
            f"{len(self.base_tables)} tables, {len(self.depends_on_views)} dependencies"
        )


class ViewSearchResult(BaseModel):
    """
    Search result for semantic view discovery.
    """
    view: ViewMetadata
    similarity_score: float = Field(..., ge=0.0, le=1.0, description="Cosine similarity score")

    def __repr__(self):
        return f"<ViewSearchResult: {self.view.view_name} (score={self.similarity_score:.3f})>"


class ViewStatistics(BaseModel):
    """
    Statistics about views in the catalog.
    """
    total_views: int
    by_layer: Dict[int, int] = Field(default_factory=dict)
    by_domain: Dict[str, int] = Field(default_factory=dict)
    by_status: Dict[str, int] = Field(default_factory=dict)
    total_usage: int
    most_used: Optional[ViewMetadata] = None


class SessionInfo(BaseModel):
    """
    Information about a research session.
    """
    session_id: str
    user_role: str
    start_time: datetime
    end_time: Optional[datetime] = None
    user_query: Optional[str] = None
    views_created: int = 0
    views_reused: int = 0
    status: str = "ACTIVE"
