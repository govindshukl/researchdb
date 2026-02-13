# Research Agent Architecture v5 — View Hierarchy & Knowledge Crystallization

> **Version:** 5.0  
> **Date:** February 13, 2026  
> **Status:** Draft  
> **Classification:** Internal - Confidential

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [View Hierarchy Model](#view-hierarchy-model)
3. [Updated Architecture Flow](#updated-architecture-flow)
4. [Layer Definitions](#layer-definitions)
5. [View Catalog — The System's Memory](#view-catalog)
6. [View Lifecycle & Governance](#view-lifecycle--governance)
7. [Dependency Graph & Steiner Tree Integration](#dependency-graph--steiner-tree-integration)
8. [Agent Responsibilities (Updated)](#agent-responsibilities-updated)
9. [T-SQL View Creation Patterns](#t-sql-view-creation-patterns)
10. [Security Model for Views](#security-model-for-views)
11. [Performance Optimization](#performance-optimization)
12. [End-to-End Scenario](#end-to-end-scenario)
13. [Zettelkasten Parallel](#zettelkasten-parallel)
14. [Tech Stack Summary](#tech-stack-summary)
15. [Risks & Mitigations](#risks--mitigations)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         ▷ User Query + Auth Token                          │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────────────────┐
│  ◈ ORCHESTRATION                                          LangGraph + FastAPI│
│  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐ ┌───────────────┐ │
│  │Query Decomposer │ │Context Assessor │ │ Task Router  │ │Auth→SESSION_  │ │
│  │                 │ │                 │ │              │ │CONTEXT        │ │
│  └─────────────────┘ └─────────────────┘ └──────────────┘ └───────────────┘ │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────────────────┐
│  ◈ SCHEMA POLICY LAYER                           Lightweight · Git-versioned │
│  [ Column Visibility ] [ Domain Access ] [ T-SQL Validation ] [ View Rules ] │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
              ┌────────────────┴────────────────┐
              ▼                                 ▼
┌─────────────────────────────┐  ┌─────────────────────────────────────────────┐
│  ◎ EXPLORER AGENT           │  │  ◆ SCHEMA INTELLIGENCE                      │
│  Claude Sonnet · Data Scout │  │  Neo4j + Fine-tuned Embeddings              │
│                             │  │                                             │
│  PRE-RESEARCH               │  │  ┌─────────────────┐ ┌───────────────────┐ │
│  ┌──────────┐ ┌───────────┐│  │  │Semantic Catalog │ │FK Graph + View    │ │
│  │Availabil.│ │Context    ││  │  │(tables + views) │ │Dependency Graph   │ │
│  │Scanner   │ │Builder    ││  │  └─────────────────┘ └───────────────────┘ │
│  └──────────┘ └───────────┘│  │  ┌─────────────────┐ ┌───────────────────┐ │
│  MID-RESEARCH               │  │  │⊛ Steiner Tree   │ │Domain Clusters    │ │
│  ┌──────────┐ ┌───────────┐│  │  │(tables + views) │ │                   │ │
│  │Data      │ │Pattern    ││  │  └─────────────────┘ └───────────────────┘ │
│  │Profiler  │ │Spotter    ││  │                                             │
│  └──────────┘ └───────────┘│  │  ┌─────────────────────────────────────────┐ │
│                             │  │  │★ VIEW CATALOG                          │ │
│  ★ VIEW CREATION            │  │  │  Registry of all Explorer/Research     │ │
│  ┌──────────┐ ┌───────────┐│  │  │  views with metadata, embeddings,      │ │
│  │View      │ │View       ││◄─►  │  lineage, usage stats, freshness       │ │
│  │Generator │ │Registrar  ││  │  └─────────────────────────────────────────┘ │
│  └──────────┘ └───────────┘│  │                                             │
└─────────────────────────────┘  └─────────────────────────────────────────────┘
              │                                 │
              └────────────────┬────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  ◉ RESEARCHER AGENT                                  Claude Opus · Analyst   │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌─────────────────────┐ │
│  │Research      │ │Analysis      │ │Cross-        │ │Direction Evaluator  │ │
│  │Planner       │ │Engine        │ │Reference     │ │  ↕ Explorer consult │ │
│  └──────────────┘ └──────────────┘ └──────────────┘ └─────────────────────┘ │
│  ┌──────────────┐ ┌──────────────────────────────────────────────────────┐   │
│  │Report Writer │ │★ View Promoter — crystallize findings as views      │   │
│  └──────────────┘ └──────────────────────────────────────────────────────┘   │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────────────────┐
│  ◇ DATA AGENT                                    LlamaIndex + Claude Sonnet  │
│                                                                              │
│  T-SQL GENERATION PATH                    UNSTRUCTURED DATA PATH             │
│  ┌─────────────────┐ ┌──────────────┐    ┌──────────────┐ ┌──────────────┐  │
│  │T-SQL Generator  │ │Security      │    │Vector Search │ │Doc Parser    │  │
│  │(tables + views) │ │Validator     │    │              │ │              │  │
│  └─────────────────┘ └──────────────┘    └──────────────┘ └──────────────┘  │
│  ┌─────────────────┐ ┌──────────────┐    ┌──────────────┐ ┌──────────────┐  │
│  │Self-Correction  │ │Few-Shot T-SQL│    │Result Cache  │ │Result        │  │
│  │Loop (×3)        │ │Templates     │    │              │ │Sanitizer     │  │
│  └─────────────────┘ └──────────────┘    └──────────────┘ └──────────────┘  │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │★ VIEW DDL EXECUTOR — creates/validates views on behalf of Explorer  │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────────────────┐
│  ⊞ T-SQL EXECUTION & STAGING                       SQL Server T-SQL Engine   │
│  ┌─────────────────┐ ┌──────────────────┐ ┌────────────────────────────────┐│
│  │T-SQL Block      │ │research_results  │ │★ VIEW SCHEMA (dbo.v_*)        ││
│  │Executor         │ │Staging Table     │ │  Persistent analytical views   ││
│  └─────────────────┘ └──────────────────┘ │  Layer 1-3 hierarchy           ││
│  ┌─────────────────┐ ┌──────────────────┐ │  Indexed + SCHEMABINDING      ││
│  │FOR JSON PATH    │ │RLS Auto-Enforce  │ │  RLS applies to all views     ││
│  └─────────────────┘ └──────────────────┘ └────────────────────────────────┘│
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────────────────┐
│  ⛨ SQL SERVER ENGINE SECURITY                   Defense-in-depth · Transparent│
│  [ Row-Level Security ] [ Dynamic Data Masking ] [ Column DENY ] [ Audit ]   │
│  Note: RLS applies equally to base tables AND all views in the hierarchy     │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────────────────┐
│  ▣ DATA INFRASTRUCTURE                                   Persistent Storage  │
│  ┌──────────────┐ ┌────────────┐ ┌──────────┐ ┌─────────┐ ┌──────────────┐ │
│  │SQL Server    │ │research_   │ │pgvector  │ │Qdrant   │ │Azure Blob    │ │
│  │(1000+ tables)│ │results     │ │          │ │         │ │              │ │
│  │+ views (v_*) │ │staging     │ │Schema +  │ │Doc      │ │PDFs,         │ │
│  │              │ │            │ │View embed │ │embeddings│ │reports       │ │
│  └──────────────┘ └────────────┘ └──────────┘ └─────────┘ └──────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────────────────┐
│  ▤ REPORT GENERATION                                          Output Layer   │
│  [ PDF Report ] [ DOCX Report ] [ Dashboard ] [ Markdown ]                   │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│  ◎ OBSERVABILITY & GOVERNANCE                                                │
│  [ Langfuse ] [ Agent Tracing ] [ Policy Audit ] [ T-SQL Steps ]             │
│  [ SQL Server Audit ] [ View Lineage Tracker ] [ Compliance ]                │
└──────────────────────────────────────────────────────────────────────────────┘
```

**★ marks new/modified components for the View Hierarchy feature.**

---

## View Hierarchy Model

The system builds persistent analytical abstractions in layers, where each layer builds on the one below. Raw tables are source material, Explorer-created views are crystallized understanding, and compound views represent emergent higher-order thinking.

```
LAYER 0 — RAW TABLES (existing, 1000+)
│   transactions, merchants, customers, fraud_rules_log, 
│   channel_config, mcc_ref_lkp, acct_bal_snap, ...
│
├── LAYER 1 — DISCOVERY VIEWS (Explorer creates during reconnaissance)
│   │   v_fraud_monthly_trend
│   │   v_merchant_risk_profile  
│   │   v_channel_velocity_stats
│   │   v_customer_segment_activity
│   │
│   ├── LAYER 2 — RESEARCH VIEWS (crystallized during analysis)
│   │   │   v_fraud_anomaly_jan2026
│   │   │   v_electronics_velocity_clusters
│   │   │   v_gift_card_fraud_patterns
│   │   │
│   │   └── LAYER 3 — COMPOUND VIEWS (views on views — higher-order)
│   │           v_fraud_pattern_evolution
│   │           v_merchant_risk_trajectory
│   │           v_quarterly_risk_composite
│   │
│   └── LAYER 2 — RESEARCH VIEWS (from different research sessions)
│           v_kyc_risk_scoring_model
│           v_dormant_account_reactivation_patterns
│
└── LAYER 1 — DISCOVERY VIEWS (from different domains)
        v_compliance_alert_trends
        v_branch_performance_metrics
```

### Layer Definitions

| Layer | Name | Created By | Purpose | Lifecycle |
|-------|------|-----------|---------|-----------|
| 0 | Raw Tables | DBA / ETL | Source data | Permanent, managed by data team |
| 1 | Discovery Views | Explorer Agent | Profiling aggregations, trend baselines, dimension summaries | Auto-created, promoted if reused 3+ times |
| 2 | Research Views | Researcher Agent | Analysis-specific transformations, anomaly calculations, scored datasets | Created when research produces reusable findings |
| 3 | Compound Views | Researcher Agent | Cross-domain insights, views built on L1+L2 views | Highest value, always promoted to permanent |

### What Each Layer Encodes

**Layer 1 (Discovery)** — "What does the data look like?"
- Monthly/daily aggregations by key dimensions
- Statistical baselines (averages, standard deviations)
- Data quality profiles (null rates, cardinality, freshness)
- Dimension lookup joins pre-computed

**Layer 2 (Research)** — "What did we find?"
- Anomaly scores and outlier flags
- Pattern-specific calculations (velocity, frequency, amount clusters)
- Filtered and scored subsets for specific investigations
- Cross-table correlations materialized

**Layer 3 (Compound)** — "What does it mean over time?"
- Trend evolution (is the anomaly growing or shrinking?)
- Cross-domain composite scores
- Trajectory analysis (views comparing this month's L2 to last month's L2)
- Institutional knowledge crystallized as queryable structures

---

## Updated Architecture Flow

### First Research Session

```
User: "What's happening with fraud lately?"
  │
  ▼
Orchestration → Context Assessor: "vague query, route to Explorer"
  │
  ▼
Explorer Agent (Pre-Research Mode)
  ├── Runs profiling queries against raw tables (Layer 0)
  ├── Discovers: fraud spike, channel concentration, merchant anomalies
  ├── ★ CREATES Discovery Views:
  │     CREATE VIEW dbo.v_fraud_monthly_trend AS ...
  │     CREATE VIEW dbo.v_merchant_risk_profile AS ...
  │     CREATE VIEW dbo.v_channel_velocity_stats AS ...
  ├── ★ REGISTERS views in View Catalog with embeddings + metadata
  └── Returns clarifying options to user
  │
  ▼
User: "Deep-dive into January 2026 spike + Electronics angle"
  │
  ▼
Schema Intelligence
  ├── Semantic Catalog now includes Layer 1 views
  ├── Steiner Tree solver considers views as shortcut nodes
  │     v_fraud_monthly_trend replaces [transactions GROUP BY month, channel]
  │     v_merchant_risk_profile replaces [transactions JOIN merchants JOIN mcc_ref_lkp]
  └── Returns subgraph: 2 views + 2 base tables (instead of 5 base tables)
  │
  ▼
Researcher Agent
  ├── Research plan uses existing views as foundation
  ├── T-SQL blocks reference views (simpler, faster SQL)
  ├── Mid-research: asks Explorer about velocity data
  │     Explorer creates v_channel_velocity_stats (Layer 1)
  ├── Analysis produces findings
  └── ★ PROMOTES key findings to Research Views (Layer 2):
        CREATE VIEW dbo.v_fraud_anomaly_jan2026 AS ...
        CREATE VIEW dbo.v_electronics_velocity_clusters AS ...
  │
  ▼
Report Generation → delivers report
```

### Second Research Session (weeks later, different analyst)

```
User: "Is the fraud situation from last month getting better or worse?"
  │
  ▼
Schema Intelligence
  ├── View Catalog already contains:
  │     v_fraud_monthly_trend (L1) — 47 uses, PROMOTED
  │     v_fraud_anomaly_jan2026 (L2) — 12 uses
  │     v_electronics_velocity_clusters (L2) — 8 uses
  ├── Steiner Tree prefers existing views over raw tables
  └── Returns subgraph: 3 views + 0 base tables
  │
  ▼
Explorer Agent
  ├── Checks if existing views cover the time range → YES
  ├── No new Discovery Views needed (reuses L1)
  └── Quick freshness check: views on raw data, always current
  │
  ▼
Researcher Agent
  ├── Builds on L2 views directly
  ├── ★ CREATES Compound View (Layer 3):
  │     CREATE VIEW dbo.v_fraud_pattern_evolution AS
  │     SELECT ... FROM v_fraud_anomaly_jan2026
  │     CROSS APPLY (SELECT ... FROM v_fraud_monthly_trend) ...
  └── Research is 3x faster because foundation already exists
```

### Tenth Research Session (months later)

```
System's View Hierarchy:
  Layer 1: 23 Discovery Views (covering fraud, compliance, customer, branch domains)
  Layer 2: 14 Research Views (specific investigations crystallized)
  Layer 3: 6 Compound Views (cross-domain institutional knowledge)

New query: "Compare fraud patterns with customer onboarding trends"
  │
  ▼
Schema Intelligence
  ├── Finds: v_fraud_pattern_evolution (L3) + v_customer_segment_activity (L1)
  ├── These two views encode months of accumulated analysis
  ├── Steiner Tree connects them via a single base table join
  └── Result: analysis that would have taken days now takes minutes
```

---

## View Catalog

The View Catalog is the registry that makes views discoverable. It lives in both the Schema Intelligence layer (pgvector embeddings for semantic search) and as a metadata table in SQL Server.

### Catalog Schema

```sql
CREATE TABLE dbo.view_catalog (
    view_id             INT IDENTITY(1,1) PRIMARY KEY,
    view_name           NVARCHAR(128) NOT NULL UNIQUE,
    layer               TINYINT NOT NULL,           -- 1=Discovery, 2=Research, 3=Compound
    domain              NVARCHAR(50) NOT NULL,       -- fraud, compliance, customer, branch
    description         NVARCHAR(500) NOT NULL,      -- Business-level description
    
    -- Lineage
    base_tables         NVARCHAR(MAX),               -- JSON array of raw tables used
    depends_on_views    NVARCHAR(MAX),               -- JSON array of view dependencies
    used_by_views       NVARCHAR(MAX),               -- JSON array of dependent views
    steiner_subgraph    NVARCHAR(MAX),               -- Original Steiner Tree that led to this
    
    -- Creation context
    created_by_session  UNIQUEIDENTIFIER NOT NULL,
    created_by_role     NVARCHAR(50) NOT NULL,
    created_by_query    NVARCHAR(MAX),               -- Original user question that led to this
    created_date        DATETIME2 DEFAULT SYSUTCDATETIME(),
    
    -- Lifecycle
    status              NVARCHAR(20) DEFAULT 'DRAFT', -- DRAFT, PROMOTED, MATERIALIZED, STALE, ARCHIVED
    promoted_date       DATETIME2,
    materialized_date   DATETIME2,
    
    -- Usage tracking
    usage_count         INT DEFAULT 0,
    last_used           DATETIME2,
    avg_query_time_ms   FLOAT,
    
    -- Freshness
    freshness_type      NVARCHAR(20),                -- LIVE (view on raw), STATIC (snapshot), SCHEDULED
    last_validated      DATETIME2,
    is_valid            BIT DEFAULT 1,
    
    -- Semantic search
    embedding_id        NVARCHAR(100),               -- Reference to pgvector embedding
    tags                NVARCHAR(MAX),               -- JSON array of semantic tags
    
    -- DDL
    view_definition     NVARCHAR(MAX) NOT NULL,      -- Full CREATE VIEW statement
    
    -- Governance
    approved_by         NVARCHAR(50),
    approval_date       DATETIME2,
    review_notes        NVARCHAR(MAX)
);

-- Index for frequent lookups
CREATE INDEX IX_view_catalog_domain_layer ON dbo.view_catalog(domain, layer);
CREATE INDEX IX_view_catalog_status ON dbo.view_catalog(status);
```

### Catalog Entry Example

```json
{
    "view_name": "v_fraud_monthly_trend",
    "layer": 1,
    "domain": "fraud",
    "description": "Monthly fraud aggregation by channel — count, amount, rate. Covers all channels and full transaction history. Foundation view for most fraud research.",
    
    "base_tables": ["transactions", "channel_config"],
    "depends_on_views": [],
    "used_by_views": ["v_fraud_channel_anomalies", "v_fraud_pattern_evolution"],
    
    "created_by_query": "What's happening with fraud lately?",
    "status": "MATERIALIZED",
    "usage_count": 47,
    
    "freshness_type": "LIVE",
    "tags": ["fraud", "monthly", "channel", "trend", "rate", "baseline"]
}
```

---

## View Lifecycle & Governance

### Lifecycle State Machine

```
                    ┌──────────────────────────────────────┐
                    │                                      │
  CREATE            ▼           3+ uses                    │
  ──────►  [ DRAFT ] ────────────────►  [ PROMOTED ]       │
              │                            │               │
              │ unused 30d                 │ high usage     │
              ▼                            ▼               │
          [ ARCHIVED ]              [ MATERIALIZED ]       │
              │                            │               │
              │ referenced again           │ schema change  │
              └────────────────────────────┼───────────────┘
                                           ▼
                                       [ STALE ]
                                           │
                                    ┌──────┴──────┐
                                    ▼             ▼
                              [ revalidated ] [ ARCHIVED ]
                              back to prev     if unfixable
                              state
```

### Governance Rules

```yaml
# view_governance_policy.yaml

naming:
  pattern: "v_{domain}_{concept}_{granularity}"
  domains: [fraud, compliance, customer, branch, merchant, transaction, risk]
  max_name_length: 128
  examples:
    - v_fraud_monthly_trend
    - v_compliance_alert_weekly_summary
    - v_merchant_risk_profile

creation:
  allowed_creators: [explorer_agent, researcher_agent]  # Never direct user creation
  require_description: true                              # Must have business description
  require_tags: true                                     # Must have semantic tags
  max_base_tables: 10                                    # Prevent overly complex views
  require_schemabinding: preferred                       # When possible

hierarchy:
  max_nesting_depth: 4                                   # L0 → L1 → L2 → L3 (no L4+)
  max_views_per_session: 5                               # Prevent view explosion
  max_total_views: 500                                   # System-wide cap

promotion:
  auto_promote_threshold: 3                              # Uses before auto-promotion
  materialize_threshold: 20                              # Uses before materialization
  require_approval_for_layer3: true                      # Compound views need human review

archival:
  stale_after_days: 90                                   # Unused views
  archive_schema: "archive"                              # Moved to archive.v_* schema
  keep_catalog_entry: true                               # Metadata preserved even after archival

security:
  inherit_rls: true                                      # Views inherit RLS from base tables
  role_domain_access:                                    # Which roles can create in which domains
    fraud_analyst: [fraud, merchant, transaction]
    compliance_officer: [compliance, customer, risk]
    risk_manager: [fraud, compliance, risk, customer, merchant, transaction]
    admin: all
```

### Staleness Detection

```sql
-- Scheduled job: detect stale views when base table schemas change
CREATE PROCEDURE dbo.sp_check_view_staleness
AS
BEGIN
    -- Find views whose base tables have been altered since last validation
    UPDATE vc
    SET vc.status = 'STALE',
        vc.is_valid = 0
    FROM dbo.view_catalog vc
    CROSS APPLY OPENJSON(vc.base_tables) bt
    JOIN sys.tables t ON t.name = bt.value
    WHERE t.modify_date > vc.last_validated;
    
    -- Find views depending on stale views (cascade staleness)
    ;WITH stale_cascade AS (
        SELECT view_name FROM dbo.view_catalog WHERE status = 'STALE'
        UNION ALL
        SELECT vc.view_name
        FROM dbo.view_catalog vc
        CROSS APPLY OPENJSON(vc.depends_on_views) dv
        JOIN stale_cascade sc ON sc.view_name = dv.value
    )
    UPDATE vc
    SET vc.status = 'STALE', vc.is_valid = 0
    FROM dbo.view_catalog vc
    JOIN stale_cascade sc ON vc.view_name = sc.view_name;
END;
```

---

## Dependency Graph & Steiner Tree Integration

### Unified Graph Model

The Steiner Tree solver now operates on a unified graph containing both base tables and views:

```
GRAPH NODES:
  ├── Base Table Nodes (1000+)
  │     Properties: table_name, columns, row_count, domain
  │
  └── View Nodes (grows over time)
        Properties: view_name, layer, columns, base_tables, usage_count
        
GRAPH EDGES:
  ├── FK Edges (base table → base table)
  │     Weight: join_cost (cardinality, selectivity)
  │
  ├── View-to-Base Edges (view → base tables it uses)
  │     Weight: 0 (view already encapsulates the join)
  │
  └── View-to-View Edges (L2/L3 view → L1/L2 view it depends on)
        Weight: 0 (dependency already resolved)
```

### Why Views Are Preferred by Steiner Tree

Views act as **shortcut edges** in the graph. Consider this scenario:

**Without views (5 edges, 5 nodes):**
```
transactions ──FK──► merchants ──FK──► mcc_ref_lkp
     │
     FK
     ▼
fraud_rules_log
     │
     FK
     ▼
channel_config
```
Steiner Tree cost: 5 edges × avg_join_cost = HIGH

**With views (2 edges, 3 nodes):**
```
v_merchant_risk_profile (encapsulates transactions + merchants + mcc_ref_lkp)
     │
     view_edge (cost: 0, join already done)
     ▼
v_channel_velocity_stats (encapsulates transactions + channel_config)
```
Steiner Tree cost: 2 edges × 0 + 1 bridge = LOW

The Steiner Tree naturally prefers the view-based path because it has lower total weight.

### Edge Weight Formula

```python
def compute_edge_weight(source, target, edge_type):
    if edge_type == "view_dependency":
        return 0  # View already encapsulates the join
    
    if edge_type == "fk":
        cardinality_cost = target.row_count / max_row_count
        selectivity_cost = 1 / fk_selectivity
        
        # Check if a view exists that covers this join
        covering_view = find_covering_view(source, target)
        if covering_view and covering_view.status in ('PROMOTED', 'MATERIALIZED'):
            return 0  # Prefer the view
        
        return cardinality_cost * 0.4 + selectivity_cost * 0.4 + base_cost * 0.2
    
    return float('inf')  # Unknown edge type
```

### View Discovery During Steiner Tree

```python
def find_steiner_tree_with_views(terminal_tables, user_role):
    """
    Enhanced Steiner Tree that considers views as shortcut nodes.
    """
    # Step 1: Get policy-filtered schema
    filtered_schema = policy_layer.filter_schema(terminal_tables, user_role)
    
    # Step 2: Find candidate views from View Catalog
    candidate_views = view_catalog.find_covering_views(
        terminal_tables=terminal_tables,
        user_role=user_role,
        min_status='PROMOTED',  # Only use promoted/materialized views
        domain=infer_domain(terminal_tables)
    )
    
    # Step 3: Build unified graph
    graph = build_fk_graph(filtered_schema)
    for view in candidate_views:
        graph.add_node(view.name, type='view', layer=view.layer)
        for base_table in view.base_tables:
            graph.add_edge(view.name, base_table, weight=0, type='view_dependency')
    
    # Step 4: Run Steiner Tree on unified graph
    tree = nx.algorithms.approximation.steiner_tree(graph, terminal_tables)
    
    # Step 5: Report which views were selected
    selected_views = [n for n in tree.nodes if graph.nodes[n].get('type') == 'view']
    selected_tables = [n for n in tree.nodes if graph.nodes[n].get('type') != 'view']
    
    return {
        'subgraph': tree,
        'views_used': selected_views,    # e.g., ['v_fraud_monthly_trend']
        'base_tables_used': selected_tables,  # e.g., ['fraud_rules_log']
        'total_cost': tree_weight(tree),
        'tables_avoided': len(terminal_tables) - len(selected_tables)  # Tables the views replaced
    }
```

---

## Agent Responsibilities (Updated)

### Explorer Agent — Now Creates Views

**Before (v4):** Runs profiling queries, returns results, forgets.  
**After (v5):** Runs profiling queries, **crystallizes findings as views**, registers in View Catalog.

```python
class ExplorerAgent:
    def explore_pre_research(self, query, user_role):
        # 1. Check existing views first
        existing_views = view_catalog.find_relevant(query, domain=infer_domain(query))
        
        if existing_views:
            # Reuse existing views, increment usage count
            for v in existing_views:
                view_catalog.increment_usage(v.view_name)
            return self.build_context_from_views(existing_views, query)
        
        # 2. No existing views — run profiling on raw tables
        profiling_results = self.run_profiling(query, user_role)
        
        # 3. Crystallize profiling as Discovery Views (Layer 1)
        views_created = []
        for result in profiling_results:
            if result.is_reusable():  # Aggregations, baselines, profiles
                view = self.create_discovery_view(result, user_role)
                views_created.append(view)
        
        # 4. Register in View Catalog
        for view in views_created:
            view_catalog.register(view, layer=1, domain=result.domain)
        
        return self.build_context(profiling_results, views_created)
    
    def create_discovery_view(self, profiling_result, user_role):
        """Generate CREATE VIEW DDL from a profiling result."""
        view_name = self.generate_view_name(profiling_result)
        view_ddl = self.generate_view_ddl(profiling_result)
        
        # Validate through Data Agent's security validator
        data_agent.validate_view_ddl(view_ddl)
        
        # Execute through Data Agent
        data_agent.execute_view_ddl(view_ddl)
        
        return View(
            name=view_name,
            ddl=view_ddl,
            description=profiling_result.describe(),
            base_tables=profiling_result.tables_used,
            tags=profiling_result.extract_tags()
        )
```

### Schema Intelligence — Now Includes Views

**Before (v4):** Steiner Tree on base tables only.  
**After (v5):** Steiner Tree on base tables + views. Views are shortcut nodes.

The Semantic Catalog now stores embeddings for both tables and views. When a query arrives, it retrieves relevant tables AND views, and the Steiner Tree decides which combination is optimal.

### Researcher Agent — Now Promotes Views

**Before (v4):** Analyzes and generates report, findings are ephemeral.  
**After (v5):** After analysis, evaluates which findings should be crystallized as Layer 2/3 views.

```python
class ResearcherAgent:
    def post_analysis_crystallize(self, research_session):
        """Evaluate which research findings should become persistent views."""
        
        for finding in research_session.key_findings:
            # Criteria for view promotion
            if finding.is_reusable and finding.complexity > THRESHOLD:
                if finding.uses_existing_views:
                    layer = 3  # Compound view (built on views)
                else:
                    layer = 2  # Research view (built on raw tables)
                
                view = self.create_research_view(finding, layer)
                view_catalog.register(view, layer=layer, status='DRAFT')
    
    def create_research_view(self, finding, layer):
        """Generate a view that crystallizes a research finding."""
        view_name = f"v_{finding.domain}_{finding.concept}_{finding.granularity}"
        
        # The T-SQL from the research becomes the view definition
        view_ddl = f"""
        CREATE VIEW dbo.{view_name} AS
        {finding.sql_that_produced_this}
        """
        
        return View(name=view_name, ddl=view_ddl, layer=layer, ...)
```

### Data Agent — Now Handles View DDL

**Before (v4):** Generates and executes T-SQL query blocks.  
**After (v5):** Also creates, validates, and manages views on behalf of Explorer and Researcher.

New validation rules for view creation:

```python
VIEW_DDL_RULES = {
    'allowed_statements': ['CREATE VIEW', 'ALTER VIEW'],
    'blocked_in_views': [
        'ORDER BY',        # Not allowed in views (without TOP)
        'INTO',            # No SELECT INTO inside views
        'OPTION',          # No query hints in view definitions
    ],
    'required': [
        'WITH SCHEMABINDING',  # Preferred for stability
    ],
    'naming': {
        'prefix': 'dbo.v_',
        'max_length': 128,
        'pattern': r'^dbo\.v_[a-z]+_[a-z_]+$'
    },
    'max_base_tables': 10,
    'max_nesting_depth': 4,  # Prevent view-on-view-on-view-on-view-on-view
}
```

---

## T-SQL View Creation Patterns

### Layer 1 — Discovery View (Explorer)

```sql
-- Monthly fraud trend by channel (reusable across all fraud research)
CREATE VIEW dbo.v_fraud_monthly_trend
WITH SCHEMABINDING
AS
SELECT 
    FORMAT(t.txn_date, 'yyyy-MM') AS month,
    cc.channel_name,
    COUNT_BIG(*) AS total_txns,
    SUM(CASE WHEN t.fraud_flag = 1 THEN 1 ELSE 0 END) AS fraud_count,
    CAST(SUM(CASE WHEN t.fraud_flag = 1 THEN 1 ELSE 0 END) AS FLOAT) 
        / NULLIF(COUNT_BIG(*), 0) AS fraud_rate,
    SUM(CASE WHEN t.fraud_flag = 1 THEN t.amount ELSE 0 END) AS fraud_amount,
    AVG(CASE WHEN t.fraud_flag = 1 THEN t.amount ELSE NULL END) AS avg_fraud_amount
FROM dbo.transactions t
JOIN dbo.channel_config cc ON t.channel_code = cc.channel_code
GROUP BY FORMAT(t.txn_date, 'yyyy-MM'), cc.channel_name;
GO

-- Unique index for materialized view performance
CREATE UNIQUE CLUSTERED INDEX IX_v_fraud_monthly_trend 
ON dbo.v_fraud_monthly_trend(month, channel_name);
```

### Layer 2 — Research View (Researcher)

```sql
-- Fraud channel anomalies with z-scores (built on Layer 1)
CREATE VIEW dbo.v_fraud_channel_anomalies
AS
SELECT 
    fmt.month,
    fmt.channel_name,
    fmt.fraud_rate,
    fmt.fraud_count,
    fmt.fraud_amount,
    AVG(fmt.fraud_rate) OVER (
        PARTITION BY fmt.channel_name 
        ORDER BY fmt.month 
        ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
    ) AS baseline_avg,
    STDEV(fmt.fraud_rate) OVER (
        PARTITION BY fmt.channel_name 
        ORDER BY fmt.month 
        ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
    ) AS baseline_std,
    (fmt.fraud_rate - AVG(fmt.fraud_rate) OVER (
        PARTITION BY fmt.channel_name 
        ORDER BY fmt.month 
        ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
    )) / NULLIF(STDEV(fmt.fraud_rate) OVER (
        PARTITION BY fmt.channel_name 
        ORDER BY fmt.month 
        ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
    ), 0) AS z_score,
    CASE 
        WHEN ABS((fmt.fraud_rate - AVG(fmt.fraud_rate) OVER (...)) 
            / NULLIF(STDEV(fmt.fraud_rate) OVER (...), 0)) > 2.0 
        THEN 1 ELSE 0 
    END AS is_anomaly
FROM dbo.v_fraud_monthly_trend fmt;
```

### Layer 3 — Compound View (Higher-Order)

```sql
-- Fraud pattern evolution — are anomalies growing or shrinking?
-- Built on Layer 2 (which is built on Layer 1)
CREATE VIEW dbo.v_fraud_pattern_evolution
AS
SELECT 
    fca.month,
    fca.channel_name,
    fca.fraud_rate,
    fca.z_score,
    fca.is_anomaly,
    LAG(fca.z_score, 1) OVER (
        PARTITION BY fca.channel_name ORDER BY fca.month
    ) AS prev_month_z,
    LAG(fca.z_score, 3) OVER (
        PARTITION BY fca.channel_name ORDER BY fca.month
    ) AS three_months_ago_z,
    CASE 
        WHEN fca.z_score > LAG(fca.z_score, 1) OVER (
            PARTITION BY fca.channel_name ORDER BY fca.month
        ) THEN 'WORSENING'
        WHEN fca.z_score < LAG(fca.z_score, 1) OVER (
            PARTITION BY fca.channel_name ORDER BY fca.month
        ) THEN 'IMPROVING'
        ELSE 'STABLE'
    END AS trend_direction,
    -- Velocity of change
    fca.z_score - ISNULL(LAG(fca.z_score, 1) OVER (
        PARTITION BY fca.channel_name ORDER BY fca.month
    ), fca.z_score) AS z_score_delta
FROM dbo.v_fraud_channel_anomalies fca;
```

---

## Security Model for Views

### Critical: RLS Applies to All Views

SQL Server RLS security predicates on base tables **automatically propagate through views**. When `v_fraud_monthly_trend` queries `transactions`, RLS filters rows before the view aggregation. This means:

- Sarah (fraud_analyst, Bahrain/GCC region) sees `v_fraud_monthly_trend` with only her authorized branch regions
- Ahmed (risk_manager, all regions) sees the same view with all regions
- The view definition is identical — RLS handles the filtering transparently

```sql
-- RLS predicate on base table
CREATE FUNCTION dbo.fn_rls_transactions(@region NVARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
    RETURN SELECT 1 AS access
    WHERE @region IN (
        SELECT allowed_region 
        FROM dbo.user_role_regions 
        WHERE user_role = CAST(SESSION_CONTEXT(N'user_role') AS NVARCHAR(50))
    );

-- Security policy — applies to base table AND all views that reference it
CREATE SECURITY POLICY dbo.TransactionFilter
ADD FILTER PREDICATE dbo.fn_rls_transactions(branch_region) ON dbo.transactions
WITH (STATE = ON);
```

### View Creation Security

```yaml
# view_security_policy.yaml

creation_rules:
  - Only Explorer Agent and Researcher Agent can create views
  - Views must pass T-SQL Security Validator (same rules as query blocks)
  - No dynamic SQL inside view definitions
  - No system tables or procedures referenced
  - SCHEMABINDING preferred (prevents accidental base table changes)
  - View names must follow naming convention

access_rules:
  - Views inherit RLS from base tables (automatic in SQL Server)
  - DDM applies through views (SQL Server handles this)
  - Column DENY on base tables blocks access through views
  - View catalog tracks which role created which view

audit_rules:
  - Every CREATE VIEW logged with full context
  - Every view query logged via SQL Server Audit
  - View catalog changes tracked with before/after state
```

---

## Performance Optimization

### When to Materialize (Indexed Views)

```sql
-- Auto-materialization trigger: when usage_count > 20
-- The Observability layer monitors and recommends materialization

-- Materialized indexed view (Layer 1 — high reuse)
CREATE VIEW dbo.v_fraud_monthly_trend
WITH SCHEMABINDING  -- Required for indexed views in SQL Server
AS
SELECT 
    FORMAT(t.txn_date, 'yyyy-MM') AS month,
    cc.channel_name,
    COUNT_BIG(*) AS total_txns,  -- COUNT_BIG required for indexed views
    SUM(CASE WHEN t.fraud_flag = 1 THEN 1 ELSE 0 END) AS fraud_count,
    SUM(t.amount) AS total_amount
FROM dbo.transactions t
JOIN dbo.channel_config cc ON t.channel_code = cc.channel_code
GROUP BY FORMAT(t.txn_date, 'yyyy-MM'), cc.channel_name;
GO

-- Unique clustered index materializes the view
CREATE UNIQUE CLUSTERED INDEX IX_v_fraud_monthly
ON dbo.v_fraud_monthly_trend(month, channel_name);
```

### Performance Monitoring

```sql
-- Query to track view performance and identify materialization candidates
SELECT 
    vc.view_name,
    vc.layer,
    vc.usage_count,
    vc.avg_query_time_ms,
    vc.status,
    CASE 
        WHEN vc.usage_count > 20 AND vc.avg_query_time_ms > 5000 
        THEN 'RECOMMEND_MATERIALIZE'
        WHEN vc.usage_count > 50 
        THEN 'RECOMMEND_MATERIALIZE'
        WHEN vc.usage_count = 0 AND DATEDIFF(DAY, vc.last_used, GETDATE()) > 30
        THEN 'RECOMMEND_ARCHIVE'
        ELSE 'OK'
    END AS recommendation
FROM dbo.view_catalog vc
ORDER BY vc.usage_count DESC;
```

---

## End-to-End Scenario: View Accumulation Over Time

### Month 1: First Fraud Research

```
Session: "What's happening with fraud?"
Created Views:
  L1: v_fraud_monthly_trend (Explorer)
  L1: v_merchant_risk_profile (Explorer)
  L2: v_fraud_anomaly_jan2026 (Researcher)
  L2: v_electronics_velocity_clusters (Researcher)
Total views: 4
```

### Month 2: Compliance Review

```
Session: "Show me compliance alert trends for Q4"
Reused Views:
  L1: v_fraud_monthly_trend (already exists! usage: +1)
Created Views:
  L1: v_compliance_alert_weekly (Explorer)
  L1: v_kyc_completion_rates (Explorer)
  L2: v_q4_compliance_risk_score (Researcher)
Total views: 7
```

### Month 3: Cross-Domain Analysis

```
Session: "Is there a correlation between fraud spikes and KYC completion rates?"
Reused Views:
  L1: v_fraud_monthly_trend (usage: 12, auto-PROMOTED)
  L1: v_kyc_completion_rates (usage: 5, auto-PROMOTED)
  L2: v_fraud_anomaly_jan2026 (usage: 8)
Created Views:
  L3: v_fraud_kyc_correlation (Compound! Built on L1+L2 views)
Total views: 8

Note: This cross-domain insight would have been impossible in Month 1.
      The system needed both fraud AND compliance views to exist first.
```

### Month 6: Institutional Knowledge Accumulated

```
View Inventory:
  Layer 1 (Discovery): 18 views across 4 domains
  Layer 2 (Research):  11 views from specific investigations
  Layer 3 (Compound):   4 views connecting domains
  
  Materialized: 6 (high-use views with indexed views)
  Archived: 3 (unused views cleaned up)
  
Impact:
  - Average research time: 3 minutes (was 15+ minutes in Month 1)
  - Steiner Tree uses views in 78% of research queries
  - New analysts onboard faster — existing views encode tribal knowledge
  - Cross-domain research possible that was never attempted before
```

---

## Zettelkasten Parallel

The View Hierarchy maps directly to knowledge management principles:

| Zettelkasten Concept | View Hierarchy Equivalent | Example |
|---|---|---|
| Source material | Layer 0: Raw tables | `transactions`, `customers` |
| Fleeting notes | Throwaway profiling queries | Explorer's initial SELECT COUNT(*) |
| Literature notes | Layer 1: Discovery Views | `v_fraud_monthly_trend` |
| Permanent notes | Layer 2: Research Views | `v_fraud_anomaly_jan2026` |
| Hub notes / MOCs | Layer 3: Compound Views | `v_fraud_pattern_evolution` |
| Links between notes | FK edges + view dependency edges | View Catalog lineage |
| Atomic principle | Each view answers one question | Single aggregation or calculation |
| Emergence | Cross-domain compound views | Views connecting fraud + compliance |
| Link > folder | View graph > domain silos | Steiner Tree traverses across domains |

The key insight: **just as a Zettelkasten compounds knowledge over time through linked atomic notes, the View Hierarchy compounds analytical capability over time through linked atomic views.** Each research session leaves the system smarter.

---

## Tech Stack Summary

| Component | Technology | Role |
|---|---|---|
| Orchestration | LangGraph + FastAPI | Agent state machine, auth propagation |
| Explorer Agent | Claude Sonnet | Data reconnaissance + **view creation** |
| Schema Intelligence | Neo4j + pgvector | FK graph + View Catalog + Steiner Tree |
| Researcher Agent | Claude Opus | Analysis + **view promotion** |
| Data Agent | LlamaIndex + Claude Sonnet | T-SQL gen + **view DDL execution** |
| View Catalog | SQL Server + pgvector | View registry with semantic search |
| Schema Policy | FastAPI + YAML config | Column filtering + **view creation rules** |
| SQL Server Security | RLS + DDM | Engine-level security (applies to all views) |
| Database | Microsoft SQL Server | 1000+ tables + persistent view hierarchy |
| Vector Store | Qdrant | Document embeddings |
| Observability | Langfuse + Grafana | **View lineage tracking** + compliance |

---

## Risks & Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| View proliferation (too many views) | High | Max 500 views cap, 90-day archival, max 5 views per session |
| View nesting performance collapse | High | Max depth of 4 layers, materialization for high-use L1 views, execution plan monitoring |
| Stale views after schema changes | Medium | Automated staleness detection job, cascade staleness through dependency graph |
| Security policy bypass through views | Critical | SQL Server RLS propagates through views automatically; DDM + Column DENY as backstop |
| Inconsistent views across roles | Medium | Views defined once, RLS provides per-role filtering transparently |
| View naming conflicts | Low | Strict naming convention + uniqueness constraint in catalog |
| LLM hallucinates non-existent views | Medium | Steiner Tree only considers views registered in View Catalog; validation before execution |
| View dependency cycles | Low | DAG enforcement in View Catalog — no circular dependencies allowed |

---

*End of Document*
