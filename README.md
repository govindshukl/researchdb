# ResearchDB - Knowledge Crystallization POC

> Multi-Agent Research System with View Hierarchy and Knowledge Accumulation

A proof-of-concept demonstration of autonomous database research using **LangGraph** multi-agent orchestration, **View Hierarchy** for knowledge crystallization, and **Steiner Tree optimization** for efficient query planning.

---

## 🚀 Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/govindshukl/researchdb.git
cd researchdb

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set up API key
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY

# 4. Initialize database
python scripts/init_db.py

# 5. Run demo
python scripts/run_demo.py
```

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [System Design](#system-design)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Technical Decisions](#technical-decisions)
- [Future Roadmap](#future-roadmap)

---

## Overview

ResearchDB demonstrates **knowledge crystallization** in database research through a multi-agent system that autonomously creates and reuses SQL views across research sessions.

### The Problem

Traditional database research workflows require:
- Manual SQL query writing
- Repeated joins across multiple sessions
- No reuse of previous analytical work
- Lost knowledge when sessions end

### The Solution

ResearchDB introduces:
- **Multi-Agent System**: Explorer (discovery) + Researcher (analysis) agents
- **View Hierarchy**: Layer 0 (raw tables) → Layer 1 (discovery) → Layer 2 (research)
- **Knowledge Accumulation**: Views persist and get promoted with usage
- **Semantic Search**: Find relevant views using natural language
- **Steiner Tree**: Graph optimization for minimal query paths

### Key Innovation

The system gets "smarter" over time - each research session leaves behind reusable views that accelerate future queries.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       USER QUERY (NL)                            │
└──────────────┬──────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LANGGRAPH ORCHESTRATION                       │
│                                                                  │
│   ┌──────────┐    ┌─────────────┐    ┌─────────────┐          │
│   │  START   │───▶│   EXPLORE   │───▶│  RESEARCH   │───┐      │
│   └──────────┘    └─────────────┘    └─────────────┘   │      │
│                           │                   │          │      │
│                           ▼                   ▼          ▼      │
│                    Explorer Agent     Researcher     Report     │
│                    (Layer 1 Views)    (Layer 2       Generator  │
│                                        Views)                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
         ┌─────────────────────────────────────────┐
         │         KNOWLEDGE LAYER                  │
         │                                          │
         │  ┌──────────────┐   ┌────────────────┐ │
         │  │ View Catalog │   │ Semantic Search│ │
         │  │  (Registry)  │   │  (Embeddings)  │ │
         │  └──────────────┘   └────────────────┘ │
         │                                          │
         │  ┌──────────────┐   ┌────────────────┐ │
         │  │ Schema Graph │   │ Steiner Tree   │ │
         │  │ (NetworkX)   │   │  Optimization  │ │
         │  └──────────────┘   └────────────────┘ │
         └──────────────────┬───────────────────────┘
                            │
                            ▼
         ┌──────────────────────────────────────────┐
         │          DATABASE LAYER                   │
         │                                           │
         │   [transactions]  [customers]             │
         │   [merchants]     [channels]              │
         │   [mcc_codes]     [view_catalog]          │
         │                                           │
         │   + Created Views (v_*)                   │
         └───────────────────────────────────────────┘
```

---

## Features

### ✅ Implemented (POC)

- **Multi-Agent Orchestration**: LangGraph workflow with 2 specialized agents
- **View Hierarchy**: 3-layer system (Raw → Discovery → Research)
- **Knowledge Accumulation**: Views persist across sessions, auto-promote with usage
- **Semantic Search**: Sentence-transformers for view discovery by description
- **Steiner Tree**: Graph optimization prefers views over raw table joins
- **Natural Language Queries**: Claude-powered query understanding
- **Interactive CLI**: User-friendly demo interface
- **Comprehensive Testing**: Unit + integration + end-to-end tests

### 🔜 Future Enhancements

- Data Agent for T-SQL self-correction
- Layer 3 compound views
- Materialized views with indexing
- Full RLS + DDM security
- Neo4j for FK graph (production scale)
- Langfuse observability
- Web UI

---

## Installation

### Prerequisites

- Python 3.10+
- Anthropic API key (for Claude)
- 2GB disk space

### Setup

```bash
# 1. Clone repository
git clone https://github.com/govindshukl/researchdb.git
cd researchdb

# 2. Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env

# Edit .env and add:
# ANTHROPIC_API_KEY=your_key_here
# DATABASE_PATH=data/researchdb.db

# 5. Initialize database with sample data
python scripts/init_db.py
```

Expected output:
```
✓ Database schema initialized
✓ Sample data generated
  - 250 customers
  - 130 merchants
  - 1,200 transactions (9.75% fraud rate)
Database Ready!
```

---

## Usage

### Interactive Demo

```bash
python scripts/run_demo.py
```

Example session:
```
research> What are the fraud patterns in the last 3 months?

[1/4] Starting...
[2/4] Explorer Agent (Layer 1 discovery)...
[3/4] Researcher Agent (Layer 2 analysis)...
[4/4] Generating report...

✓ Workflow completed successfully!

Views Created: 2
  ✓ v_fraud_monthly_trend
  ✓ v_channel_fraud_stats

Views Reused: 1
  ↻ v_merchant_risk_profile

RESEARCH REPORT:
================================================================================
[Report content...]
```

### Single Query Mode

```bash
python scripts/run_demo.py -q "Show merchant risk analysis by category"
```

### View Statistics

```bash
# Show database stats
python scripts/run_demo.py --stats

# List all views in catalog
python scripts/run_demo.py --views
```

### Programmatic API

```python
from src.orchestration.graph import run_research_workflow

result = run_research_workflow(
    user_query="What channels have the highest fraud rates?",
    user_role="fraud_analyst"
)

print(result['report'])
print(f"Views created: {result['views_created']}")
```

---

## System Design

### View Hierarchy Model

ResearchDB implements a 3-layer view architecture:

| Layer | Purpose | Created By | Examples | Complexity |
|-------|---------|------------|----------|------------|
| **Layer 0** | Raw tables | Schema | transactions, customers | - |
| **Layer 1** | Discovery views | Explorer | v_fraud_monthly_trend | Basic aggregations |
| **Layer 2** | Research views | Researcher | v_fraud_anomaly_detection | Window functions, z-scores |
| **Layer 3** | Compound views | Future | v_comprehensive_fraud_analysis | Multi-view joins |

### Agents

#### Explorer Agent
- **Purpose**: Data reconnaissance and Layer 1 discovery
- **Responsibilities**:
  - Profile tables with exploratory queries
  - Create simple aggregation views (GROUP BY, COUNT, AVG)
  - Identify relevant tables for query
  - Check for existing views via semantic search
- **Output**: Layer 1 views + context for Researcher

#### Researcher Agent
- **Purpose**: Deep analysis and Layer 2 research
- **Responsibilities**:
  - Execute analytical queries (anomaly detection, patterns)
  - Create complex views (window functions, z-scores)
  - Reuse Explorer's Layer 1 views
  - Generate research reports
- **Output**: Layer 2 views + analytical findings + report

### View Catalog

The View Catalog is the knowledge registry:

```sql
CREATE TABLE view_catalog (
    view_id INTEGER PRIMARY KEY,
    view_name TEXT UNIQUE NOT NULL,
    layer INTEGER NOT NULL,               -- 1, 2, or 3
    domain TEXT NOT NULL,                 -- e.g., 'fraud'
    description TEXT,
    base_tables TEXT,                     -- JSON array
    depends_on_views TEXT,                -- JSON array
    created_by_role TEXT,
    created_date TIMESTAMP,
    status TEXT DEFAULT 'DRAFT',          -- DRAFT/PROMOTED/MATERIALIZED
    usage_count INTEGER DEFAULT 0,
    last_used TIMESTAMP,
    view_definition TEXT NOT NULL,
    embedding_id TEXT,
    tags TEXT                             -- JSON array
);
```

**Auto-Promotion**: Views with `usage_count >= 3` are automatically promoted to `PROMOTED` status, indicating proven value.

### Steiner Tree Optimization

Given a query requiring tables `[A, B, C]`:

**Without views**:
```
Cost = join(A, B) + join(B, C) = 2 joins
```

**With view** (v_abc that covers A+B+C):
```
Cost = 0 (view already has the join)
```

The Steiner Tree solver finds the minimal set of tables/views to answer a query, with views as zero-weight shortcut edges.

---

## Testing

### Run All Tests

```bash
# Phase 1: Foundation
python scripts/test_foundation.py

# Phase 2: Graph Intelligence
python scripts/test_graph_intelligence.py

# Phase 3: LLM Agents (requires API key)
python scripts/test_agents.py

# Phase 4: Orchestration (requires API key)
python scripts/test_orchestration.py
```

### Test Coverage

- ✅ Database initialization and schema
- ✅ View executor with SQL validation
- ✅ View catalog CRUD operations
- ✅ Semantic search with embeddings
- ✅ Schema graph construction
- ✅ Steiner Tree with/without views
- ✅ Agent initialization and utilities
- ✅ LangGraph state management
- ✅ End-to-end workflow

---

## Project Structure

```
researchdb/
├── README.md                       # This file
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment template
├── .gitignore
│
├── data/
│   ├── sample_data.sql            # Database schema
│   └── researchdb.db              # SQLite database (generated)
│
├── src/
│   ├── __init__.py
│   │
│   ├── database/
│   │   ├── connection.py          # SQLite connection manager
│   │   ├── schema.py              # Schema initialization
│   │   └── view_executor.py       # View DDL executor
│   │
│   ├── catalog/
│   │   ├── models.py              # Pydantic models
│   │   ├── view_catalog.py        # View registry
│   │   └── semantic_search.py     # Sentence-transformers search
│   │
│   ├── graph/
│   │   ├── schema_graph.py        # FK graph (NetworkX)
│   │   ├── steiner_tree.py        # Steiner Tree solver
│   │   └── view_integration.py    # View as graph nodes
│   │
│   ├── agents/
│   │   ├── llm_client.py          # Claude API wrapper
│   │   ├── base_agent.py          # Shared agent utilities
│   │   ├── explorer_agent.py      # Layer 1 discovery
│   │   └── researcher_agent.py    # Layer 2 research
│   │
│   └── orchestration/
│       ├── state.py               # ResearchState TypedDict
│       ├── router.py              # Conditional routing
│       └── graph.py               # LangGraph workflow
│
└── scripts/
    ├── init_db.py                 # Initialize database
    ├── generate_sample_data.py    # Generate test data
    ├── run_demo.py                # Interactive CLI
    ├── test_foundation.py         # Phase 1 tests
    ├── test_graph_intelligence.py # Phase 2 tests
    ├── test_agents.py             # Phase 3 tests
    └── test_orchestration.py      # Phase 4 tests
```

---

## Technical Decisions

### Why SQLite?
- ✅ Fast setup, no server required
- ✅ Supports `CREATE VIEW` (core requirement)
- ✅ Easy to share (single file database)
- ✅ Sufficient for 1,200-row POC
- ⚠️ Migrate to SQL Server for production

### Why Sentence-Transformers?
- ✅ Runs locally (no external dependencies)
- ✅ Fast for <100 views
- ✅ Simple integration
- ⚠️ Migrate to Qdrant/pgvector for scale

### Why NetworkX?
- ✅ Lightweight Python library
- ✅ Built-in Steiner Tree approximation
- ✅ Easy to visualize
- ⚠️ Migrate to Neo4j for 1000+ tables

### Why LangGraph?
- ✅ State management with TypedDict
- ✅ Multi-agent orchestration
- ✅ Conditional routing
- ✅ Built for LLM workflows

### Why Claude Haiku?
- ✅ Fast inference (low latency)
- ✅ Cost-effective for POC
- ✅ Good at structured output (JSON)
- ⚠️ Upgrade to Sonnet for production

---

## Future Roadmap

### Phase 7: Data Agent (Week 3)
- T-SQL syntax validation
- Self-correction loop
- Query optimization hints
- Error recovery

### Phase 8: Security (Week 4)
- Row-Level Security (RLS)
- Dynamic Data Masking (DDM)
- User role enforcement
- Audit logging

### Phase 9: Performance (Week 5)
- Materialized views with indexing
- Query plan caching
- Incremental refresh
- View staleness detection

### Phase 10: Production (Week 6+)
- SQL Server migration
- Neo4j for FK graph
- Langfuse observability
- Web UI (React + FastAPI)
- Horizontal scaling

---

## Example Use Cases

### 1. Fraud Pattern Analysis
```
Query: "What are the fraud patterns by channel in the last 3 months?"

Explorer creates:
- v_fraud_monthly_trend
- v_channel_fraud_stats

Researcher creates:
- v_fraud_spike_detection
- v_channel_risk_scoring

Report: Shows spike in Mobile App fraud in Jan 2026
```

### 2. Merchant Risk Profiling
```
Query: "Which merchants have the highest fraud rates?"

Explorer reuses:
- v_merchant_risk_profile (already exists)

Researcher creates:
- v_merchant_anomaly_detection
- v_merchant_category_benchmark

Report: Identifies 5 high-risk merchants with fraud rate >20%
```

### 3. Customer Behavior Analysis
```
Query: "Are there unusual customer transaction patterns?"

Explorer creates:
- v_customer_monthly_summary

Researcher creates:
- v_customer_velocity_check
- v_customer_spending_anomalies

Report: Flags 12 customers with suspicious activity
```

---

## Contributing

This is a POC/research project. For production use:
1. Review security implications
2. Test with your schema
3. Benchmark performance
4. Customize agents for your domain

---

## License

MIT License - See LICENSE file

---

## Acknowledgments

- **LangGraph** for multi-agent orchestration
- **Anthropic Claude** for natural language understanding
- **Sentence-Transformers** for semantic search
- **NetworkX** for graph algorithms

---

## Contact

For questions about this POC:
- GitHub Issues: [github.com/govindshukl/researchdb/issues](https://github.com/govindshukl/researchdb/issues)
- Architecture Document: See `architecture_v5_view_hierarchy.md`

---

**Built with ❤️ using Claude Code**
