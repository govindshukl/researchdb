-- ResearchDB Sample Database Schema
-- SQLite schema for Research Agent POC

-- ====================
-- Core Business Tables
-- ====================

-- Customers Table
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    region TEXT NOT NULL,
    segment TEXT NOT NULL,
    created_date DATE DEFAULT (date('now')),
    CONSTRAINT chk_region CHECK (region IN ('GCC', 'MENA', 'Europe', 'Asia')),
    CONSTRAINT chk_segment CHECK (segment IN ('Retail', 'Corporate', 'SME', 'Private Banking'))
);

-- MCC Codes (Merchant Category Codes) Reference
CREATE TABLE IF NOT EXISTS mcc_codes (
    mcc_code TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    description TEXT,
    risk_level TEXT DEFAULT 'LOW',
    CONSTRAINT chk_risk CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH'))
);

-- Merchants Table
CREATE TABLE IF NOT EXISTS merchants (
    merchant_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    mcc_code TEXT NOT NULL,
    risk_tier TEXT DEFAULT 'LOW',
    country TEXT,
    created_date DATE DEFAULT (date('now')),
    FOREIGN KEY (mcc_code) REFERENCES mcc_codes(mcc_code),
    CONSTRAINT chk_risk_tier CHECK (risk_tier IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'))
);

-- Channels Table
CREATE TABLE IF NOT EXISTS channels (
    channel_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    channel_type TEXT NOT NULL,
    description TEXT,
    CONSTRAINT chk_channel_type CHECK (channel_type IN ('ATM', 'POS', 'Online', 'Mobile', 'Branch'))
);

-- Transactions Table (Main fact table)
CREATE TABLE IF NOT EXISTS transactions (
    txn_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    merchant_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    txn_date DATE NOT NULL,
    txn_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fraud_flag INTEGER DEFAULT 0,
    fraud_score DECIMAL(5,4),
    currency TEXT DEFAULT 'USD',
    status TEXT DEFAULT 'COMPLETED',
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id),
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id),
    CONSTRAINT chk_fraud_flag CHECK (fraud_flag IN (0, 1)),
    CONSTRAINT chk_amount CHECK (amount > 0),
    CONSTRAINT chk_status CHECK (status IN ('COMPLETED', 'PENDING', 'FAILED', 'REVERSED'))
);

-- ====================
-- View Catalog (Metadata Registry)
-- ====================

CREATE TABLE IF NOT EXISTS view_catalog (
    view_id INTEGER PRIMARY KEY AUTOINCREMENT,
    view_name TEXT UNIQUE NOT NULL,
    layer INTEGER NOT NULL,
    domain TEXT NOT NULL,
    description TEXT,

    -- Lineage (stored as JSON strings)
    base_tables TEXT,              -- JSON array: ["transactions", "merchants"]
    depends_on_views TEXT,         -- JSON array: ["v_fraud_monthly_trend"]
    used_by_views TEXT,            -- JSON array: ["v_fraud_pattern_evolution"]
    steiner_subgraph TEXT,         -- JSON object: original Steiner Tree

    -- Creation context
    created_by_session TEXT,
    created_by_role TEXT DEFAULT 'fraud_analyst',
    created_by_query TEXT,         -- Original user question
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Lifecycle
    status TEXT DEFAULT 'DRAFT',
    promoted_date TIMESTAMP,
    materialized_date TIMESTAMP,

    -- Usage tracking
    usage_count INTEGER DEFAULT 0,
    last_used TIMESTAMP,
    avg_query_time_ms REAL,

    -- Freshness
    freshness_type TEXT DEFAULT 'LIVE',
    last_validated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_valid INTEGER DEFAULT 1,

    -- Semantic search
    embedding_id TEXT,             -- Reference to embedding (stored in-memory for POC)
    tags TEXT,                     -- JSON array: ["fraud", "monthly", "trend"]

    -- DDL
    view_definition TEXT NOT NULL, -- Full CREATE VIEW statement

    -- Governance
    approved_by TEXT,
    approval_date TIMESTAMP,
    review_notes TEXT,

    CONSTRAINT chk_layer CHECK (layer IN (1, 2, 3)),
    CONSTRAINT chk_domain CHECK (domain IN ('fraud', 'compliance', 'customer', 'merchant', 'transaction', 'risk')),
    CONSTRAINT chk_status CHECK (status IN ('DRAFT', 'PROMOTED', 'MATERIALIZED', 'STALE', 'ARCHIVED')),
    CONSTRAINT chk_freshness CHECK (freshness_type IN ('LIVE', 'STATIC', 'SCHEDULED')),
    CONSTRAINT chk_is_valid CHECK (is_valid IN (0, 1))
);

-- ====================
-- Indexes for Performance
-- ====================

-- Transactions indexes (most queried table)
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(txn_date);
CREATE INDEX IF NOT EXISTS idx_transactions_customer ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_merchant ON transactions(merchant_id);
CREATE INDEX IF NOT EXISTS idx_transactions_channel ON transactions(channel_id);
CREATE INDEX IF NOT EXISTS idx_transactions_fraud ON transactions(fraud_flag);
CREATE INDEX IF NOT EXISTS idx_transactions_date_fraud ON transactions(txn_date, fraud_flag);

-- View Catalog indexes
CREATE INDEX IF NOT EXISTS idx_view_catalog_domain_layer ON view_catalog(domain, layer);
CREATE INDEX IF NOT EXISTS idx_view_catalog_status ON view_catalog(status);
CREATE INDEX IF NOT EXISTS idx_view_catalog_usage ON view_catalog(usage_count DESC);
CREATE INDEX IF NOT EXISTS idx_view_catalog_created ON view_catalog(created_date);

-- Merchants index
CREATE INDEX IF NOT EXISTS idx_merchants_mcc ON merchants(mcc_code);
CREATE INDEX IF NOT EXISTS idx_merchants_risk ON merchants(risk_tier);

-- ====================
-- Seed Data - Reference Tables
-- ====================

-- Insert MCC Codes
INSERT OR IGNORE INTO mcc_codes (mcc_code, category, description, risk_level) VALUES
('5411', 'Grocery', 'Grocery Stores, Supermarkets', 'LOW'),
('5812', 'Restaurants', 'Eating Places, Restaurants', 'LOW'),
('5732', 'Electronics', 'Electronics Sales', 'MEDIUM'),
('5999', 'Retail', 'Miscellaneous Retail Stores', 'LOW'),
('7995', 'Gaming', 'Gambling, Casino', 'HIGH'),
('6211', 'Securities', 'Securities Brokers/Dealers', 'MEDIUM'),
('5541', 'Gas', 'Service Stations (with or without ancillary services)', 'LOW'),
('4814', 'Telecom', 'Telecommunication Services', 'LOW'),
('5651', 'Apparel', 'Family Clothing Stores', 'LOW'),
('7372', 'Software', 'Computer Programming', 'MEDIUM'),
('5912', 'Pharmacy', 'Drug Stores, Pharmacies', 'LOW'),
('7922', 'Entertainment', 'Theatrical Producers (except motion pictures)', 'MEDIUM'),
('5699', 'Fashion', 'Miscellaneous Apparel and Accessory Shops', 'LOW'),
('5661', 'Shoes', 'Shoe Stores', 'LOW'),
('7996', 'Amusement', 'Amusement Parks, Circuses, Carnivals', 'LOW');

-- Insert Channels
INSERT OR IGNORE INTO channels (name, channel_type, description) VALUES
('ATM Withdrawals', 'ATM', 'Cash withdrawals from ATMs'),
('POS Terminal', 'POS', 'Point-of-sale card transactions'),
('Online Banking', 'Online', 'Web-based banking portal'),
('Mobile App', 'Mobile', 'Mobile banking application'),
('Branch Counter', 'Branch', 'In-branch teller transactions'),
('Phone Banking', 'Online', 'Telephone banking service');

-- ====================
-- Statistics Table (Optional - for caching profiling results)
-- ====================

CREATE TABLE IF NOT EXISTS table_statistics (
    table_name TEXT PRIMARY KEY,
    row_count INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    column_info TEXT  -- JSON: column names, types, null counts
);

-- ====================
-- Session Tracking (Optional - for multi-session analysis)
-- ====================

CREATE TABLE IF NOT EXISTS research_sessions (
    session_id TEXT PRIMARY KEY,
    user_role TEXT NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    user_query TEXT,
    views_created INTEGER DEFAULT 0,
    views_reused INTEGER DEFAULT 0,
    status TEXT DEFAULT 'ACTIVE',
    CONSTRAINT chk_session_status CHECK (status IN ('ACTIVE', 'COMPLETED', 'FAILED'))
);
