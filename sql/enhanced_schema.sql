-- Enhanced Database Schema for iheardAI Data Pipeline with Transform Layer
-- This schema supports the enhanced data structures from the transform layer

-- Drop existing tables in reverse dependency order
DROP TABLE IF EXISTS daily_lead_metrics CASCADE;
DROP TABLE IF EXISTS session_kpis CASCADE;
DROP TABLE IF EXISTS frontend_analytics CASCADE;
DROP TABLE IF EXISTS agent_turns CASCADE;
DROP TABLE IF EXISTS marketo_leads CASCADE;

-- Enhanced Marketo Leads table with transformation metadata
CREATE TABLE marketo_leads (
    lead_id INTEGER PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    company VARCHAR(255),
    title VARCHAR(255),
    phone VARCHAR(50),
    lead_source VARCHAR(100),
    original_source_type VARCHAR(100),
    lead_status VARCHAR(50),
    email_domain VARCHAR(100),
    lead_quality_score INTEGER DEFAULT 0 CHECK (lead_quality_score >= 0 AND lead_quality_score <= 100),
    
    -- Geographic information (JSON)
    geographic_info JSONB,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    
    -- Transformation metadata (JSON)
    transformation_metadata JSONB,
    
    -- Indexes
    UNIQUE(email)
);

-- Enhanced Agent Turns table with comprehensive analytics
CREATE TABLE agent_turns (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(100) NOT NULL,
    turn_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(100),
    channel VARCHAR(50) NOT NULL DEFAULT 'text',
    
    -- Model information
    model VARCHAR(100),
    model_family VARCHAR(50),
    
    -- Token metrics
    tokens_in INTEGER DEFAULT 0,
    tokens_out INTEGER DEFAULT 0,
    total_tokens INTEGER DEFAULT 0,
    
    -- Performance metrics
    latency_ms REAL DEFAULT 0,
    tokens_per_second REAL DEFAULT 0,
    efficiency_score INTEGER DEFAULT 0 CHECK (efficiency_score >= 0 AND efficiency_score <= 100),
    
    -- Content analysis
    response_length INTEGER DEFAULT 0,
    word_count INTEGER DEFAULT 0,
    language VARCHAR(10) DEFAULT 'en',
    sentiment VARCHAR(20),
    topics JSONB,
    
    -- Quality metrics
    quality_score INTEGER DEFAULT 0 CHECK (quality_score >= 0 AND quality_score <= 100),
    
    -- Business metrics
    business_value_score INTEGER DEFAULT 0 CHECK (business_value_score >= 0 AND business_value_score <= 100),
    estimated_cost_usd DECIMAL(10,6) DEFAULT 0,
    
    -- Tool usage metrics
    tools_used_count INTEGER DEFAULT 0,
    tools_success_rate REAL DEFAULT 0,
    
    -- Timestamps
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Transformation metadata (JSON)
    transformation_metadata JSONB,
    
    -- Constraints
    UNIQUE(session_id, turn_id)
);

-- Enhanced Frontend Analytics table
CREATE TABLE frontend_analytics (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) UNIQUE,
    session_id VARCHAR(100),
    user_id VARCHAR(100),
    
    -- Event information
    event_type VARCHAR(50) NOT NULL,
    interaction_type VARCHAR(50),
    widget_id VARCHAR(100),
    
    -- Page information
    page_url TEXT,
    page_category VARCHAR(50),
    page_title VARCHAR(500),
    
    -- Referrer information
    referrer_type VARCHAR(50),
    referrer_url TEXT,
    
    -- Device information
    device_type VARCHAR(20),
    browser VARCHAR(50),
    operating_system VARCHAR(50),
    is_mobile BOOLEAN DEFAULT FALSE,
    
    -- User analytics
    user_segment VARCHAR(50),
    engagement_score INTEGER DEFAULT 0 CHECK (engagement_score >= 0 AND engagement_score <= 100),
    conversion_stage VARCHAR(50),
    quality_score INTEGER DEFAULT 0 CHECK (quality_score >= 0 AND quality_score <= 100),
    
    -- Timestamps
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Event metadata (JSON)
    event_metadata JSONB,
    
    -- Transformation metadata (JSON)
    transformation_metadata JSONB
);

-- Enhanced Session KPIs table with comprehensive metrics
CREATE TABLE session_kpis (
    session_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100),
    channel VARCHAR(50),
    
    -- Turn metrics
    turns INTEGER DEFAULT 0,
    
    -- Token metrics
    tokens_in INTEGER DEFAULT 0,
    tokens_out INTEGER DEFAULT 0,
    total_tokens INTEGER DEFAULT 0,
    
    -- Performance metrics
    avg_latency_ms REAL DEFAULT 0,
    avg_efficiency_score REAL DEFAULT 0,
    
    -- Quality metrics
    avg_quality_score REAL DEFAULT 0,
    avg_business_value_score REAL DEFAULT 0,
    
    -- Cost metrics
    total_cost_usd DECIMAL(10,6) DEFAULT 0,
    
    -- Session timing
    started_at TIMESTAMP WITH TIME ZONE,
    ended_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Daily Lead Metrics aggregation table
CREATE TABLE daily_lead_metrics (
    date DATE PRIMARY KEY,
    leads_count INTEGER DEFAULT 0,
    avg_quality_score REAL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
-- Marketo leads indexes
CREATE INDEX idx_marketo_leads_email_domain ON marketo_leads(email_domain);
CREATE INDEX idx_marketo_leads_lead_source ON marketo_leads(lead_source);
CREATE INDEX idx_marketo_leads_quality_score ON marketo_leads(lead_quality_score DESC);
CREATE INDEX idx_marketo_leads_created_at ON marketo_leads(created_at);
CREATE INDEX idx_marketo_leads_geographic ON marketo_leads USING GIN(geographic_info);

-- Agent turns indexes
CREATE INDEX idx_agent_turns_session_id ON agent_turns(session_id);
CREATE INDEX idx_agent_turns_user_id ON agent_turns(user_id);
CREATE INDEX idx_agent_turns_channel ON agent_turns(channel);
CREATE INDEX idx_agent_turns_timestamp ON agent_turns(timestamp);
CREATE INDEX idx_agent_turns_model ON agent_turns(model);
CREATE INDEX idx_agent_turns_quality_score ON agent_turns(quality_score DESC);
CREATE INDEX idx_agent_turns_business_value ON agent_turns(business_value_score DESC);
CREATE INDEX idx_agent_turns_topics ON agent_turns USING GIN(topics);

-- Frontend analytics indexes
CREATE INDEX idx_frontend_analytics_session_id ON frontend_analytics(session_id);
CREATE INDEX idx_frontend_analytics_user_id ON frontend_analytics(user_id);
CREATE INDEX idx_frontend_analytics_event_type ON frontend_analytics(event_type);
CREATE INDEX idx_frontend_analytics_timestamp ON frontend_analytics(timestamp);
CREATE INDEX idx_frontend_analytics_page_category ON frontend_analytics(page_category);
CREATE INDEX idx_frontend_analytics_user_segment ON frontend_analytics(user_segment);
CREATE INDEX idx_frontend_analytics_engagement ON frontend_analytics(engagement_score DESC);
CREATE INDEX idx_frontend_analytics_conversion ON frontend_analytics(conversion_stage);

-- Session KPIs indexes
CREATE INDEX idx_session_kpis_user_id ON session_kpis(user_id);
CREATE INDEX idx_session_kpis_channel ON session_kpis(channel);
CREATE INDEX idx_session_kpis_started_at ON session_kpis(started_at);
CREATE INDEX idx_session_kpis_quality_score ON session_kpis(avg_quality_score DESC);

-- Create views for common analytics queries
CREATE OR REPLACE VIEW session_analytics_summary AS
SELECT 
    sk.session_id,
    sk.user_id,
    sk.channel,
    sk.turns,
    sk.total_tokens,
    sk.avg_latency_ms,
    sk.avg_quality_score,
    sk.avg_business_value_score,
    sk.total_cost_usd,
    sk.started_at,
    sk.ended_at,
    EXTRACT(EPOCH FROM (sk.ended_at - sk.started_at)) / 60 as session_duration_minutes,
    -- Frontend engagement metrics
    COUNT(fa.id) as frontend_events_count,
    AVG(fa.engagement_score) as avg_frontend_engagement,
    fa.user_segment,
    fa.device_type
FROM session_kpis sk
LEFT JOIN frontend_analytics fa ON sk.session_id = fa.session_id
GROUP BY 
    sk.session_id, sk.user_id, sk.channel, sk.turns, sk.total_tokens,
    sk.avg_latency_ms, sk.avg_quality_score, sk.avg_business_value_score,
    sk.total_cost_usd, sk.started_at, sk.ended_at, fa.user_segment, fa.device_type;

CREATE OR REPLACE VIEW daily_performance_summary AS
SELECT 
    DATE(at.timestamp) as date,
    COUNT(DISTINCT at.session_id) as unique_sessions,
    COUNT(*) as total_turns,
    SUM(at.tokens_in) as total_tokens_in,
    SUM(at.tokens_out) as total_tokens_out,
    AVG(at.latency_ms) as avg_latency_ms,
    AVG(at.quality_score) as avg_quality_score,
    AVG(at.business_value_score) as avg_business_value_score,
    SUM(at.estimated_cost_usd) as total_cost_usd,
    -- Lead metrics
    COALESCE(dlm.leads_count, 0) as new_leads,
    COALESCE(dlm.avg_quality_score, 0) as avg_lead_quality
FROM agent_turns at
LEFT JOIN daily_lead_metrics dlm ON DATE(at.timestamp) = dlm.date
GROUP BY DATE(at.timestamp), dlm.leads_count, dlm.avg_quality_score
ORDER BY date DESC;

-- Create materialized view for real-time dashboards
CREATE MATERIALIZED VIEW real_time_kpi_dashboard AS
SELECT 
    -- Current hour metrics
    DATE_TRUNC('hour', NOW()) as current_hour,
    
    -- Session metrics (last hour)
    COUNT(DISTINCT CASE WHEN at.timestamp >= NOW() - INTERVAL '1 hour' THEN at.session_id END) as sessions_last_hour,
    COUNT(CASE WHEN at.timestamp >= NOW() - INTERVAL '1 hour' THEN 1 END) as turns_last_hour,
    AVG(CASE WHEN at.timestamp >= NOW() - INTERVAL '1 hour' THEN at.quality_score END) as avg_quality_last_hour,
    
    -- Daily totals
    COUNT(DISTINCT CASE WHEN DATE(at.timestamp) = CURRENT_DATE THEN at.session_id END) as sessions_today,
    COUNT(CASE WHEN DATE(at.timestamp) = CURRENT_DATE THEN 1 END) as turns_today,
    SUM(CASE WHEN DATE(at.timestamp) = CURRENT_DATE THEN at.estimated_cost_usd END) as cost_today,
    
    -- Frontend engagement (last hour)
    COUNT(CASE WHEN fa.timestamp >= NOW() - INTERVAL '1 hour' THEN 1 END) as frontend_events_last_hour,
    AVG(CASE WHEN fa.timestamp >= NOW() - INTERVAL '1 hour' THEN fa.engagement_score END) as avg_engagement_last_hour,
    
    -- Lead metrics (today)
    COALESCE(MAX(dlm.leads_count), 0) as leads_today
    
FROM agent_turns at
CROSS JOIN frontend_analytics fa
LEFT JOIN daily_lead_metrics dlm ON dlm.date = CURRENT_DATE;

-- Create index on materialized view
CREATE UNIQUE INDEX idx_real_time_kpi_dashboard_hour ON real_time_kpi_dashboard(current_hour);

-- Function to refresh materialized view
CREATE OR REPLACE FUNCTION refresh_real_time_dashboard()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY real_time_kpi_dashboard;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON TABLE marketo_leads IS 'Enhanced Marketo lead data with transformation metadata and quality scoring';
COMMENT ON TABLE agent_turns IS 'Enhanced agent conversation turns with comprehensive analytics and business metrics';
COMMENT ON TABLE frontend_analytics IS 'Enhanced frontend user interaction events with device analysis and engagement scoring';
COMMENT ON TABLE session_kpis IS 'Aggregated session-level KPIs with comprehensive performance and quality metrics';
COMMENT ON TABLE daily_lead_metrics IS 'Daily aggregated lead generation metrics';

COMMENT ON VIEW session_analytics_summary IS 'Combined session analytics with both agent and frontend metrics';
COMMENT ON VIEW daily_performance_summary IS 'Daily performance summary combining all data sources';
COMMENT ON MATERIALIZED VIEW real_time_kpi_dashboard IS 'Real-time KPI dashboard data for monitoring applications';