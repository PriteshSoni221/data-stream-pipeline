-- Create the processed logs table for storing streaming anomalies and metrics
CREATE TABLE processed_logs (
    -- 1. Identity & Framework Metadata
    id BIGSERIAL PRIMARY KEY, -- Auto-incrementing unique identifier for every log record
    pipeline_type VARCHAR(20) NOT NULL, -- Identifies the source engine: 'NAIVE' or 'OPTIMIZED'
    timestamp TIMESTAMPTZ NOT NULL, -- Timezone-aware timestamp of when the log event occurred

    -- 2. Thesis Performance Metrics (Core Quantitative Data for Plots)
    processing_latency_ms INT NOT NULL, -- Total duration (in ms) from Kafka ingestion to end of model inference
    token_cost_usd NUMERIC(8, 6) DEFAULT 0.000000, -- Financial cost of this specific log's inference pipeline (e.g., 0.000150)

    -- 3. Semi-Structured Flex Data (Consolidating payloads into a single, high-performance JSON block)
    -- This single column will contain:
    --   - "raw_message": The original string error log
    --   - "routing_decision": Which path it took ('CACHE_HIT', 'SML_CLASSIFIED', 'LLM_FALLBACK')
    --   - "confidence_score": The probability score outputted by the SML router
    --   - "ai_diagnosis": The plain-text explanation of the error
    inference_payload JSONB NOT NULL
);

-- ==========================================
-- INDEXES FOR HIGH-PERFORMANCE DASHBOARD QUERIES
-- ==========================================

-- Composite index for your main dashboard comparisons. 
-- Allows the backend to instantly fetch or plot the latest logs for either the Naive or Optimized pipeline.
CREATE INDEX idx_logs_pipeline_time 
ON processed_logs (pipeline_type, timestamp DESC);

-- GIN (Generalized Inverted Index) specifically for the JSONB payload.
-- This allows your backend to rapidly run queries filtering by the internal routing path 
-- (e.g., "Show me all logs where inference_payload->>'routing_decision' = 'LLM_FALLBACK'")
CREATE INDEX idx_logs_payload_routing 
ON processed_logs USING gin ((inference_payload -> 'routing_decision'));