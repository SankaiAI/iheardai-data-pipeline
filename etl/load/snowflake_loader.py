#!/usr/bin/env python3
"""
Snowflake Loader for iheardAI Data Pipeline (Optional Enhancement)

This loader would be added when you need advanced analytics capabilities
beyond what PostgreSQL provides.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """
    Optional Snowflake data loader for advanced analytics
    Add this when you need:
    - Complex analytical queries
    - Data science workloads  
    - BI tool integrations
    - Historical data analysis
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.load_config()
        self.snowflake_conn = None
        
    def load_config(self):
        """Load Snowflake configuration"""
        self.snowflake_config = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'IHEARDAI_ANALYTICS'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'RAW_EVENTS'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'ANALYTICS_WH'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'ANALYTICS_ROLE')
        }
        
    async def initialize_connection(self):
        """Initialize Snowflake connection"""
        self.snowflake_conn = snowflake.connector.connect(
            **self.snowflake_config
        )
        logger.info("Snowflake connection established")
        
    async def create_tables_if_not_exist(self):
        """Create Snowflake tables for different event types"""
        
        tables = {
            'FRONTEND_EVENTS': """
                CREATE TABLE IF NOT EXISTS FRONTEND_EVENTS (
                    EVENT_ID STRING PRIMARY KEY,
                    SESSION_ID STRING,
                    USER_ID STRING,
                    EVENT_TYPE STRING,
                    PAGE_URL STRING,
                    WIDGET_ID STRING,
                    INTERACTION_TYPE STRING,
                    TIMESTAMP TIMESTAMP_NTZ,
                    METADATA VARIANT,
                    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """,
            
            'TEXT_AGENT_EVENTS': """
                CREATE TABLE IF NOT EXISTS TEXT_AGENT_EVENTS (
                    EVENT_ID STRING PRIMARY KEY,
                    SESSION_ID STRING,
                    TURN_ID STRING,
                    USER_ID STRING,
                    MODEL STRING,
                    TOKENS_IN NUMBER,
                    TOKENS_OUT NUMBER,
                    LATENCY_MS NUMBER,
                    RESPONSE_TEXT STRING,
                    TIMESTAMP TIMESTAMP_NTZ,
                    METADATA VARIANT,
                    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """,
            
            'MARKETO_EVENTS': """
                CREATE TABLE IF NOT EXISTS MARKETO_EVENTS (
                    LEAD_ID NUMBER PRIMARY KEY,
                    EMAIL STRING,
                    FIRST_NAME STRING,
                    LAST_NAME STRING,
                    LEAD_SOURCE STRING,
                    CREATED_AT TIMESTAMP_NTZ,
                    UPDATED_AT TIMESTAMP_NTZ,
                    METADATA VARIANT,
                    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """
        }
        
        cursor = self.snowflake_conn.cursor()
        try:
            for table_name, create_sql in tables.items():
                cursor.execute(create_sql)
                logger.info(f"Table {table_name} ready")
        finally:
            cursor.close()
    
    async def load_batch_to_snowflake(self, events: List[Dict[str, Any]], table_name: str):
        """Load batch of events to Snowflake table"""
        if not events:
            return
            
        # Convert to DataFrame
        df = pd.DataFrame(events)
        
        # Write to Snowflake using pandas
        success, nchunks, nrows, _ = write_pandas(
            self.snowflake_conn,
            df,
            table_name,
            database=self.snowflake_config['database'],
            schema=self.snowflake_config['schema'],
            chunk_size=1000,
            compression='gzip'
        )
        
        if success:
            logger.info(f"Loaded {nrows} rows to {table_name}")
        else:
            logger.error(f"Failed to load batch to {table_name}")
    
    async def process_kafka_events(self):
        """Process events from Kafka topics"""
        
        # Subscribe to all relevant topics
        consumer = KafkaConsumer(
            'frontend.user.interaction',
            'text.agent.turn.completed', 
            'marketo.leads.delta',
            bootstrap_servers=['kafka1:9092', 'kafka2:9092'],
            group_id='snowflake-loader',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Batch processing
        batch_size = 1000
        batches = {
            'frontend.user.interaction': [],
            'text.agent.turn.completed': [],
            'marketo.leads.delta': []
        }
        
        for message in consumer:
            topic = message.topic
            event_data = message.value
            
            # Add to appropriate batch
            if topic in batches:
                batches[topic].append(self.transform_event_for_snowflake(event_data))
                
                # Load batch when full
                if len(batches[topic]) >= batch_size:
                    table_name = self.get_table_name(topic)
                    await self.load_batch_to_snowflake(batches[topic], table_name)
                    batches[topic].clear()
    
    def transform_event_for_snowflake(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform event data for Snowflake schema"""
        
        data = event_data.get('data', {})
        
        return {
            'EVENT_ID': event_data.get('event_id'),
            'SESSION_ID': data.get('session_id'),
            'USER_ID': data.get('user_id'),
            'EVENT_TYPE': event_data.get('event_type'),
            'TIMESTAMP': datetime.fromtimestamp(
                event_data.get('ts_ms', 0) / 1000
            ).strftime('%Y-%m-%d %H:%M:%S'),
            'METADATA': json.dumps(data),
            **data  # Flatten common fields
        }
    
    def get_table_name(self, topic: str) -> str:
        """Map Kafka topic to Snowflake table"""
        mapping = {
            'frontend.user.interaction': 'FRONTEND_EVENTS',
            'text.agent.turn.completed': 'TEXT_AGENT_EVENTS', 
            'marketo.leads.delta': 'MARKETO_EVENTS'
        }
        return mapping.get(topic, 'UNKNOWN_EVENTS')


# Example Snowflake Analytics Queries
ANALYTICS_QUERIES = {
    'user_journey_analysis': """
        WITH user_sessions AS (
            SELECT 
                USER_ID,
                SESSION_ID,
                MIN(TIMESTAMP) as session_start,
                MAX(TIMESTAMP) as session_end,
                COUNT(*) as event_count,
                ARRAY_AGG(EVENT_TYPE ORDER BY TIMESTAMP) as event_sequence
            FROM FRONTEND_EVENTS 
            WHERE TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE())
            GROUP BY USER_ID, SESSION_ID
        )
        SELECT 
            event_sequence,
            COUNT(*) as frequency,
            AVG(event_count) as avg_events_per_session,
            AVG(DATEDIFF(second, session_start, session_end)) as avg_duration_seconds
        FROM user_sessions
        GROUP BY event_sequence
        ORDER BY frequency DESC;
    """,
    
    'conversion_funnel': """
        WITH funnel_events AS (
            SELECT 
                SESSION_ID,
                CASE 
                    WHEN EVENT_TYPE = 'page_view' THEN 1
                    WHEN EVENT_TYPE = 'widget_open' THEN 2
                    WHEN EVENT_TYPE = 'user_message' THEN 3
                    WHEN EVENT_TYPE = 'product_interest' THEN 4
                    WHEN EVENT_TYPE = 'conversion' THEN 5
                END as funnel_stage,
                EVENT_TYPE,
                TIMESTAMP
            FROM FRONTEND_EVENTS
            WHERE TIMESTAMP >= DATEADD(day, -7, CURRENT_DATE())
        )
        SELECT 
            funnel_stage,
            EVENT_TYPE,
            COUNT(DISTINCT SESSION_ID) as unique_sessions,
            LAG(COUNT(DISTINCT SESSION_ID)) OVER (ORDER BY funnel_stage) as previous_stage_sessions,
            CASE 
                WHEN LAG(COUNT(DISTINCT SESSION_ID)) OVER (ORDER BY funnel_stage) > 0
                THEN COUNT(DISTINCT SESSION_ID) / LAG(COUNT(DISTINCT SESSION_ID)) OVER (ORDER BY funnel_stage)
                ELSE NULL 
            END as conversion_rate
        FROM funnel_events
        GROUP BY funnel_stage, EVENT_TYPE
        ORDER BY funnel_stage;
    """
}


async def main():
    """Main execution for Snowflake loader"""
    loader = SnowflakeLoader()
    await loader.initialize_connection()
    await loader.create_tables_if_not_exist() 
    await loader.process_kafka_events()


if __name__ == "__main__":
    asyncio.run(main())