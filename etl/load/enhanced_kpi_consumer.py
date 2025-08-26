#!/usr/bin/env python3
"""
Enhanced KPI Consumer using Transform Scripts

This enhanced version uses dedicated transform scripts for cleaner,
more maintainable data processing.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Any
from contextlib import asynccontextmanager

import asyncpg
import aioredis
from kafka import KafkaConsumer
import yaml

# Import our transformers
from etl.transform import get_transformer, ValidationError, TransformationError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhancedKPIConsumer:
    """
    Enhanced KPI Consumer with dedicated transformation layer
    
    Benefits:
    - Clean separation of concerns
    - Reusable transformation logic
    - Better testing and maintenance
    - Standardized data formats
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.load_config()
        self.db_manager = DatabaseManager(self.postgres_dsn)
        self.redis_manager = RedisManager(self.redis_url)
        self.kafka_consumer = self.create_kafka_consumer()
        self.running = False
        
        # Initialize transformers
        self.transformers = {
            'marketo': get_transformer('marketo'),
            'frontend': get_transformer('frontend'),
            'text_agent': get_transformer('text_agent')
        }
    
    def load_config(self) -> None:
        """Load configuration from YAML file"""
        with open(self.config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Build connection strings
        pg_config = config_data['postgresql']
        self.postgres_dsn = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )
        
        redis_config = config_data['redis']
        self.redis_url = (
            f"rediss://:{os.getenv('REDIS_PASSWORD')}@{os.getenv('REDIS_HOST')}:"
            f"{os.getenv('REDIS_PORT')}/{redis_config['db']}"
        )
        
        # Kafka topics
        self.kafka_topics = [
            config_data['kafka']['topics']['text_agent_turn_completed']['name'],
            config_data['kafka']['topics']['frontend_user_interaction']['name'],
            config_data['kafka']['topics']['marketo_leads_delta']['name']
        ]
        
        self.kafka_config = {
            'brokers': config_data['kafka']['brokers'],
            'sasl_username': os.getenv('KAFKA_USER'),
            'sasl_password': os.getenv('KAFKA_PASSWORD')
        }
    
    def create_kafka_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer instance"""
        return KafkaConsumer(
            *self.kafka_topics,
            bootstrap_servers=self.kafka_config['brokers'],
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=self.kafka_config['sasl_username'],
            sasl_plain_password=self.kafka_config['sasl_password'],
            group_id='enhanced-kpi-consumer',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            max_poll_records=100
        )
    
    def get_transformer_for_event(self, event_type: str) -> str:
        """Determine which transformer to use for event type"""
        if 'marketo' in event_type:
            return 'marketo'
        elif 'frontend' in event_type:
            return 'frontend'
        elif 'text_agent' in event_type:
            return 'text_agent'
        else:
            raise ValueError(f"Unknown event type: {event_type}")
    
    async def process_event_with_transform(self, raw_event: Dict[str, Any]) -> Dict[str, Any]:
        """Process event using appropriate transformer"""
        try:
            event_type = raw_event.get('event_type', '')
            transformer_type = self.get_transformer_for_event(event_type)
            transformer = self.transformers[transformer_type]
            
            # Extract raw data (depends on event structure)
            raw_data = raw_event.get('data', raw_event)
            
            # Transform the data
            transformed_event = transformer.transform(raw_data)
            
            logger.debug(f"Transformed {event_type} event successfully")
            return transformed_event
            
        except (ValidationError, TransformationError) as e:
            logger.error(f"Transform error for event {raw_event.get('event_id', 'unknown')}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error transforming event: {e}")
            raise
    
    async def process_marketo_event(self, transformed_event: Dict[str, Any]) -> None:
        """Process transformed Marketo event"""
        try:
            event_data = transformed_event['data']
            
            # Store in marketo_leads table with enhanced data
            await self.db_manager.upsert_marketo_lead_enhanced({
                'lead_id': event_data['lead_id'],
                'email': event_data['email'],
                'first_name': event_data['first_name'],
                'last_name': event_data['last_name'],
                'full_name': event_data['full_name'],
                'company': event_data['company'],
                'lead_source': event_data['lead_source'],
                'email_domain': event_data['email_domain'],
                'lead_quality_score': event_data['lead_quality_score'],
                'created_at': datetime.fromtimestamp(event_data['created_at'] / 1000) if event_data['created_at'] else None,
                'updated_at': datetime.fromtimestamp(event_data['updated_at'] / 1000) if event_data['updated_at'] else None,
                'geographic_info': json.dumps(event_data['geographic_info']),
                'transformation_metadata': json.dumps(transformed_event['metadata'])
            })
            
            # Update lead analytics
            await self.update_lead_analytics(event_data)
            
            logger.debug(f"Processed Marketo lead {event_data['lead_id']}")
            
        except Exception as e:
            logger.error(f"Error processing Marketo event: {e}")
            raise
    
    async def process_frontend_event(self, transformed_event: Dict[str, Any]) -> None:
        """Process transformed frontend event"""
        try:
            event_data = transformed_event['data']
            
            # Store in frontend_analytics table with enhanced data
            await self.db_manager.store_frontend_analytics_enhanced({
                'event_id': transformed_event['event_id'],
                'session_id': event_data['session_id'],
                'user_id': event_data['user_id'],
                'event_type': event_data['event_type'],
                'interaction_type': event_data['interaction_type'],
                'widget_id': event_data['widget_id'],
                'page_url': event_data['page_info']['url'],
                'page_category': event_data['page_info']['category'],
                'referrer_type': event_data['referrer_info']['referrer_type'],
                'device_type': event_data['device_info']['device_type'],
                'browser': event_data['device_info']['browser'],
                'is_mobile': event_data['device_info']['is_mobile'],
                'user_segment': event_data['user_segment'],
                'engagement_score': event_data['engagement_score'],
                'conversion_stage': event_data['conversion_stage'],
                'quality_score': event_data['quality_score'],
                'timestamp': datetime.fromtimestamp(event_data['timestamp'] / 1000),
                'event_metadata': json.dumps(event_data['event_data']),
                'transformation_metadata': json.dumps(transformed_event['metadata'])
            })
            
            # Update session state in Redis with enhanced data
            if event_data['session_id']:
                await self.redis_manager.update_session_state_enhanced(
                    event_data['session_id'],
                    event_data,
                    seq=int(event_data['timestamp'])
                )
            
            logger.debug(f"Processed frontend event {event_data['event_type']}")
            
        except Exception as e:
            logger.error(f"Error processing frontend event: {e}")
            raise
    
    async def process_text_agent_event(self, transformed_event: Dict[str, Any]) -> None:
        """Process transformed text agent event"""
        try:
            event_data = transformed_event['data']
            
            # Store in agent_turns table with enhanced data
            await self.db_manager.upsert_agent_turn_enhanced({
                'session_id': event_data['session_id'],
                'turn_id': event_data['turn_id'],
                'user_id': event_data['user_id'],
                'channel': event_data['channel'],
                'model': event_data['model_info']['model_name'],
                'model_family': event_data['model_info']['model_family'],
                'tokens_in': event_data['performance_metrics']['tokens_in'],
                'tokens_out': event_data['performance_metrics']['tokens_out'],
                'total_tokens': event_data['performance_metrics']['total_tokens'],
                'latency_ms': event_data['performance_metrics']['latency_ms'],
                'tokens_per_second': event_data['performance_metrics']['tokens_per_second'],
                'efficiency_score': event_data['performance_metrics']['efficiency_score'],
                'response_length': event_data['content_analysis']['response_length'],
                'word_count': event_data['content_analysis']['word_count'],
                'language': event_data['content_analysis']['language'],
                'sentiment': event_data['content_analysis']['sentiment'],
                'topics': json.dumps(event_data['content_analysis']['topics']),
                'quality_score': event_data['quality_metrics']['overall_quality_score'],
                'business_value_score': event_data['business_metrics']['business_value_score'],
                'estimated_cost_usd': event_data['business_metrics']['estimated_cost_usd'],
                'tools_used_count': event_data['tool_usage']['tools_count'],
                'tools_success_rate': event_data['tool_usage']['tool_success_rate'],
                'timestamp': datetime.fromtimestamp(event_data['timestamp'] / 1000),
                'transformation_metadata': json.dumps(transformed_event['metadata'])
            })
            
            # Update session KPIs with enhanced data
            await self.update_session_kpis_enhanced(event_data['session_id'])
            
            logger.debug(f"Processed text agent turn {event_data['turn_id']}")
            
        except Exception as e:
            logger.error(f"Error processing text agent event: {e}")
            raise
    
    async def process_batch(self, messages: List[Any]) -> None:
        """Process a batch of Kafka messages using transformers"""
        if not messages:
            return
        
        processed_count = 0
        transform_errors = 0
        processing_errors = 0
        
        for message in messages:
            try:
                raw_event = message.value
                if not raw_event:
                    continue
                
                # Transform the event
                try:
                    transformed_event = await self.process_event_with_transform(raw_event)
                except (ValidationError, TransformationError):
                    transform_errors += 1
                    continue
                
                # Process based on transformed event type
                event_type = transformed_event.get('event_type', '')
                
                try:
                    if 'marketo' in event_type:
                        await self.process_marketo_event(transformed_event)
                    elif 'frontend' in event_type:
                        await self.process_frontend_event(transformed_event)
                    elif 'text_agent' in event_type:
                        await self.process_text_agent_event(transformed_event)
                    else:
                        logger.warning(f"Unknown transformed event type: {event_type}")
                        continue
                    
                    processed_count += 1
                    
                except Exception as e:
                    processing_errors += 1
                    logger.error(f"Processing error: {e}")
                
            except Exception as e:
                logger.error(f"Unexpected error processing message: {e}")
                processing_errors += 1
        
        logger.info(
            f"Batch processed: {processed_count} successful, "
            f"{transform_errors} transform errors, {processing_errors} processing errors"
        )
        
        # Fail if too many errors
        total_messages = len(messages)
        error_rate = (transform_errors + processing_errors) / total_messages
        if error_rate > 0.5:
            raise Exception(f"High error rate in batch: {error_rate:.1%}")
    
    async def run_consumer(self) -> None:
        """Run the enhanced consumer loop"""
        self.running = True
        logger.info("Starting Enhanced KPI Consumer with Transform Layer")
        
        try:
            # Initialize connections
            await self.db_manager.initialize()
            await self.redis_manager.initialize()
            
            # Consumer loop
            while self.running:
                try:
                    # Poll for messages
                    messages = self.kafka_consumer.poll(
                        timeout_ms=1000,
                        max_records=100
                    )
                    
                    if messages:
                        # Flatten messages from all topic partitions
                        all_messages = []
                        for topic_partition, partition_messages in messages.items():
                            all_messages.extend(partition_messages)
                        
                        # Process batch using transformers
                        await self.process_batch(all_messages)
                        
                        # Commit offsets
                        self.kafka_consumer.commit()
                        
                    else:
                        # Brief sleep when no messages
                        await asyncio.sleep(0.1)
                        
                except Exception as e:
                    logger.error(f"Consumer loop error: {e}")
                    await asyncio.sleep(1)
                    
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            await self.cleanup()
    
    async def cleanup(self) -> None:
        """Cleanup resources"""
        self.running = False
        
        if hasattr(self, 'kafka_consumer'):
            self.kafka_consumer.close()
            
        await self.db_manager.close()
        await self.redis_manager.close()
        
        logger.info("Enhanced KPI consumer cleanup completed")


class DatabaseManager:
    """Enhanced PostgreSQL database operations manager"""
    
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
        
    async def initialize(self) -> None:
        """Initialize database connection pool"""
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=5,
            max_size=20,
            command_timeout=60
        )
        logger.info("Database connection pool initialized")
        
    async def close(self) -> None:
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Get database connection from pool"""
        async with self.pool.acquire() as connection:
            yield connection
    
    async def upsert_marketo_lead_enhanced(self, lead_data: Dict[str, Any]) -> None:
        """Upsert enhanced Marketo lead with transformation metadata"""
        query = """
        INSERT INTO marketo_leads (
            lead_id, email, first_name, last_name, full_name, company,
            lead_source, email_domain, lead_quality_score, created_at,
            updated_at, geographic_info, transformation_metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (lead_id)
        DO UPDATE SET
            email = EXCLUDED.email,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            full_name = EXCLUDED.full_name,
            company = EXCLUDED.company,
            lead_source = EXCLUDED.lead_source,
            email_domain = EXCLUDED.email_domain,
            lead_quality_score = EXCLUDED.lead_quality_score,
            updated_at = EXCLUDED.updated_at,
            geographic_info = EXCLUDED.geographic_info,
            transformation_metadata = EXCLUDED.transformation_metadata
        """
        
        async with self.get_connection() as conn:
            await conn.execute(
                query,
                lead_data['lead_id'],
                lead_data['email'],
                lead_data['first_name'],
                lead_data['last_name'],
                lead_data['full_name'],
                lead_data['company'],
                lead_data['lead_source'],
                lead_data['email_domain'],
                lead_data['lead_quality_score'],
                lead_data['created_at'],
                lead_data['updated_at'],
                lead_data['geographic_info'],
                lead_data['transformation_metadata']
            )
    
    async def store_frontend_analytics_enhanced(self, event_data: Dict[str, Any]) -> None:
        """Store enhanced frontend analytics with transformation metadata"""
        query = """
        INSERT INTO frontend_analytics (
            event_id, session_id, user_id, event_type, interaction_type,
            widget_id, page_url, page_category, referrer_type, device_type,
            browser, is_mobile, user_segment, engagement_score,
            conversion_stage, quality_score, timestamp, event_metadata,
            transformation_metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        """
        
        async with self.get_connection() as conn:
            await conn.execute(
                query,
                event_data['event_id'],
                event_data['session_id'],
                event_data['user_id'],
                event_data['event_type'],
                event_data['interaction_type'],
                event_data['widget_id'],
                event_data['page_url'],
                event_data['page_category'],
                event_data['referrer_type'],
                event_data['device_type'],
                event_data['browser'],
                event_data['is_mobile'],
                event_data['user_segment'],
                event_data['engagement_score'],
                event_data['conversion_stage'],
                event_data['quality_score'],
                event_data['timestamp'],
                event_data['event_metadata'],
                event_data['transformation_metadata']
            )
    
    async def upsert_agent_turn_enhanced(self, turn_data: Dict[str, Any]) -> None:
        """Upsert enhanced agent turn with comprehensive metrics"""
        query = """
        INSERT INTO agent_turns (
            session_id, turn_id, user_id, channel, model, model_family,
            tokens_in, tokens_out, total_tokens, latency_ms, tokens_per_second,
            efficiency_score, response_length, word_count, language,
            sentiment, topics, quality_score, business_value_score,
            estimated_cost_usd, tools_used_count, tools_success_rate,
            timestamp, transformation_metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
        ON CONFLICT (session_id, turn_id)
        DO UPDATE SET
            user_id = EXCLUDED.user_id,
            channel = EXCLUDED.channel,
            model = EXCLUDED.model,
            model_family = EXCLUDED.model_family,
            tokens_in = EXCLUDED.tokens_in,
            tokens_out = EXCLUDED.tokens_out,
            total_tokens = EXCLUDED.total_tokens,
            latency_ms = EXCLUDED.latency_ms,
            tokens_per_second = EXCLUDED.tokens_per_second,
            efficiency_score = EXCLUDED.efficiency_score,
            response_length = EXCLUDED.response_length,
            word_count = EXCLUDED.word_count,
            language = EXCLUDED.language,
            sentiment = EXCLUDED.sentiment,
            topics = EXCLUDED.topics,
            quality_score = EXCLUDED.quality_score,
            business_value_score = EXCLUDED.business_value_score,
            estimated_cost_usd = EXCLUDED.estimated_cost_usd,
            tools_used_count = EXCLUDED.tools_used_count,
            tools_success_rate = EXCLUDED.tools_success_rate,
            timestamp = EXCLUDED.timestamp,
            transformation_metadata = EXCLUDED.transformation_metadata
        """
        
        async with self.get_connection() as conn:
            await conn.execute(
                query,
                turn_data['session_id'],
                turn_data['turn_id'],
                turn_data['user_id'],
                turn_data['channel'],
                turn_data['model'],
                turn_data['model_family'],
                turn_data['tokens_in'],
                turn_data['tokens_out'],
                turn_data['total_tokens'],
                turn_data['latency_ms'],
                turn_data['tokens_per_second'],
                turn_data['efficiency_score'],
                turn_data['response_length'],
                turn_data['word_count'],
                turn_data['language'],
                turn_data['sentiment'],
                turn_data['topics'],
                turn_data['quality_score'],
                turn_data['business_value_score'],
                turn_data['estimated_cost_usd'],
                turn_data['tools_used_count'],
                turn_data['tools_success_rate'],
                turn_data['timestamp'],
                turn_data['transformation_metadata']
            )
    
    async def update_lead_analytics(self, lead_data: Dict[str, Any]) -> None:
        """Update lead analytics aggregations"""
        # Update daily lead metrics
        query = """
        INSERT INTO daily_lead_metrics (date, leads_count, avg_quality_score)
        SELECT 
            CURRENT_DATE,
            COUNT(*),
            AVG(lead_quality_score)
        FROM marketo_leads 
        WHERE DATE(created_at) = CURRENT_DATE
        ON CONFLICT (date) 
        DO UPDATE SET
            leads_count = EXCLUDED.leads_count,
            avg_quality_score = EXCLUDED.avg_quality_score
        """
        
        async with self.get_connection() as conn:
            await conn.execute(query)
    
    async def update_session_kpis_enhanced(self, session_id: str) -> None:
        """Update enhanced session KPIs with comprehensive metrics"""
        query = """
        WITH session_stats AS (
            SELECT 
                session_id,
                user_id,
                channel,
                COUNT(*) as turns,
                SUM(tokens_in) as tokens_in,
                SUM(tokens_out) as tokens_out,
                SUM(total_tokens) as total_tokens,
                AVG(latency_ms) as avg_latency_ms,
                AVG(efficiency_score) as avg_efficiency_score,
                AVG(quality_score) as avg_quality_score,
                AVG(business_value_score) as avg_business_value_score,
                SUM(estimated_cost_usd) as total_cost_usd,
                MIN(timestamp) as started_at,
                MAX(timestamp) as ended_at
            FROM agent_turns
            WHERE session_id = $1
            GROUP BY session_id, user_id, channel
        )
        INSERT INTO session_kpis (
            session_id, user_id, channel, turns, tokens_in, tokens_out,
            total_tokens, avg_latency_ms, avg_efficiency_score,
            avg_quality_score, avg_business_value_score, total_cost_usd,
            started_at, ended_at, updated_at
        )
        SELECT 
            session_id, user_id, channel, turns, tokens_in, tokens_out,
            total_tokens, avg_latency_ms, avg_efficiency_score,
            avg_quality_score, avg_business_value_score, total_cost_usd,
            started_at, ended_at, NOW()
        FROM session_stats
        ON CONFLICT (session_id)
        DO UPDATE SET
            user_id = EXCLUDED.user_id,
            channel = EXCLUDED.channel,
            turns = EXCLUDED.turns,
            tokens_in = EXCLUDED.tokens_in,
            tokens_out = EXCLUDED.tokens_out,
            total_tokens = EXCLUDED.total_tokens,
            avg_latency_ms = EXCLUDED.avg_latency_ms,
            avg_efficiency_score = EXCLUDED.avg_efficiency_score,
            avg_quality_score = EXCLUDED.avg_quality_score,
            avg_business_value_score = EXCLUDED.avg_business_value_score,
            total_cost_usd = EXCLUDED.total_cost_usd,
            started_at = EXCLUDED.started_at,
            ended_at = EXCLUDED.ended_at,
            updated_at = NOW()
        """
        
        async with self.get_connection() as conn:
            await conn.execute(query, session_id)


class RedisManager:
    """Enhanced Redis operations manager for hot session state"""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis = None
        
    async def initialize(self) -> None:
        """Initialize Redis connection"""
        self.redis = aioredis.from_url(
            self.redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        logger.info("Redis connection initialized")
        
    async def close(self) -> None:
        """Close Redis connection"""
        if self.redis:
            await self.redis.close()
            logger.info("Redis connection closed")
    
    async def update_session_state_enhanced(self, session_id: str, event_data: Dict[str, Any], seq: int) -> None:
        """Update session hot state with enhanced analytics data"""
        state_key = f"session:{session_id}:state"
        
        # Get current sequence number
        current_seq = await self.redis.hget(state_key, "seq")
        if current_seq and int(current_seq) >= seq:
            logger.debug(f"Skipping update for {session_id}, sequence {seq} <= {current_seq}")
            return
        
        # Enhanced session state data
        state_data = {
            "session_id": session_id,
            "user_id": event_data.get('user_id', ''),
            "channel": event_data.get('channel', 'unknown'),
            "user_segment": event_data.get('user_segment', 'unknown'),
            "engagement_score": event_data.get('engagement_score', 0),
            "conversion_stage": event_data.get('conversion_stage', 'awareness'),
            "device_type": event_data.get('device_info', {}).get('device_type', 'unknown'),
            "last_event_type": event_data.get('event_type', ''),
            "seq": seq,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "status": "active"
        }
        
        # Additional data based on event type
        if 'page_info' in event_data:
            state_data["last_page_url"] = event_data['page_info'].get('url', '')
            state_data["last_page_category"] = event_data['page_info'].get('category', '')
        
        pipe = self.redis.pipeline()
        pipe.hset(state_key, mapping=state_data)
        pipe.expire(state_key, 3600)  # 1 hour TTL
        await pipe.execute()
        
        logger.debug(f"Updated enhanced session state for {session_id} with seq {seq}")

async def main():
    """Main execution function"""
    import signal
    
    consumer = EnhancedKPIConsumer()
    
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        consumer.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    await consumer.run_consumer()


if __name__ == "__main__":
    asyncio.run(main())