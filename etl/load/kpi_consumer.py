#!/usr/bin/env python3
"""
KPI Consumer for iheardAI Data Pipeline

This consumer processes agent turn completion events and updates PostgreSQL
tables for KPIs and session analytics, plus maintains Redis hot session state.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager

import asyncpg
import aioredis
from kafka import KafkaConsumer
from pydantic import BaseModel, Field
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KPIConsumerConfig(BaseModel):
    """KPI Consumer configuration"""
    consumer_group: str = Field(default="kpi-consumer-group")
    batch_size: int = Field(default=100)
    commit_interval: int = Field(default=5000)  # ms
    max_retries: int = Field(default=3)
    retry_delay: int = Field(default=1000)  # ms


class AgentTurnRecord(BaseModel):
    """Agent turn database record"""
    session_id: str
    turn_id: str
    user_id: Optional[str]
    channel: str
    model: str
    tokens_in: int
    tokens_out: int
    latency_ms: float
    response_text: Optional[str]
    ts_ms: int


class SessionKPIRecord(BaseModel):
    """Session KPI database record"""
    session_id: str
    user_id: Optional[str]
    channel: str
    turns: int
    tokens_in: int
    tokens_out: int
    avg_latency_ms: float
    started_at: datetime
    ended_at: Optional[datetime]
    updated_at: datetime


class DatabaseManager:
    """PostgreSQL database operations manager"""
    
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
    
    async def upsert_agent_turn(self, turn_record: AgentTurnRecord) -> None:
        """Upsert agent turn record"""
        query = """
        INSERT INTO agent_turns (
            session_id, turn_id, user_id, channel, model,
            tokens_in, tokens_out, latency_ms, response_text, ts_ms
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (session_id, turn_id)
        DO UPDATE SET
            user_id = EXCLUDED.user_id,
            channel = EXCLUDED.channel,
            model = EXCLUDED.model,
            tokens_in = EXCLUDED.tokens_in,
            tokens_out = EXCLUDED.tokens_out,
            latency_ms = EXCLUDED.latency_ms,
            response_text = EXCLUDED.response_text,
            ts_ms = EXCLUDED.ts_ms
        """
        
        async with self.get_connection() as conn:
            await conn.execute(
                query,
                turn_record.session_id,
                turn_record.turn_id,
                turn_record.user_id,
                turn_record.channel,
                turn_record.model,
                turn_record.tokens_in,
                turn_record.tokens_out,
                turn_record.latency_ms,
                turn_record.response_text,
                turn_record.ts_ms
            )
    
    async def update_session_kpis(self, session_id: str) -> None:
        """Recompute and update session KPIs"""
        query = """
        WITH session_stats AS (
            SELECT 
                session_id,
                user_id,
                channel,
                COUNT(*) as turns,
                SUM(tokens_in) as tokens_in,
                SUM(tokens_out) as tokens_out,
                AVG(latency_ms) as avg_latency_ms,
                MIN(to_timestamp(ts_ms / 1000.0)) as started_at,
                MAX(to_timestamp(ts_ms / 1000.0)) as ended_at
            FROM agent_turns
            WHERE session_id = $1
            GROUP BY session_id, user_id, channel
        )
        INSERT INTO session_kpis (
            session_id, user_id, channel, turns, tokens_in, tokens_out,
            avg_latency_ms, started_at, ended_at, updated_at
        )
        SELECT 
            session_id, user_id, channel, turns, tokens_in, tokens_out,
            avg_latency_ms, started_at, ended_at, NOW()
        FROM session_stats
        ON CONFLICT (session_id)
        DO UPDATE SET
            user_id = EXCLUDED.user_id,
            channel = EXCLUDED.channel,
            turns = EXCLUDED.turns,
            tokens_in = EXCLUDED.tokens_in,
            tokens_out = EXCLUDED.tokens_out,
            avg_latency_ms = EXCLUDED.avg_latency_ms,
            started_at = EXCLUDED.started_at,
            ended_at = EXCLUDED.ended_at,
            updated_at = NOW()
        """
        
        async with self.get_connection() as conn:
            await conn.execute(query, session_id)
    
    async def get_session_metrics(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session metrics for Redis hot state"""
        query = """
        SELECT 
            session_id,
            turns,
            avg_latency_ms,
            updated_at
        FROM session_kpis
        WHERE session_id = $1
        """
        
        async with self.get_connection() as conn:
            row = await conn.fetchrow(query, session_id)
            if row:
                return {
                    'session_id': row['session_id'],
                    'total_turns': row['turns'],
                    'avg_response_time': row['avg_latency_ms'],
                    'last_activity': row['updated_at'].isoformat()
                }
        return None


class RedisManager:
    """Redis operations manager for hot session state"""
    
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
    
    async def update_session_state(self, session_id: str, turn_data: Dict[str, Any], seq: int) -> None:
        """Update session hot state with sequence guard"""
        state_key = f"session:{session_id}:state"
        
        # Get current sequence number
        current_seq = await self.redis.hget(state_key, "seq")
        if current_seq and int(current_seq) >= seq:
            logger.debug(f"Skipping update for {session_id}, sequence {seq} <= {current_seq}")
            return
        
        # Update session state
        state_data = {
            "last_turn_id": turn_data.get("turn_id", ""),
            "last_message": turn_data.get("response_text", "")[:200] if turn_data.get("response_text") else "",
            "seq": seq,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "channel": turn_data.get("channel", "text"),
            "status": "active"
        }
        
        pipe = self.redis.pipeline()
        pipe.hset(state_key, mapping=state_data)
        pipe.expire(state_key, 3600)  # 1 hour TTL
        await pipe.execute()
        
        logger.debug(f"Updated session state for {session_id} with seq {seq}")
    
    async def update_session_metrics(self, session_id: str, metrics: Dict[str, Any]) -> None:
        """Update session metrics in Redis"""
        metrics_key = f"session:{session_id}:metrics"
        
        pipe = self.redis.pipeline()
        pipe.hset(metrics_key, mapping=metrics)
        pipe.expire(metrics_key, 86400)  # 24 hours TTL
        await pipe.execute()
        
        logger.debug(f"Updated session metrics for {session_id}")


class KPIConsumer:
    """Main KPI consumer class"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.load_config()
        self.db_manager = DatabaseManager(self.postgres_dsn)
        self.redis_manager = RedisManager(self.redis_url)
        self.kafka_consumer = self.create_kafka_consumer()
        self.running = False
        
    def load_config(self) -> None:
        """Load configuration from YAML file"""
        with open(self.config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Load configurations
        self.kpi_config = KPIConsumerConfig(**config_data['processing']['kpi_consumer'])
        
        # Build connection strings
        pg_config = config_data['postgresql']
        self.postgres_dsn = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
            f"?sslmode={pg_config['sslmode']}"
        )
        
        redis_config = config_data['redis']
        self.redis_url = (
            f"rediss://:{os.getenv('REDIS_PASSWORD')}@{os.getenv('REDIS_HOST')}:"
            f"{os.getenv('REDIS_PORT')}/{redis_config['db']}"
        )
        
        # Kafka topics to consume
        self.kafka_topics = [
            config_data['kafka']['topics']['text_agent_turn_completed']['name'],
            config_data['kafka']['topics']['frontend_user_interaction']['name']
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
            group_id=self.kpi_config.consumer_group,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            max_poll_records=self.kpi_config.batch_size,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000
        )
    
    async def process_turn_event(self, event_data: Dict[str, Any]) -> None:
        """Process a single turn completion event"""
        try:
            turn_data = event_data.get('data', {})
            
            # Validate required fields
            if not all(key in turn_data for key in ['session_id', 'turn_id']):
                logger.warning(f"Missing required fields in turn event: {turn_data}")
                return
            
            # Create database record
            turn_record = AgentTurnRecord(
                session_id=turn_data['session_id'],
                turn_id=turn_data['turn_id'],
                user_id=turn_data.get('user_id'),
                channel=turn_data.get('channel', 'text'),
                model=turn_data.get('model', 'unknown'),
                tokens_in=turn_data.get('tokens_in', 0),
                tokens_out=turn_data.get('tokens_out', 0),
                latency_ms=turn_data.get('latency_ms', 0.0),
                response_text=turn_data.get('response_text'),
                ts_ms=turn_data.get('ts_ms', int(time.time() * 1000))
            )
            
            # Update database
            await self.db_manager.upsert_agent_turn(turn_record)
            await self.db_manager.update_session_kpis(turn_record.session_id)
            
            # Update Redis hot state
            seq = turn_data.get('metadata', {}).get('seq', int(time.time()))
            await self.redis_manager.update_session_state(
                turn_record.session_id, 
                turn_data, 
                seq
            )
            
            # Update Redis metrics
            session_metrics = await self.db_manager.get_session_metrics(turn_record.session_id)
            if session_metrics:
                await self.redis_manager.update_session_metrics(
                    turn_record.session_id, 
                    session_metrics
                )
            
            logger.debug(f"Processed turn event for session {turn_record.session_id}")
            
        except Exception as e:
            logger.error(f"Error processing turn event: {e}, data: {event_data}")
            raise
    
    async def process_frontend_event(self, event_data: Dict[str, Any]) -> None:
        """Process frontend interaction event"""
        try:
            frontend_data = event_data.get('data', {})
            
            # Only process events that have session information
            if 'session_id' not in frontend_data:
                return
            
            # Update Redis session activity
            session_id = frontend_data['session_id']
            seq = int(time.time())
            
            session_data = {
                'last_activity': datetime.now(timezone.utc).isoformat(),
                'last_event_type': frontend_data.get('event_type', 'unknown')
            }
            
            await self.redis_manager.update_session_state(
                session_id,
                session_data,
                seq
            )
            
            logger.debug(f"Updated session activity for {session_id}")
            
        except Exception as e:
            logger.error(f"Error processing frontend event: {e}, data: {event_data}")
    
    async def process_batch(self, messages: List[Any]) -> None:
        """Process a batch of Kafka messages"""
        if not messages:
            return
        
        processed_count = 0
        errors = []
        
        for message in messages:
            try:
                event_data = message.value
                if not event_data:
                    continue
                
                event_type = event_data.get('event_type', '')
                
                if 'text_agent.turn_completed' in event_type:
                    await self.process_turn_event(event_data)
                elif 'frontend.user.interaction' in event_type:
                    await self.process_frontend_event(event_data)
                else:
                    logger.debug(f"Skipping unknown event type: {event_type}")
                    continue
                
                processed_count += 1
                
            except Exception as e:
                errors.append(f"Message processing error: {e}")
                logger.error(f"Error processing message: {e}")
        
        logger.info(f"Processed batch: {processed_count} events, {len(errors)} errors")
        
        if errors and len(errors) > len(messages) * 0.5:  # More than 50% errors
            raise Exception(f"High error rate in batch: {errors[:5]}")  # Show first 5 errors
    
    async def run_consumer(self) -> None:
        """Run the main consumer loop"""
        self.running = True
        logger.info("Starting KPI consumer")
        
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
                        max_records=self.kpi_config.batch_size
                    )
                    
                    if messages:
                        # Flatten messages from all topic partitions
                        all_messages = []
                        for topic_partition, partition_messages in messages.items():
                            all_messages.extend(partition_messages)
                        
                        # Process batch
                        await self.process_batch(all_messages)
                        
                        # Commit offsets
                        self.kafka_consumer.commit()
                        
                    else:
                        # Brief sleep when no messages
                        await asyncio.sleep(0.1)
                        
                except Exception as e:
                    logger.error(f"Consumer loop error: {e}")
                    await asyncio.sleep(1)  # Brief pause before retry
                    
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
        
        logger.info("KPI consumer cleanup completed")
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for monitoring"""
        status = {
            'status': 'healthy' if self.running else 'stopped',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'consumer_group': self.kpi_config.consumer_group,
            'topics': self.kafka_topics
        }
        
        try:
            # Check database connection
            async with self.db_manager.get_connection() as conn:
                await conn.fetchval('SELECT 1')
            status['database'] = 'connected'
        except Exception as e:
            status['database'] = f'error: {e}'
        
        try:
            # Check Redis connection
            await self.redis_manager.redis.ping()
            status['redis'] = 'connected'
        except Exception as e:
            status['redis'] = f'error: {e}'
        
        return status


async def main():
    """Main execution function"""
    import signal
    
    consumer = KPIConsumer()
    
    # Handle graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        consumer.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    await consumer.run_consumer()


if __name__ == "__main__":
    asyncio.run(main())