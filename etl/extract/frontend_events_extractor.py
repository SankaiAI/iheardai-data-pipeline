#!/usr/bin/env python3
"""
Frontend Events Extractor for iheardAI Data Pipeline

This module integrates with the iheardAI_Frontend to capture and publish
user interaction events, analytics data, and widget performance metrics to Kafka.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, AsyncGenerator
from uuid import uuid4

import aiohttp
from kafka import KafkaProducer
from pydantic import BaseModel, Field, validator
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FrontendConfig(BaseModel):
    """Frontend API configuration"""
    api_url: str = Field(..., description="Frontend API base URL")
    webhook_secret: str = Field(..., description="Webhook authentication secret")
    batch_size: int = Field(default=100, description="Batch size for event processing")
    poll_interval: int = Field(default=30, description="Polling interval in seconds")


class EventSchema(BaseModel):
    """Frontend event schema validation"""
    event_id: str = Field(..., description="Unique event identifier")
    session_id: str = Field(..., description="Session identifier")
    user_id: Optional[str] = Field(None, description="User identifier")
    event_type: str = Field(..., description="Event type")
    page_url: str = Field(..., description="Page URL where event occurred")
    widget_id: Optional[str] = Field(None, description="Widget identifier")
    interaction_type: str = Field(..., description="Type of interaction")
    ts_ms: int = Field(..., description="Timestamp in milliseconds")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional event data")
    
    @validator('event_type')
    def validate_event_type(cls, v):
        allowed_types = [
            'widget_load', 'widget_open', 'widget_close', 'message_sent',
            'message_received', 'voice_start', 'voice_end', 'page_view',
            'click', 'form_submit', 'error', 'performance'
        ]
        if v not in allowed_types:
            raise ValueError(f"Invalid event_type: {v}")
        return v


class FrontendAPIClient:
    """Frontend API client for event extraction"""
    
    def __init__(self, config: FrontendConfig):
        self.config = config
        
    async def get_events(self, since: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get events from frontend API"""
        url = f"{self.config.api_url}/api/analytics"
        headers = {
            'Authorization': f'Bearer {self.config.webhook_secret}',
            'Content-Type': 'application/json'
        }
        
        params = {'limit': limit}
        if since:
            params['since'] = since
            
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        events = data.get('events', [])
                        logger.info(f"Retrieved {len(events)} events from frontend")
                        return events
                    elif response.status == 404:
                        logger.warning("Analytics endpoint not found, returning empty list")
                        return []
                    else:
                        logger.error(f"Failed to get events: {response.status}")
                        return []
            except Exception as e:
                logger.error(f"Error fetching events: {e}")
                return []
    
    async def get_widget_performance(self) -> List[Dict[str, Any]]:
        """Get widget performance metrics"""
        url = f"{self.config.api_url}/api/analytics/widget-performance"
        headers = {
            'Authorization': f'Bearer {self.config.webhook_secret}',
            'Content-Type': 'application/json'
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        metrics = data.get('metrics', [])
                        logger.info(f"Retrieved {len(metrics)} widget performance metrics")
                        return metrics
                    else:
                        logger.warning(f"Widget performance endpoint returned: {response.status}")
                        return []
            except Exception as e:
                logger.error(f"Error fetching widget performance: {e}")
                return []


class WebSocketEventCollector:
    """Collects real-time events from frontend WebSocket"""
    
    def __init__(self, websocket_url: str, auth_token: str):
        self.websocket_url = websocket_url
        self.auth_token = auth_token
        self.event_queue = asyncio.Queue()
        
    async def connect_and_collect(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Connect to WebSocket and yield events"""
        try:
            import websockets
            
            headers = {'Authorization': f'Bearer {self.auth_token}'}
            
            async with websockets.connect(self.websocket_url, extra_headers=headers) as websocket:
                logger.info("Connected to frontend WebSocket")
                
                async for message in websocket:
                    try:
                        event_data = json.loads(message)
                        yield event_data
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON received: {e}")
                    except Exception as e:
                        logger.error(f"Error processing WebSocket message: {e}")
                        
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")


class FrontendEventsExtractor:
    """Main frontend events extraction class"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.load_config()
        self.frontend_client = FrontendAPIClient(self.frontend_config)
        self.kafka_producer = self.create_kafka_producer()
        self.last_event_timestamp = None
        
    def load_config(self) -> None:
        """Load configuration from YAML file"""
        with open(self.config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Load configurations
        self.frontend_config = FrontendConfig(
            api_url=os.getenv('FRONTEND_API_URL', config_data.get('frontend', {}).get('api_url', 'http://localhost:3000')),
            webhook_secret=os.getenv('FRONTEND_WEBHOOK_SECRET', 'default_secret'),
            batch_size=config_data.get('frontend', {}).get('batch_size', 100),
            poll_interval=config_data.get('frontend', {}).get('poll_interval', 30)
        )
        
        self.kafka_config = {
            'brokers': config_data['kafka']['brokers'],
            'topic': config_data['kafka']['topics']['frontend_user_interaction']['name'],
            'sasl_username': os.getenv('KAFKA_USER'),
            'sasl_password': os.getenv('KAFKA_PASSWORD')
        }
    
    def create_kafka_producer(self) -> KafkaProducer:
        """Create Kafka producer instance"""
        return KafkaProducer(
            bootstrap_servers=self.kafka_config['brokers'],
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=self.kafka_config['sasl_username'],
            sasl_plain_password=self.kafka_config['sasl_password'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            enable_idempotence=True,
            compression_type='snappy'
        )
    
    def normalize_event(self, raw_event: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize frontend event to standard schema"""
        try:
            # Create normalized event
            normalized = {
                'event_id': raw_event.get('event_id', str(uuid4())),
                'session_id': raw_event.get('session_id', ''),
                'user_id': raw_event.get('user_id'),
                'event_type': raw_event.get('event_type', 'unknown'),
                'page_url': raw_event.get('page_url', ''),
                'widget_id': raw_event.get('widget_id'),
                'interaction_type': raw_event.get('interaction_type', 'unknown'),
                'ts_ms': raw_event.get('timestamp', int(time.time() * 1000)),
                'metadata': raw_event.get('metadata', {})
            }
            
            # Validate with Pydantic
            validated_event = EventSchema(**normalized)
            return validated_event.dict()
            
        except Exception as e:
            logger.error(f"Error normalizing event: {e}, raw_event: {raw_event}")
            return None
    
    def create_kafka_event(self, frontend_event: Dict[str, Any]) -> Dict[str, Any]:
        """Create Kafka event wrapper"""
        return {
            'event_id': str(uuid4()),
            'event_type': 'frontend.user.interaction',
            'source': 'iheardai_frontend',
            'ts_ms': int(datetime.now(timezone.utc).timestamp() * 1000),
            'data': frontend_event
        }
    
    async def publish_event(self, event_data: Dict[str, Any]) -> None:
        """Publish single event to Kafka"""
        try:
            normalized_event = self.normalize_event(event_data)
            if normalized_event:
                kafka_event = self.create_kafka_event(normalized_event)
                key = normalized_event.get('session_id', '')
                
                self.kafka_producer.send(
                    self.kafka_config['topic'],
                    key=key,
                    value=kafka_event
                )
                
                # Update last event timestamp
                event_ts = normalized_event.get('ts_ms')
                if event_ts and (not self.last_event_timestamp or event_ts > self.last_event_timestamp):
                    self.last_event_timestamp = event_ts
                    
        except Exception as e:
            logger.error(f"Error publishing event: {e}")
    
    async def publish_batch(self, events: List[Dict[str, Any]]) -> None:
        """Publish batch of events to Kafka"""
        if not events:
            return
            
        published_count = 0
        for event_data in events:
            await self.publish_event(event_data)
            published_count += 1
        
        # Flush producer
        self.kafka_producer.flush()
        logger.info(f"Published {published_count} frontend events to Kafka")
    
    async def poll_events(self) -> None:
        """Poll frontend API for new events"""
        try:
            since_timestamp = None
            if self.last_event_timestamp:
                since_timestamp = datetime.fromtimestamp(
                    self.last_event_timestamp / 1000, tz=timezone.utc
                ).isoformat()
            
            events = await self.frontend_client.get_events(
                since=since_timestamp,
                limit=self.frontend_config.batch_size
            )
            
            if events:
                await self.publish_batch(events)
            
            # Also collect widget performance metrics
            performance_metrics = await self.frontend_client.get_widget_performance()
            if performance_metrics:
                # Transform performance metrics to events
                performance_events = []
                for metric in performance_metrics:
                    event = {
                        'event_id': str(uuid4()),
                        'session_id': metric.get('session_id', ''),
                        'user_id': metric.get('user_id'),
                        'event_type': 'performance',
                        'page_url': metric.get('page_url', ''),
                        'widget_id': metric.get('widget_id'),
                        'interaction_type': 'performance_metric',
                        'timestamp': int(time.time() * 1000),
                        'metadata': metric
                    }
                    performance_events.append(event)
                
                await self.publish_batch(performance_events)
                
        except Exception as e:
            logger.error(f"Error polling events: {e}")
    
    async def run_continuous_polling(self) -> None:
        """Run continuous event polling"""
        logger.info(f"Starting continuous polling every {self.frontend_config.poll_interval} seconds")
        
        while True:
            try:
                await self.poll_events()
                await asyncio.sleep(self.frontend_config.poll_interval)
            except KeyboardInterrupt:
                logger.info("Stopping event polling")
                break
            except Exception as e:
                logger.error(f"Polling error: {e}")
                await asyncio.sleep(10)  # Brief pause before retry
    
    async def run_websocket_collection(self) -> None:
        """Run real-time WebSocket event collection"""
        websocket_url = f"{self.frontend_config.api_url.replace('http', 'ws')}/ws/events"
        collector = WebSocketEventCollector(websocket_url, self.frontend_config.webhook_secret)
        
        logger.info("Starting WebSocket event collection")
        
        try:
            async for event_data in collector.connect_and_collect():
                await self.publish_event(event_data)
        except Exception as e:
            logger.error(f"WebSocket collection error: {e}")
    
    async def run(self, mode: str = "polling") -> None:
        """Run the extractor in specified mode"""
        try:
            if mode == "polling":
                await self.run_continuous_polling()
            elif mode == "websocket":
                await self.run_websocket_collection()
            elif mode == "hybrid":
                # Run both polling and WebSocket collection
                await asyncio.gather(
                    self.run_continuous_polling(),
                    self.run_websocket_collection()
                )
            else:
                raise ValueError(f"Unknown mode: {mode}")
        finally:
            if hasattr(self, 'kafka_producer'):
                self.kafka_producer.close()


async def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Frontend Events Extractor')
    parser.add_argument('--mode', choices=['polling', 'websocket', 'hybrid'], 
                       default='polling', help='Collection mode')
    parser.add_argument('--config', default='config/config.yaml', 
                       help='Configuration file path')
    
    args = parser.parse_args()
    
    extractor = FrontendEventsExtractor(args.config)
    await extractor.run(args.mode)


if __name__ == "__main__":
    asyncio.run(main())