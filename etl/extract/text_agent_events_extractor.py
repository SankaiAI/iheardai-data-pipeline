#!/usr/bin/env python3
"""
Text Agent Events Extractor for iheardAI Data Pipeline

This module integrates with the text-agent-server to capture and publish
agent turn completions, tool invocations, and session data to Kafka.
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


class TextAgentConfig(BaseModel):
    """Text Agent API configuration"""
    api_url: str = Field(..., description="Text Agent API base URL")
    api_key: str = Field(..., description="API authentication key")
    batch_size: int = Field(default=100, description="Batch size for event processing")
    poll_interval: int = Field(default=30, description="Polling interval in seconds")


class AgentTurnEvent(BaseModel):
    """Agent turn completion event schema"""
    session_id: str = Field(..., description="Session identifier")
    turn_id: str = Field(..., description="Turn identifier")
    user_id: Optional[str] = Field(None, description="User identifier")
    channel: str = Field(default="text", description="Communication channel")
    model: str = Field(..., description="Language model used")
    tokens_in: int = Field(..., description="Input tokens consumed")
    tokens_out: int = Field(..., description="Output tokens generated")
    latency_ms: float = Field(..., description="Response latency in milliseconds")
    response_text: Optional[str] = Field(None, description="Response text (may be redacted)")
    ts_ms: int = Field(..., description="Timestamp in milliseconds")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional turn data")
    
    @validator('channel')
    def validate_channel(cls, v):
        allowed_channels = ['text', 'voice', 'api']
        if v not in allowed_channels:
            raise ValueError(f"Invalid channel: {v}")
        return v


class ToolInvocationEvent(BaseModel):
    """Tool invocation event schema"""
    session_id: str = Field(..., description="Session identifier")
    turn_id: str = Field(..., description="Turn identifier")
    tool_name: str = Field(..., description="Name of invoked tool")
    tool_input: Dict[str, Any] = Field(..., description="Tool input parameters")
    tool_output: Optional[Dict[str, Any]] = Field(None, description="Tool output result")
    execution_time_ms: float = Field(..., description="Tool execution time")
    success: bool = Field(..., description="Whether tool execution succeeded")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    ts_ms: int = Field(..., description="Timestamp in milliseconds")


class TextAgentAPIClient:
    """Text Agent API client for event extraction"""
    
    def __init__(self, config: TextAgentConfig):
        self.config = config
        
    async def get_turn_events(self, since: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get turn completion events from text agent server"""
        url = f"{self.config.api_url}/api/analytics/turns"
        headers = {
            'Authorization': f'Bearer {self.config.api_key}',
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
                        events = data.get('turns', [])
                        logger.info(f"Retrieved {len(events)} turn events from text agent")
                        return events
                    elif response.status == 404:
                        logger.warning("Turn analytics endpoint not found")
                        return []
                    else:
                        logger.error(f"Failed to get turn events: {response.status}")
                        return []
            except Exception as e:
                logger.error(f"Error fetching turn events: {e}")
                return []
    
    async def get_tool_events(self, since: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get tool invocation events from text agent server"""
        url = f"{self.config.api_url}/api/analytics/tools"
        headers = {
            'Authorization': f'Bearer {self.config.api_key}',
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
                        events = data.get('tool_invocations', [])
                        logger.info(f"Retrieved {len(events)} tool events from text agent")
                        return events
                    elif response.status == 404:
                        logger.warning("Tool analytics endpoint not found")
                        return []
                    else:
                        logger.error(f"Failed to get tool events: {response.status}")
                        return []
            except Exception as e:
                logger.error(f"Error fetching tool events: {e}")
                return []
    
    async def get_session_updates(self, since: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get session update events from text agent server"""
        url = f"{self.config.api_url}/api/analytics/sessions"
        headers = {
            'Authorization': f'Bearer {self.config.api_key}',
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
                        events = data.get('sessions', [])
                        logger.info(f"Retrieved {len(events)} session events from text agent")
                        return events
                    else:
                        logger.warning(f"Session analytics endpoint returned: {response.status}")
                        return []
            except Exception as e:
                logger.error(f"Error fetching session events: {e}")
                return []


class TextAgentWebSocketClient:
    """Real-time event collection from text agent WebSocket"""
    
    def __init__(self, websocket_url: str, api_key: str):
        self.websocket_url = websocket_url
        self.api_key = api_key
        
    async def connect_and_collect(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Connect to WebSocket and yield real-time events"""
        try:
            import websockets
            
            headers = {'Authorization': f'Bearer {self.api_key}'}
            
            async with websockets.connect(self.websocket_url, extra_headers=headers) as websocket:
                logger.info("Connected to text agent WebSocket")
                
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


class TextAgentEventsExtractor:
    """Main text agent events extraction class"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.load_config()
        self.text_agent_client = TextAgentAPIClient(self.text_agent_config)
        self.kafka_producer = self.create_kafka_producer()
        self.last_event_timestamps = {
            'turns': None,
            'tools': None,
            'sessions': None
        }
        
    def load_config(self) -> None:
        """Load configuration from YAML file"""
        with open(self.config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Load configurations
        self.text_agent_config = TextAgentConfig(
            api_url=os.getenv('TEXT_AGENT_API_URL', 'http://localhost:8000'),
            api_key=os.getenv('TEXT_AGENT_API_KEY', 'default_key'),
            batch_size=config_data.get('text_agent', {}).get('batch_size', 100),
            poll_interval=config_data.get('text_agent', {}).get('poll_interval', 30)
        )
        
        self.kafka_topics = {
            'turns': config_data['kafka']['topics']['text_agent_turn_completed']['name'],
            'tools': config_data['kafka']['topics']['agent_tool_invoked']['name'],
            'sessions': 'text.agent.session.updated'  # Add to config if needed
        }
        
        self.kafka_config = {
            'brokers': config_data['kafka']['brokers'],
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
    
    def normalize_turn_event(self, raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize turn event to standard schema"""
        try:
            # Apply PII redaction to response text
            response_text = raw_event.get('response_text', '')
            if response_text:
                response_text = self.redact_pii(response_text)
            
            normalized = {
                'session_id': raw_event.get('session_id', ''),
                'turn_id': raw_event.get('turn_id', ''),
                'user_id': raw_event.get('user_id'),
                'channel': raw_event.get('channel', 'text'),
                'model': raw_event.get('model', 'unknown'),
                'tokens_in': raw_event.get('tokens_in', 0),
                'tokens_out': raw_event.get('tokens_out', 0),
                'latency_ms': raw_event.get('latency_ms', 0.0),
                'response_text': response_text,
                'ts_ms': raw_event.get('timestamp', int(time.time() * 1000)),
                'metadata': raw_event.get('metadata', {})
            }
            
            # Validate with Pydantic
            validated_event = AgentTurnEvent(**normalized)
            return validated_event.dict()
            
        except Exception as e:
            logger.error(f"Error normalizing turn event: {e}")
            return None
    
    def normalize_tool_event(self, raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize tool invocation event to standard schema"""
        try:
            normalized = {
                'session_id': raw_event.get('session_id', ''),
                'turn_id': raw_event.get('turn_id', ''),
                'tool_name': raw_event.get('tool_name', 'unknown'),
                'tool_input': raw_event.get('tool_input', {}),
                'tool_output': raw_event.get('tool_output'),
                'execution_time_ms': raw_event.get('execution_time_ms', 0.0),
                'success': raw_event.get('success', False),
                'error_message': raw_event.get('error_message'),
                'ts_ms': raw_event.get('timestamp', int(time.time() * 1000))
            }
            
            # Validate with Pydantic
            validated_event = ToolInvocationEvent(**normalized)
            return validated_event.dict()
            
        except Exception as e:
            logger.error(f"Error normalizing tool event: {e}")
            return None
    
    def redact_pii(self, text: str) -> str:
        """Apply PII redaction to text content"""
        # Simple PII redaction - replace with more sophisticated solution
        import re
        
        # Redact email addresses
        text = re.sub(r'\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b', '[EMAIL_REDACTED]', text)
        
        # Redact phone numbers (basic pattern)
        text = re.sub(r'\\b\\d{3}-\\d{3}-\\d{4}\\b', '[PHONE_REDACTED]', text)
        
        # Redact credit card numbers (basic pattern)
        text = re.sub(r'\\b\\d{4}[\\s-]?\\d{4}[\\s-]?\\d{4}[\\s-]?\\d{4}\\b', '[CARD_REDACTED]', text)
        
        return text
    
    def create_kafka_event(self, event_data: Dict[str, Any], event_type: str) -> Dict[str, Any]:
        """Create Kafka event wrapper"""
        return {
            'event_id': str(uuid4()),
            'event_type': f'text_agent.{event_type}',
            'source': 'text_agent_server',
            'ts_ms': int(datetime.now(timezone.utc).timestamp() * 1000),
            'data': event_data
        }
    
    async def publish_events(self, events: List[Dict[str, Any]], topic: str, event_type: str) -> None:
        """Publish batch of events to specific Kafka topic"""
        if not events:
            return
            
        published_count = 0
        max_timestamp = None
        
        for raw_event in events:
            # Normalize event based on type
            if event_type == 'turn_completed':
                normalized_event = self.normalize_turn_event(raw_event)
            elif event_type == 'tool_invoked':
                normalized_event = self.normalize_tool_event(raw_event)
            else:
                normalized_event = raw_event  # Session events don't need special normalization
            
            if normalized_event:
                kafka_event = self.create_kafka_event(normalized_event, event_type)
                key = normalized_event.get('session_id', '')
                
                self.kafka_producer.send(
                    topic,
                    key=key,
                    value=kafka_event
                )
                published_count += 1
                
                # Track latest timestamp for checkpointing
                event_ts = normalized_event.get('ts_ms')
                if event_ts and (not max_timestamp or event_ts > max_timestamp):
                    max_timestamp = event_ts
        
        # Flush producer
        self.kafka_producer.flush()
        logger.info(f"Published {published_count} {event_type} events to {topic}")
        
        # Update timestamp checkpoint
        if max_timestamp:
            checkpoint_key = event_type.replace('_completed', '').replace('_invoked', '')
            if checkpoint_key in self.last_event_timestamps:
                self.last_event_timestamps[checkpoint_key] = max_timestamp
    
    async def poll_all_events(self) -> None:
        """Poll all event types from text agent server"""
        try:
            # Prepare since timestamps
            since_timestamps = {}
            for event_type, timestamp in self.last_event_timestamps.items():
                if timestamp:
                    since_timestamps[event_type] = datetime.fromtimestamp(
                        timestamp / 1000, tz=timezone.utc
                    ).isoformat()
            
            # Fetch all event types concurrently
            turn_events_task = self.text_agent_client.get_turn_events(
                since=since_timestamps.get('turns'),
                limit=self.text_agent_config.batch_size
            )
            
            tool_events_task = self.text_agent_client.get_tool_events(
                since=since_timestamps.get('tools'),
                limit=self.text_agent_config.batch_size
            )
            
            session_events_task = self.text_agent_client.get_session_updates(
                since=since_timestamps.get('sessions'),
                limit=self.text_agent_config.batch_size
            )
            
            # Wait for all requests to complete
            turn_events, tool_events, session_events = await asyncio.gather(
                turn_events_task, tool_events_task, session_events_task
            )
            
            # Publish events to respective topics
            await asyncio.gather(
                self.publish_events(turn_events, self.kafka_topics['turns'], 'turn_completed'),
                self.publish_events(tool_events, self.kafka_topics['tools'], 'tool_invoked'),
                self.publish_events(session_events, self.kafka_topics['sessions'], 'session_updated')
            )
            
        except Exception as e:
            logger.error(f"Error polling events: {e}")
    
    async def run_continuous_polling(self) -> None:
        """Run continuous event polling"""
        logger.info(f"Starting continuous polling every {self.text_agent_config.poll_interval} seconds")
        
        while True:
            try:
                await self.poll_all_events()
                await asyncio.sleep(self.text_agent_config.poll_interval)
            except KeyboardInterrupt:
                logger.info("Stopping event polling")
                break
            except Exception as e:
                logger.error(f"Polling error: {e}")
                await asyncio.sleep(10)  # Brief pause before retry
    
    async def run_websocket_collection(self) -> None:
        """Run real-time WebSocket event collection"""
        websocket_url = f"{self.text_agent_config.api_url.replace('http', 'ws')}/ws/events"
        client = TextAgentWebSocketClient(websocket_url, self.text_agent_config.api_key)
        
        logger.info("Starting WebSocket event collection")
        
        try:
            async for event_data in client.connect_and_collect():
                # Determine event type and publish to appropriate topic
                event_type = event_data.get('type', 'unknown')
                
                if event_type == 'turn_completed':
                    await self.publish_events([event_data], self.kafka_topics['turns'], 'turn_completed')
                elif event_type == 'tool_invoked':
                    await self.publish_events([event_data], self.kafka_topics['tools'], 'tool_invoked')
                elif event_type == 'session_updated':
                    await self.publish_events([event_data], self.kafka_topics['sessions'], 'session_updated')
                    
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
    
    parser = argparse.ArgumentParser(description='Text Agent Events Extractor')
    parser.add_argument('--mode', choices=['polling', 'websocket', 'hybrid'], 
                       default='polling', help='Collection mode')
    parser.add_argument('--config', default='config/config.yaml', 
                       help='Configuration file path')
    
    args = parser.parse_args()
    
    extractor = TextAgentEventsExtractor(args.config)
    await extractor.run(args.mode)


if __name__ == "__main__":
    asyncio.run(main())