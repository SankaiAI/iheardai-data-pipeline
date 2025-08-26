#!/usr/bin/env python3
"""
Marketo Data Extractor for iheardAI Data Pipeline

This module extracts lead data from Marketo using the Bulk Lead Export API
and publishes the data to Kafka for downstream processing.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from uuid import uuid4

import aiohttp
import pandas as pd
from kafka import KafkaProducer
from pydantic import BaseModel, Field
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MarketoConfig(BaseModel):
    """Marketo API configuration"""
    base_url: str = Field(..., description="Marketo REST API base URL")
    client_id: str = Field(..., description="Marketo client ID")
    client_secret: str = Field(..., description="Marketo client secret")
    batch_size: int = Field(default=300, description="Batch size for export")
    fields: List[str] = Field(
        default=[
            "id", "email", "firstName", "lastName", 
            "createdAt", "updatedAt", "leadSource", "originalSourceType"
        ],
        description="Fields to extract"
    )


class KafkaConfig(BaseModel):
    """Kafka configuration"""
    brokers: List[str] = Field(..., description="Kafka broker URLs")
    topic: str = Field(default="marketo.leads.delta", description="Kafka topic")
    security_protocol: str = Field(default="SASL_SSL")
    sasl_mechanism: str = Field(default="PLAIN")
    sasl_username: str = Field(..., description="Kafka SASL username")
    sasl_password: str = Field(..., description="Kafka SASL password")


class CheckpointManager:
    """Manages extraction checkpoints"""
    
    def __init__(self, checkpoint_file: str = "/data/marketo_checkpoint.json"):
        self.checkpoint_file = checkpoint_file
        
    def get_last_checkpoint(self) -> Optional[str]:
        """Get the last successful extraction timestamp"""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    return data.get('last_updated_at')
        except Exception as e:
            logger.error(f"Error reading checkpoint: {e}")
        return None
    
    def save_checkpoint(self, updated_at: str) -> None:
        """Save the current extraction timestamp"""
        try:
            os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
            with open(self.checkpoint_file, 'w') as f:
                json.dump({
                    'last_updated_at': updated_at,
                    'updated_by': 'marketo_extractor',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }, f, indent=2)
            logger.info(f"Checkpoint saved: {updated_at}")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")


class MarketoAPIClient:
    """Marketo REST API client"""
    
    def __init__(self, config: MarketoConfig):
        self.config = config
        self.access_token = None
        self.token_expires_at = 0
        
    async def get_access_token(self) -> str:
        """Get or refresh Marketo access token"""
        if self.access_token and time.time() < self.token_expires_at - 300:
            return self.access_token
            
        auth_url = f"{self.config.base_url}/identity/oauth/token"
        params = {
            'grant_type': 'client_credentials',
            'client_id': self.config.client_id,
            'client_secret': self.config.client_secret
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(auth_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    self.access_token = data['access_token']
                    self.token_expires_at = time.time() + data['expires_in']
                    logger.info("Marketo access token refreshed")
                    return self.access_token
                else:
                    raise Exception(f"Failed to get Marketo access token: {response.status}")
    
    async def create_export_job(self, updated_at_filter: Optional[str] = None) -> str:
        """Create a bulk lead export job"""
        token = await self.get_access_token()
        
        url = f"{self.config.base_url}/bulk/v1/leads/export/create.json"
        headers = {'Authorization': f'Bearer {token}'}
        
        filter_config = {}
        if updated_at_filter:
            filter_config = {
                'updatedAt': {
                    'startAt': updated_at_filter,
                    'endAt': datetime.now(timezone.utc).isoformat()
                }
            }
        
        payload = {
            'fields': self.config.fields,
            'format': 'CSV'
        }
        
        if filter_config:
            payload['filter'] = filter_config
            
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    if data['success']:
                        export_id = data['result'][0]['exportId']
                        logger.info(f"Export job created: {export_id}")
                        return export_id
                    else:
                        raise Exception(f"Export job creation failed: {data['errors']}")
                else:
                    raise Exception(f"Failed to create export job: {response.status}")
    
    async def enqueue_export_job(self, export_id: str) -> None:
        """Enqueue the export job for processing"""
        token = await self.get_access_token()
        
        url = f"{self.config.base_url}/bulk/v1/leads/export/{export_id}/enqueue.json"
        headers = {'Authorization': f'Bearer {token}'}
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    if data['success']:
                        logger.info(f"Export job enqueued: {export_id}")
                    else:
                        raise Exception(f"Failed to enqueue job: {data['errors']}")
                else:
                    raise Exception(f"Failed to enqueue export job: {response.status}")
    
    async def get_export_status(self, export_id: str) -> Dict[str, Any]:
        """Get export job status"""
        token = await self.get_access_token()
        
        url = f"{self.config.base_url}/bulk/v1/leads/export/{export_id}/status.json"
        headers = {'Authorization': f'Bearer {token}'}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    if data['success']:
                        return data['result'][0]
                    else:
                        raise Exception(f"Failed to get status: {data['errors']}")
                else:
                    raise Exception(f"Failed to get export status: {response.status}")
    
    async def wait_for_completion(self, export_id: str, timeout: int = 1800) -> None:
        """Wait for export job to complete"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = await self.get_export_status(export_id)
            
            if status['status'] == 'Completed':
                logger.info(f"Export job completed: {export_id}")
                return
            elif status['status'] == 'Failed':
                raise Exception(f"Export job failed: {export_id}")
            elif status['status'] in ['Created', 'Queued', 'Processing']:
                logger.info(f"Export job status: {status['status']}")
                await asyncio.sleep(30)
            else:
                logger.warning(f"Unknown export status: {status['status']}")
                await asyncio.sleep(30)
        
        raise Exception(f"Export job timeout: {export_id}")
    
    async def download_export_file(self, export_id: str) -> pd.DataFrame:
        """Download and parse export file"""
        token = await self.get_access_token()
        
        url = f"{self.config.base_url}/bulk/v1/leads/export/{export_id}/file.json"
        headers = {'Authorization': f'Bearer {token}'}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    csv_data = await response.text()
                    df = pd.read_csv(pd.StringIO(csv_data))
                    logger.info(f"Downloaded {len(df)} leads")
                    return df
                else:
                    raise Exception(f"Failed to download export file: {response.status}")


class MarketoExtractor:
    """Main Marketo data extraction class"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.load_config()
        self.checkpoint_manager = CheckpointManager()
        self.marketo_client = MarketoAPIClient(self.marketo_config)
        self.kafka_producer = self.create_kafka_producer()
    
    def load_config(self) -> None:
        """Load configuration from YAML file"""
        with open(self.config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Load configurations
        self.marketo_config = MarketoConfig(**config_data['marketo'])
        self.kafka_config = KafkaConfig(
            brokers=config_data['kafka']['brokers'],
            topic=config_data['kafka']['topics']['marketo_leads_delta']['name'],
            sasl_username=os.getenv('KAFKA_USER'),
            sasl_password=os.getenv('KAFKA_PASSWORD')
        )
    
    def create_kafka_producer(self) -> KafkaProducer:
        """Create Kafka producer instance"""
        return KafkaProducer(
            bootstrap_servers=self.kafka_config.brokers,
            security_protocol=self.kafka_config.security_protocol,
            sasl_mechanism=self.kafka_config.sasl_mechanism,
            sasl_plain_username=self.kafka_config.sasl_username,
            sasl_plain_password=self.kafka_config.sasl_password,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            enable_idempotence=True
        )
    
    def create_kafka_event(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create Kafka event from lead data"""
        event_id = str(uuid4())
        ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        return {
            'event_id': event_id,
            'event_type': 'marketo.lead.delta',
            'ts_ms': ts_ms,
            'source': 'marketo',
            'data': lead_data
        }
    
    async def extract_and_publish(self) -> None:
        """Extract leads from Marketo and publish to Kafka"""
        try:
            # Get last checkpoint
            last_checkpoint = self.checkpoint_manager.get_last_checkpoint()
            logger.info(f"Starting extraction from checkpoint: {last_checkpoint}")
            
            # Create export job
            export_id = await self.marketo_client.create_export_job(last_checkpoint)
            
            # Enqueue job
            await self.marketo_client.enqueue_export_job(export_id)
            
            # Wait for completion
            await self.marketo_client.wait_for_completion(export_id)
            
            # Download results
            leads_df = await self.marketo_client.download_export_file(export_id)
            
            if len(leads_df) > 0:
                # Publish to Kafka
                published_count = 0
                current_max_updated_at = last_checkpoint
                
                for _, row in leads_df.iterrows():
                    lead_data = row.to_dict()
                    
                    # Track latest updatedAt for checkpoint
                    if 'updatedAt' in lead_data and lead_data['updatedAt']:
                        if not current_max_updated_at or lead_data['updatedAt'] > current_max_updated_at:
                            current_max_updated_at = lead_data['updatedAt']
                    
                    # Create and send Kafka event
                    event = self.create_kafka_event(lead_data)
                    key = str(lead_data.get('id', ''))
                    
                    self.kafka_producer.send(
                        self.kafka_config.topic,
                        key=key,
                        value=event
                    )
                    published_count += 1
                
                # Flush producer
                self.kafka_producer.flush()
                logger.info(f"Published {published_count} lead events to Kafka")
                
                # Update checkpoint
                if current_max_updated_at and current_max_updated_at != last_checkpoint:
                    self.checkpoint_manager.save_checkpoint(current_max_updated_at)
            else:
                logger.info("No new leads to extract")
                
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
        finally:
            if hasattr(self, 'kafka_producer'):
                self.kafka_producer.close()


async def main():
    """Main execution function"""
    extractor = MarketoExtractor()
    await extractor.extract_and_publish()


if __name__ == "__main__":
    asyncio.run(main())