#!/usr/bin/env python3
"""
Base Transformer for iheardAI Data Pipeline

Provides common transformation patterns and utilities for all data types.
"""

import json
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Union
from uuid import uuid4

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """Base class for all data transformers"""
    
    def __init__(self):
        self.transformation_metadata = {
            'transformer': self.__class__.__name__,
            'version': '1.0.0',
            'applied_at': datetime.now(timezone.utc).isoformat()
        }
    
    @abstractmethod
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standardized format"""
        pass
    
    def validate_required_fields(self, data: Dict[str, Any], required_fields: List[str]) -> bool:
        """Validate that all required fields are present"""
        missing_fields = [field for field in required_fields if field not in data or data[field] is None]
        
        if missing_fields:
            logger.warning(f"Missing required fields: {missing_fields}")
            return False
        return True
    
    def standardize_timestamp(self, timestamp_field: Any) -> Optional[int]:
        """Convert various timestamp formats to UTC milliseconds"""
        if not timestamp_field:
            return None
            
        try:
            if isinstance(timestamp_field, (int, float)):
                # Assume milliseconds if > 1000000000000, else seconds
                if timestamp_field > 1000000000000:
                    return int(timestamp_field)
                else:
                    return int(timestamp_field * 1000)
            
            if isinstance(timestamp_field, str):
                # Parse ISO format or other common formats
                dt = datetime.fromisoformat(timestamp_field.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
                
        except Exception as e:
            logger.error(f"Failed to parse timestamp {timestamp_field}: {e}")
            return int(datetime.now(timezone.utc).timestamp() * 1000)
    
    def redact_pii(self, text: str) -> str:
        """Apply PII redaction to text content"""
        if not text:
            return text
            
        # Email addresses
        text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL_REDACTED]', text)
        
        # Phone numbers (basic patterns)
        text = re.sub(r'\b\d{3}-\d{3}-\d{4}\b', '[PHONE_REDACTED]', text)
        text = re.sub(r'\b\(\d{3}\)\s*\d{3}-\d{4}\b', '[PHONE_REDACTED]', text)
        
        # Credit card numbers (basic pattern)
        text = re.sub(r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b', '[CARD_REDACTED]', text)
        
        # Social Security Numbers (US format)
        text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[SSN_REDACTED]', text)
        
        return text
    
    def normalize_string(self, value: Any) -> Optional[str]:
        """Normalize string values (trim, handle None, etc.)"""
        if value is None:
            return None
        
        if not isinstance(value, str):
            value = str(value)
        
        # Trim whitespace and normalize empty strings
        normalized = value.strip()
        return normalized if normalized else None
    
    def generate_event_id(self) -> str:
        """Generate unique event ID"""
        return str(uuid4())
    
    def add_transformation_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add metadata about the transformation process"""
        if 'metadata' not in data:
            data['metadata'] = {}
        
        data['metadata']['transformation'] = self.transformation_metadata
        return data
    
    def create_standardized_event(self, 
                                event_type: str, 
                                source: str,
                                data: Dict[str, Any],
                                event_id: Optional[str] = None,
                                timestamp: Optional[int] = None) -> Dict[str, Any]:
        """Create standardized event structure"""
        
        return {
            'event_id': event_id or self.generate_event_id(),
            'event_type': event_type,
            'source': source,
            'ts_ms': timestamp or int(datetime.now(timezone.utc).timestamp() * 1000),
            'data': data,
            'metadata': self.transformation_metadata.copy()
        }


class ValidationError(Exception):
    """Custom exception for validation failures"""
    pass


class TransformationError(Exception):
    """Custom exception for transformation failures"""
    pass