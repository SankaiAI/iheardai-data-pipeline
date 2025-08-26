#!/usr/bin/env python3
"""
Marketo Data Transformer for iheardAI Data Pipeline

Transforms raw Marketo lead data into standardized format for analytics.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from .base_transformer import BaseTransformer, ValidationError, TransformationError

logger = logging.getLogger(__name__)


class MarketoTransformer(BaseTransformer):
    """Transforms Marketo lead data"""
    
    REQUIRED_FIELDS = ['id', 'email']
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw Marketo lead data"""
        try:
            # Validate required fields
            if not self.validate_required_fields(raw_data, self.REQUIRED_FIELDS):
                raise ValidationError(f"Missing required fields in Marketo data: {raw_data}")
            
            # Extract and clean data
            transformed_data = {
                'lead_id': int(raw_data['id']),
                'email': self.normalize_email(raw_data['email']),
                'first_name': self.normalize_string(raw_data.get('firstName')),
                'last_name': self.normalize_string(raw_data.get('lastName')),
                'company': self.normalize_string(raw_data.get('company')),
                'title': self.normalize_string(raw_data.get('title')),
                'phone': self.normalize_phone(raw_data.get('phone')),
                'lead_source': self.normalize_lead_source(raw_data.get('leadSource')),
                'original_source_type': self.normalize_string(raw_data.get('originalSourceType')),
                'lead_status': self.normalize_string(raw_data.get('leadStatus')),
                'created_at': self.standardize_timestamp(raw_data.get('createdAt')),
                'updated_at': self.standardize_timestamp(raw_data.get('updatedAt')),
                
                # Derived fields
                'full_name': self.create_full_name(raw_data),
                'email_domain': self.extract_email_domain(raw_data.get('email')),
                'lead_quality_score': self.calculate_lead_score(raw_data),
                'geographic_info': self.extract_geographic_info(raw_data),
                
                # Original raw data (for debugging)
                'raw_data': raw_data
            }
            
            # Create standardized event
            return self.create_standardized_event(
                event_type='marketo.lead.transformed',
                source='marketo',
                data=transformed_data
            )
            
        except Exception as e:
            logger.error(f"Failed to transform Marketo data: {e}, raw_data: {raw_data}")
            raise TransformationError(f"Marketo transformation failed: {e}")
    
    def normalize_email(self, email: str) -> Optional[str]:
        """Normalize and validate email address"""
        if not email:
            return None
        
        email = email.strip().lower()
        
        # Basic email validation
        import re
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            logger.warning(f"Invalid email format: {email}")
            return None
        
        return email
    
    def normalize_phone(self, phone: str) -> Optional[str]:
        """Normalize phone number format"""
        if not phone:
            return None
        
        # Remove all non-digit characters
        import re
        digits_only = re.sub(r'\D', '', phone)
        
        # Handle different formats
        if len(digits_only) == 10:
            # US format: (XXX) XXX-XXXX
            return f"({digits_only[:3]}) {digits_only[3:6]}-{digits_only[6:]}"
        elif len(digits_only) == 11 and digits_only.startswith('1'):
            # US format with country code
            return f"1-({digits_only[1:4]}) {digits_only[4:7]}-{digits_only[7:]}"
        else:
            # Keep original for international numbers
            return phone.strip()
    
    def normalize_lead_source(self, lead_source: str) -> Optional[str]:
        """Normalize lead source values"""
        if not lead_source:
            return None
        
        # Standardize common lead sources
        source_mapping = {
            'web': 'Website',
            'website': 'Website',
            'organic search': 'Organic Search',
            'paid search': 'Paid Search',
            'social media': 'Social Media',
            'email': 'Email Marketing',
            'webinar': 'Webinar',
            'trade show': 'Trade Show',
            'referral': 'Referral',
            'direct mail': 'Direct Mail'
        }
        
        normalized_source = lead_source.strip().lower()
        return source_mapping.get(normalized_source, lead_source.title())
    
    def create_full_name(self, raw_data: Dict[str, Any]) -> Optional[str]:
        """Create full name from first and last name"""
        first_name = self.normalize_string(raw_data.get('firstName'))
        last_name = self.normalize_string(raw_data.get('lastName'))
        
        if first_name and last_name:
            return f"{first_name} {last_name}"
        elif first_name:
            return first_name
        elif last_name:
            return last_name
        else:
            return None
    
    def extract_email_domain(self, email: str) -> Optional[str]:
        """Extract domain from email address"""
        if not email or '@' not in email:
            return None
        
        return email.split('@')[1].lower()
    
    def calculate_lead_score(self, raw_data: Dict[str, Any]) -> int:
        """Calculate basic lead quality score"""
        score = 0
        
        # Email domain scoring
        email = raw_data.get('email', '')
        if email:
            domain = self.extract_email_domain(email)
            if domain:
                # Business domains get higher scores
                business_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
                if domain not in business_domains:
                    score += 20
                else:
                    score += 5
        
        # Completeness scoring
        fields_to_check = ['firstName', 'lastName', 'company', 'title', 'phone']
        for field in fields_to_check:
            if raw_data.get(field):
                score += 10
        
        # Lead source scoring
        lead_source = raw_data.get('leadSource', '').lower()
        source_scores = {
            'referral': 25,
            'webinar': 20,
            'trade show': 20,
            'organic search': 15,
            'website': 15,
            'paid search': 10,
            'social media': 10,
            'email': 5
        }
        score += source_scores.get(lead_source, 0)
        
        # Recent activity bonus
        updated_at = raw_data.get('updatedAt')
        if updated_at:
            try:
                update_time = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                days_since_update = (datetime.now(timezone.utc) - update_time).days
                
                if days_since_update <= 1:
                    score += 10
                elif days_since_update <= 7:
                    score += 5
            except:
                pass
        
        return min(score, 100)  # Cap at 100
    
    def extract_geographic_info(self, raw_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """Extract and normalize geographic information"""
        return {
            'country': self.normalize_string(raw_data.get('country')),
            'state': self.normalize_string(raw_data.get('state')),
            'city': self.normalize_string(raw_data.get('city')),
            'postal_code': self.normalize_string(raw_data.get('postalCode')),
            'timezone': self.normalize_string(raw_data.get('timezone'))
        }


class MarketoActivityTransformer(BaseTransformer):
    """Transforms Marketo activity data"""
    
    REQUIRED_FIELDS = ['id', 'leadId', 'activityTypeId', 'activityDate']
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw Marketo activity data"""
        try:
            if not self.validate_required_fields(raw_data, self.REQUIRED_FIELDS):
                raise ValidationError(f"Missing required fields in Marketo activity: {raw_data}")
            
            transformed_data = {
                'activity_id': int(raw_data['id']),
                'lead_id': int(raw_data['leadId']),
                'activity_type_id': int(raw_data['activityTypeId']),
                'activity_type': self.get_activity_type_name(raw_data['activityTypeId']),
                'activity_date': self.standardize_timestamp(raw_data['activityDate']),
                'primary_attribute_value': self.normalize_string(raw_data.get('primaryAttributeValue')),
                'attributes': self.extract_activity_attributes(raw_data.get('attributes', [])),
                
                # Derived fields
                'engagement_score': self.calculate_activity_engagement_score(raw_data),
                'activity_category': self.categorize_activity(raw_data['activityTypeId']),
                
                'raw_data': raw_data
            }
            
            return self.create_standardized_event(
                event_type='marketo.activity.transformed',
                source='marketo',
                data=transformed_data
            )
            
        except Exception as e:
            logger.error(f"Failed to transform Marketo activity: {e}")
            raise TransformationError(f"Marketo activity transformation failed: {e}")
    
    def get_activity_type_name(self, activity_type_id: int) -> str:
        """Map activity type ID to human-readable name"""
        activity_types = {
            1: 'Visit Webpage',
            2: 'Fill Out Form',
            3: 'Click Link',
            6: 'Send Email',
            7: 'Email Delivered',
            8: 'Email Bounced',
            9: 'Unsubscribe Email',
            10: 'Open Email',
            11: 'Click Email',
            12: 'New Lead',
            13: 'Change Data Value',
            22: 'Interesting Moment',
            24: 'Request Campaign',
            25: 'Send Alert',
            104: 'Download Content',
            110: 'Visit Booth',
            113: 'Attend Event'
        }
        
        return activity_types.get(activity_type_id, f'Unknown Activity ({activity_type_id})')
    
    def extract_activity_attributes(self, attributes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract and normalize activity attributes"""
        normalized_attributes = {}
        
        for attr in attributes:
            if 'name' in attr and 'value' in attr:
                key = attr['name'].lower().replace(' ', '_')
                normalized_attributes[key] = self.normalize_string(attr['value'])
        
        return normalized_attributes
    
    def calculate_activity_engagement_score(self, raw_data: Dict[str, Any]) -> int:
        """Calculate engagement score for activity"""
        activity_type_id = raw_data.get('activityTypeId', 0)
        
        # Score based on activity type importance
        engagement_scores = {
            2: 50,   # Fill Out Form
            104: 40, # Download Content  
            22: 35,  # Interesting Moment
            110: 30, # Visit Booth
            113: 30, # Attend Event
            10: 20,  # Open Email
            11: 25,  # Click Email
            3: 15,   # Click Link
            1: 10,   # Visit Webpage
            6: 5,    # Send Email
            7: 5,    # Email Delivered
        }
        
        return engagement_scores.get(activity_type_id, 5)
    
    def categorize_activity(self, activity_type_id: int) -> str:
        """Categorize activity into broad types"""
        categories = {
            'email': [6, 7, 8, 9, 10, 11],      # Email activities
            'web': [1, 3],                       # Web activities  
            'form': [2],                         # Form fills
            'content': [104],                    # Content downloads
            'event': [110, 113],                 # Event activities
            'campaign': [24, 25],                # Campaign activities
            'data': [12, 13],                    # Data changes
            'engagement': [22]                   # Engagement moments
        }
        
        for category, type_ids in categories.items():
            if activity_type_id in type_ids:
                return category
        
        return 'other'