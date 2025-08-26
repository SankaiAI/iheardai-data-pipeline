#!/usr/bin/env python3
"""
Frontend Events Transformer for iheardAI Data Pipeline

Transforms raw frontend interaction events into standardized analytics format.
"""

import logging
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse, parse_qs
from .base_transformer import BaseTransformer, ValidationError, TransformationError

logger = logging.getLogger(__name__)


class FrontendTransformer(BaseTransformer):
    """Transforms frontend interaction events"""
    
    REQUIRED_FIELDS = ['event_type', 'timestamp']
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw frontend event data"""
        try:
            # Validate required fields
            if not self.validate_required_fields(raw_data, self.REQUIRED_FIELDS):
                raise ValidationError(f"Missing required fields in frontend data: {raw_data}")
            
            # Extract and enhance data
            transformed_data = {
                'session_id': self.normalize_string(raw_data.get('session_id')),
                'user_id': self.normalize_string(raw_data.get('user_id')),
                'event_type': self.normalize_event_type(raw_data['event_type']),
                'interaction_type': self.normalize_string(raw_data.get('interaction_type')),
                'widget_id': self.normalize_string(raw_data.get('widget_id')),
                
                # Page and URL analysis
                'page_info': self.analyze_page_info(raw_data),
                'referrer_info': self.analyze_referrer_info(raw_data),
                
                # Device and browser analysis
                'device_info': self.analyze_device_info(raw_data),
                
                # Timing information
                'timestamp': self.standardize_timestamp(raw_data['timestamp']),
                'timing_info': self.extract_timing_info(raw_data),
                
                # Event-specific data
                'event_data': self.extract_event_specific_data(raw_data),
                
                # Derived analytics fields
                'user_segment': self.determine_user_segment(raw_data),
                'engagement_score': self.calculate_engagement_score(raw_data),
                'conversion_stage': self.determine_conversion_stage(raw_data),
                'quality_score': self.calculate_event_quality_score(raw_data),
                
                # Geographic information (if available)
                'geographic_info': self.extract_geographic_info(raw_data),
                
                'raw_data': raw_data
            }
            
            return self.create_standardized_event(
                event_type='frontend.interaction.transformed',
                source='iheardai_frontend',
                data=transformed_data
            )
            
        except Exception as e:
            logger.error(f"Failed to transform frontend data: {e}, raw_data: {raw_data}")
            raise TransformationError(f"Frontend transformation failed: {e}")
    
    def normalize_event_type(self, event_type: str) -> str:
        """Normalize event type values"""
        if not event_type:
            return 'unknown'
        
        # Standardize common event types
        event_mapping = {
            'click': 'click',
            'page_view': 'page_view',
            'widget_open': 'widget_open',
            'widget_close': 'widget_close',
            'widget_load': 'widget_load',
            'message_sent': 'message_sent',
            'message_received': 'message_received',
            'form_submit': 'form_submit',
            'scroll': 'scroll',
            'hover': 'hover',
            'focus': 'focus',
            'blur': 'blur',
            'error': 'error',
            'performance': 'performance'
        }
        
        normalized = event_type.lower().replace('-', '_')
        return event_mapping.get(normalized, normalized)
    
    def analyze_page_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze page URL and extract useful information"""
        page_url = raw_data.get('page_url', '')
        page_title = raw_data.get('page_title', '')
        
        if not page_url:
            return {
                'url': None,
                'domain': None,
                'path': None,
                'query_params': {},
                'title': self.normalize_string(page_title),
                'category': 'unknown'
            }
        
        try:
            parsed_url = urlparse(page_url)
            query_params = parse_qs(parsed_url.query)
            
            # Flatten query params (take first value)
            flattened_params = {k: v[0] if v else None for k, v in query_params.items()}
            
            return {
                'url': page_url,
                'domain': parsed_url.netloc,
                'path': parsed_url.path,
                'query_params': flattened_params,
                'title': self.normalize_string(page_title),
                'category': self.categorize_page(parsed_url.path),
                'utm_source': flattened_params.get('utm_source'),
                'utm_medium': flattened_params.get('utm_medium'),
                'utm_campaign': flattened_params.get('utm_campaign')
            }
        except Exception as e:
            logger.warning(f"Failed to parse page URL {page_url}: {e}")
            return {
                'url': page_url,
                'domain': None,
                'path': None,
                'query_params': {},
                'title': self.normalize_string(page_title),
                'category': 'unknown'
            }
    
    def categorize_page(self, path: str) -> str:
        """Categorize page based on URL path"""
        if not path or path == '/':
            return 'homepage'
        
        path = path.lower()
        
        # Common page categories
        if any(keyword in path for keyword in ['/product', '/p/']):
            return 'product'
        elif any(keyword in path for keyword in ['/category', '/c/', '/collection']):
            return 'category'
        elif any(keyword in path for keyword in ['/cart', '/checkout']):
            return 'checkout'
        elif any(keyword in path for keyword in ['/about', '/company']):
            return 'about'
        elif any(keyword in path for keyword in ['/contact', '/support']):
            return 'contact'
        elif any(keyword in path for keyword in ['/blog', '/news', '/article']):
            return 'content'
        elif any(keyword in path for keyword in ['/search', '/results']):
            return 'search'
        elif any(keyword in path for keyword in ['/account', '/profile', '/dashboard']):
            return 'account'
        else:
            return 'other'
    
    def analyze_referrer_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze referrer information"""
        referrer = raw_data.get('referrer', '')
        
        if not referrer:
            return {
                'referrer': None,
                'referrer_domain': None,
                'referrer_type': 'direct'
            }
        
        try:
            parsed_referrer = urlparse(referrer)
            domain = parsed_referrer.netloc.lower()
            
            # Categorize referrer type
            referrer_type = self.categorize_referrer(domain)
            
            return {
                'referrer': referrer,
                'referrer_domain': domain,
                'referrer_type': referrer_type
            }
        except Exception as e:
            logger.warning(f"Failed to parse referrer {referrer}: {e}")
            return {
                'referrer': referrer,
                'referrer_domain': None,
                'referrer_type': 'unknown'
            }
    
    def categorize_referrer(self, domain: str) -> str:
        """Categorize referrer domain"""
        if not domain:
            return 'direct'
        
        # Search engines
        search_engines = [
            'google.com', 'bing.com', 'yahoo.com', 'duckduckgo.com',
            'baidu.com', 'yandex.com', 'ask.com'
        ]
        
        # Social media platforms
        social_platforms = [
            'facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com',
            'pinterest.com', 'youtube.com', 'tiktok.com', 'snapchat.com',
            'reddit.com', 'tumblr.com'
        ]
        
        # Email platforms
        email_platforms = [
            'gmail.com', 'outlook.com', 'yahoo.com', 'mail.google.com',
            'webmail', 'mail.'
        ]
        
        if any(search in domain for search in search_engines):
            return 'search'
        elif any(social in domain for social in social_platforms):
            return 'social'
        elif any(email in domain for email in email_platforms):
            return 'email'
        else:
            return 'referral'
    
    def analyze_device_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze device and browser information"""
        user_agent = raw_data.get('user_agent', '')
        
        return {
            'user_agent': user_agent,
            'device_type': self.determine_device_type(user_agent),
            'browser': self.determine_browser(user_agent),
            'operating_system': self.determine_os(user_agent),
            'is_mobile': self.is_mobile_device(user_agent),
            'screen_resolution': raw_data.get('screen_resolution'),
            'viewport_size': raw_data.get('viewport_size')
        }
    
    def determine_device_type(self, user_agent: str) -> str:
        """Determine device type from user agent"""
        if not user_agent:
            return 'unknown'
        
        ua_lower = user_agent.lower()
        
        if any(mobile in ua_lower for mobile in ['mobile', 'iphone', 'ipod', 'android']):
            return 'mobile'
        elif any(tablet in ua_lower for tablet in ['tablet', 'ipad']):
            return 'tablet'
        else:
            return 'desktop'
    
    def determine_browser(self, user_agent: str) -> str:
        """Determine browser from user agent"""
        if not user_agent:
            return 'unknown'
        
        ua_lower = user_agent.lower()
        
        if 'chrome' in ua_lower and 'edg' not in ua_lower:
            return 'chrome'
        elif 'firefox' in ua_lower:
            return 'firefox'
        elif 'safari' in ua_lower and 'chrome' not in ua_lower:
            return 'safari'
        elif 'edg' in ua_lower:
            return 'edge'
        elif 'opera' in ua_lower:
            return 'opera'
        else:
            return 'other'
    
    def determine_os(self, user_agent: str) -> str:
        """Determine operating system from user agent"""
        if not user_agent:
            return 'unknown'
        
        ua_lower = user_agent.lower()
        
        if 'windows' in ua_lower:
            return 'windows'
        elif 'mac os' in ua_lower or 'macos' in ua_lower:
            return 'macos'
        elif 'linux' in ua_lower:
            return 'linux'
        elif 'android' in ua_lower:
            return 'android'
        elif 'ios' in ua_lower or 'iphone' in ua_lower or 'ipad' in ua_lower:
            return 'ios'
        else:
            return 'other'
    
    def is_mobile_device(self, user_agent: str) -> bool:
        """Check if device is mobile"""
        if not user_agent:
            return False
        
        mobile_indicators = ['mobile', 'iphone', 'ipod', 'android', 'blackberry', 'windows phone']
        return any(indicator in user_agent.lower() for indicator in mobile_indicators)
    
    def extract_timing_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract timing-related information"""
        return {
            'load_time_ms': raw_data.get('load_time_ms'),
            'time_on_page': raw_data.get('time_on_page'),
            'time_since_last_event': raw_data.get('time_since_last_event'),
            'session_duration': raw_data.get('session_duration'),
            'page_load_complete': raw_data.get('page_load_complete')
        }
    
    def extract_event_specific_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data specific to the event type"""
        event_type = raw_data.get('event_type', '').lower()
        
        if event_type == 'click':
            return {
                'element_id': raw_data.get('element_id'),
                'element_class': raw_data.get('element_class'),
                'element_text': raw_data.get('element_text'),
                'click_coordinates': raw_data.get('click_coordinates')
            }
        elif event_type == 'scroll':
            return {
                'scroll_depth': raw_data.get('scroll_depth'),
                'scroll_direction': raw_data.get('scroll_direction'),
                'max_scroll_depth': raw_data.get('max_scroll_depth')
            }
        elif event_type == 'form_submit':
            return {
                'form_id': raw_data.get('form_id'),
                'form_fields': raw_data.get('form_fields'),
                'form_completion_time': raw_data.get('form_completion_time')
            }
        elif 'widget' in event_type:
            return {
                'widget_position': raw_data.get('widget_position'),
                'widget_size': raw_data.get('widget_size'),
                'widget_config': raw_data.get('widget_config')
            }
        else:
            return raw_data.get('event_data', {})
    
    def determine_user_segment(self, raw_data: Dict[str, Any]) -> str:
        """Determine user segment based on behavior"""
        # Simple segmentation logic - can be enhanced with ML
        page_url = raw_data.get('page_url', '')
        event_type = raw_data.get('event_type', '')
        referrer = raw_data.get('referrer', '')
        
        if 'checkout' in page_url.lower() or event_type == 'purchase':
            return 'buyer'
        elif event_type in ['widget_open', 'message_sent']:
            return 'engaged'
        elif 'product' in page_url.lower():
            return 'browser'
        elif not referrer:
            return 'direct'
        else:
            return 'visitor'
    
    def calculate_engagement_score(self, raw_data: Dict[str, Any]) -> int:
        """Calculate engagement score for the event"""
        score = 0
        event_type = raw_data.get('event_type', '')
        
        # Base score by event type
        event_scores = {
            'purchase': 100,
            'form_submit': 80,
            'widget_open': 60,
            'message_sent': 70,
            'click': 20,
            'scroll': 10,
            'page_view': 15
        }
        
        score += event_scores.get(event_type, 5)
        
        # Time-based bonuses
        time_on_page = raw_data.get('time_on_page', 0)
        if time_on_page > 300:  # 5 minutes
            score += 20
        elif time_on_page > 120:  # 2 minutes
            score += 10
        elif time_on_page > 30:  # 30 seconds
            score += 5
        
        return min(score, 100)
    
    def determine_conversion_stage(self, raw_data: Dict[str, Any]) -> str:
        """Determine conversion funnel stage"""
        event_type = raw_data.get('event_type', '')
        page_url = raw_data.get('page_url', '').lower()
        
        if event_type == 'purchase' or 'thank-you' in page_url:
            return 'conversion'
        elif 'checkout' in page_url or 'cart' in page_url:
            return 'purchase_intent'
        elif event_type == 'widget_open' or 'contact' in page_url:
            return 'consideration'
        elif 'product' in page_url:
            return 'interest'
        else:
            return 'awareness'
    
    def calculate_event_quality_score(self, raw_data: Dict[str, Any]) -> int:
        """Calculate quality score for the event data"""
        score = 100
        
        # Deduct points for missing important fields
        important_fields = ['session_id', 'user_agent', 'page_url', 'timestamp']
        for field in important_fields:
            if not raw_data.get(field):
                score -= 10
        
        # Deduct points for suspicious data
        if raw_data.get('user_agent') == 'bot' or 'bot' in raw_data.get('user_agent', '').lower():
            score -= 50
        
        return max(score, 0)
    
    def extract_geographic_info(self, raw_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """Extract geographic information if available"""
        return {
            'country': raw_data.get('country'),
            'region': raw_data.get('region'),
            'city': raw_data.get('city'),
            'timezone': raw_data.get('timezone'),
            'ip_address': self.redact_pii(str(raw_data.get('ip_address', ''))) if raw_data.get('ip_address') else None
        }