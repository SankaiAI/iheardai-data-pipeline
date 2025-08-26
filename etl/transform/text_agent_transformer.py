#!/usr/bin/env python3
"""
Text Agent Events Transformer for iheardAI Data Pipeline

Transforms raw text agent interaction events into standardized analytics format.
"""

import logging
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from .base_transformer import BaseTransformer, ValidationError, TransformationError

logger = logging.getLogger(__name__)


class TextAgentTransformer(BaseTransformer):
    """Transforms text agent conversation events"""
    
    REQUIRED_FIELDS = ['session_id', 'turn_id', 'model', 'tokens_in', 'tokens_out']
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw text agent event data"""
        try:
            # Validate required fields
            if not self.validate_required_fields(raw_data, self.REQUIRED_FIELDS):
                raise ValidationError(f"Missing required fields in text agent data: {raw_data}")
            
            # Extract and enhance data
            transformed_data = {
                'session_id': self.normalize_string(raw_data['session_id']),
                'turn_id': self.normalize_string(raw_data['turn_id']),
                'user_id': self.normalize_string(raw_data.get('user_id')),
                'channel': self.normalize_string(raw_data.get('channel', 'text')),
                
                # Model and performance data
                'model_info': self.analyze_model_info(raw_data),
                'performance_metrics': self.extract_performance_metrics(raw_data),
                
                # Content analysis
                'content_analysis': self.analyze_conversation_content(raw_data),
                
                # Timing information
                'timestamp': self.standardize_timestamp(raw_data.get('timestamp', raw_data.get('ts_ms'))),
                'timing_metrics': self.extract_timing_metrics(raw_data),
                
                # Tool usage analysis
                'tool_usage': self.analyze_tool_usage(raw_data),
                
                # Quality metrics
                'quality_metrics': self.calculate_quality_metrics(raw_data),
                
                # Business value metrics
                'business_metrics': self.calculate_business_metrics(raw_data),
                
                'raw_data': raw_data
            }
            
            return self.create_standardized_event(
                event_type='text_agent.turn.transformed',
                source='text_agent_server',
                data=transformed_data
            )
            
        except Exception as e:
            logger.error(f"Failed to transform text agent data: {e}, raw_data: {raw_data}")
            raise TransformationError(f"Text agent transformation failed: {e}")
    
    def analyze_model_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze model information and capabilities"""
        model = raw_data.get('model', 'unknown')
        
        # Extract model details
        model_family = self.extract_model_family(model)
        model_size = self.estimate_model_size(model)
        model_capabilities = self.analyze_model_capabilities(model)
        
        return {
            'model_name': model,
            'model_family': model_family,
            'estimated_size': model_size,
            'capabilities': model_capabilities,
            'model_version': self.extract_model_version(model)
        }
    
    def extract_model_family(self, model: str) -> str:
        """Extract model family (GPT, Claude, etc.)"""
        model_lower = model.lower()
        
        if 'gpt' in model_lower:
            return 'gpt'
        elif 'claude' in model_lower:
            return 'claude'
        elif 'llama' in model_lower:
            return 'llama'
        elif 'palm' in model_lower:
            return 'palm'
        elif 'gemini' in model_lower:
            return 'gemini'
        else:
            return 'other'
    
    def estimate_model_size(self, model: str) -> str:
        """Estimate model size category"""
        model_lower = model.lower()
        
        if any(size in model_lower for size in ['large', 'xl', '70b', '175b']):
            return 'large'
        elif any(size in model_lower for size in ['medium', 'base', '13b', '30b']):
            return 'medium'
        elif any(size in model_lower for size in ['small', 'mini', '7b']):
            return 'small'
        else:
            return 'unknown'
    
    def analyze_model_capabilities(self, model: str) -> List[str]:
        """Analyze model capabilities"""
        model_lower = model.lower()
        capabilities = []
        
        if 'instruct' in model_lower or 'chat' in model_lower:
            capabilities.append('instruction_following')
        if 'code' in model_lower:
            capabilities.append('code_generation')
        if 'vision' in model_lower:
            capabilities.append('vision')
        if 'tool' in model_lower or 'function' in model_lower:
            capabilities.append('tool_use')
        
        return capabilities or ['text_generation']
    
    def extract_model_version(self, model: str) -> Optional[str]:
        """Extract model version if available"""
        import re
        version_match = re.search(r'v?\d+(\.\d+)*', model)
        return version_match.group() if version_match else None
    
    def extract_performance_metrics(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract performance-related metrics"""
        tokens_in = int(raw_data.get('tokens_in', 0))
        tokens_out = int(raw_data.get('tokens_out', 0))
        latency_ms = float(raw_data.get('latency_ms', 0))
        
        # Calculate derived metrics
        total_tokens = tokens_in + tokens_out
        tokens_per_second = tokens_out / (latency_ms / 1000) if latency_ms > 0 else 0
        compression_ratio = tokens_out / tokens_in if tokens_in > 0 else 0
        
        return {
            'tokens_in': tokens_in,
            'tokens_out': tokens_out,
            'total_tokens': total_tokens,
            'latency_ms': latency_ms,
            'tokens_per_second': round(tokens_per_second, 2),
            'compression_ratio': round(compression_ratio, 2),
            'efficiency_score': self.calculate_efficiency_score(tokens_out, latency_ms)
        }
    
    def calculate_efficiency_score(self, tokens_out: int, latency_ms: float) -> int:
        """Calculate efficiency score (0-100)"""
        if latency_ms <= 0:
            return 0
        
        # Base score on tokens per second
        tokens_per_second = tokens_out / (latency_ms / 1000)
        
        # Score ranges (adjust based on your standards)
        if tokens_per_second > 50:
            return 100
        elif tokens_per_second > 25:
            return 80
        elif tokens_per_second > 10:
            return 60
        elif tokens_per_second > 5:
            return 40
        elif tokens_per_second > 1:
            return 20
        else:
            return 10
    
    def analyze_conversation_content(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze conversation content for insights"""
        response_text = raw_data.get('response_text', '')
        
        # Apply PII redaction
        redacted_text = self.redact_pii(response_text) if response_text else ''
        
        return {
            'response_length': len(response_text) if response_text else 0,
            'word_count': len(response_text.split()) if response_text else 0,
            'sentence_count': response_text.count('.') + response_text.count('!') + response_text.count('?') if response_text else 0,
            'has_code_blocks': '```' in response_text if response_text else False,
            'has_links': 'http' in response_text if response_text else False,
            'language': self.detect_response_language(response_text),
            'sentiment': self.analyze_sentiment(response_text),
            'topics': self.extract_topics(response_text),
            'redacted_preview': redacted_text[:200] if redacted_text else None
        }
    
    def detect_response_language(self, text: str) -> str:
        """Simple language detection"""
        if not text:
            return 'unknown'
        
        # Very basic detection - could be enhanced with a proper library
        english_indicators = ['the', 'and', 'is', 'to', 'in', 'it', 'you', 'that', 'he', 'was']
        spanish_indicators = ['el', 'la', 'de', 'que', 'y', 'es', 'en', 'un', 'se', 'no']
        french_indicators = ['le', 'de', 'et', 'à', 'un', 'il', 'être', 'et', 'en', 'avoir']
        
        text_lower = text.lower()
        words = text_lower.split()[:50]  # Check first 50 words
        
        english_count = sum(1 for word in words if word in english_indicators)
        spanish_count = sum(1 for word in words if word in spanish_indicators)
        french_count = sum(1 for word in words if word in french_indicators)
        
        if english_count >= spanish_count and english_count >= french_count:
            return 'english'
        elif spanish_count > french_count:
            return 'spanish'
        elif french_count > 0:
            return 'french'
        else:
            return 'unknown'
    
    def analyze_sentiment(self, text: str) -> str:
        """Simple sentiment analysis"""
        if not text:
            return 'neutral'
        
        positive_words = ['good', 'great', 'excellent', 'amazing', 'wonderful', 'perfect', 'love', 'like', 'happy', 'pleased']
        negative_words = ['bad', 'terrible', 'awful', 'horrible', 'hate', 'dislike', 'angry', 'frustrated', 'disappointed', 'wrong']
        
        text_lower = text.lower()
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        if positive_count > negative_count:
            return 'positive'
        elif negative_count > positive_count:
            return 'negative'
        else:
            return 'neutral'
    
    def extract_topics(self, text: str) -> List[str]:
        """Extract topics from response text"""
        if not text:
            return []
        
        # Simple keyword-based topic extraction
        topics = []
        text_lower = text.lower()
        
        topic_keywords = {
            'product_info': ['product', 'item', 'specification', 'feature', 'price', 'cost'],
            'support': ['help', 'support', 'issue', 'problem', 'trouble', 'error'],
            'shipping': ['shipping', 'delivery', 'ship', 'arrive', 'tracking'],
            'payment': ['payment', 'pay', 'card', 'billing', 'charge', 'refund'],
            'return': ['return', 'exchange', 'refund', 'warranty', 'guarantee'],
            'recommendation': ['recommend', 'suggest', 'best', 'should', 'consider']
        }
        
        for topic, keywords in topic_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                topics.append(topic)
        
        return topics or ['general']
    
    def extract_timing_metrics(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract timing-related metrics"""
        return {
            'processing_time_ms': raw_data.get('processing_time_ms'),
            'queue_time_ms': raw_data.get('queue_time_ms'),
            'total_response_time_ms': raw_data.get('latency_ms'),
            'time_to_first_token_ms': raw_data.get('time_to_first_token_ms'),
            'time_between_tokens_ms': raw_data.get('time_between_tokens_ms')
        }
    
    def analyze_tool_usage(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze tool usage patterns"""
        tools_used = raw_data.get('tools_used', [])
        tool_results = raw_data.get('tool_results', {})
        
        return {
            'tools_count': len(tools_used) if tools_used else 0,
            'tools_used': tools_used or [],
            'tool_success_rate': self.calculate_tool_success_rate(tool_results),
            'tool_categories': self.categorize_tools(tools_used),
            'tool_execution_time_ms': raw_data.get('tool_execution_time_ms')
        }
    
    def calculate_tool_success_rate(self, tool_results: Dict[str, Any]) -> float:
        """Calculate tool success rate"""
        if not tool_results:
            return 1.0
        
        total_tools = len(tool_results)
        successful_tools = sum(1 for result in tool_results.values() if result.get('success', True))
        
        return successful_tools / total_tools if total_tools > 0 else 1.0
    
    def categorize_tools(self, tools_used: List[str]) -> List[str]:
        """Categorize tools by type"""
        if not tools_used:
            return []
        
        categories = []
        
        for tool in tools_used:
            tool_lower = tool.lower()
            
            if any(search in tool_lower for search in ['search', 'find', 'lookup']):
                categories.append('search')
            elif any(calc in tool_lower for calc in ['calc', 'math', 'compute']):
                categories.append('calculation')
            elif any(data in tool_lower for data in ['data', 'database', 'query']):
                categories.append('data_access')
            elif any(api in tool_lower for api in ['api', 'service', 'request']):
                categories.append('api_call')
            else:
                categories.append('other')
        
        return list(set(categories))
    
    def calculate_quality_metrics(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate conversation quality metrics"""
        response_text = raw_data.get('response_text', '')
        tokens_out = int(raw_data.get('tokens_out', 0))
        latency_ms = float(raw_data.get('latency_ms', 0))
        
        # Quality indicators
        completeness_score = self.calculate_completeness_score(response_text)
        relevance_score = self.calculate_relevance_score(raw_data)
        helpfulness_score = self.calculate_helpfulness_score(response_text)
        
        return {
            'completeness_score': completeness_score,
            'relevance_score': relevance_score,
            'helpfulness_score': helpfulness_score,
            'overall_quality_score': round((completeness_score + relevance_score + helpfulness_score) / 3, 1),
            'response_appropriateness': self.assess_response_appropriateness(response_text, tokens_out)
        }
    
    def calculate_completeness_score(self, response_text: str) -> float:
        """Calculate how complete the response appears"""
        if not response_text:
            return 0.0
        
        score = 50.0  # Base score
        
        # Length indicators
        if len(response_text) > 100:
            score += 20
        if len(response_text) > 500:
            score += 10
        
        # Structure indicators
        if '.' in response_text:  # Complete sentences
            score += 10
        if response_text.count('\n') > 0:  # Structured response
            score += 5
        if any(word in response_text.lower() for word in ['however', 'additionally', 'furthermore']):
            score += 5
        
        return min(score, 100.0)
    
    def calculate_relevance_score(self, raw_data: Dict[str, Any]) -> float:
        """Calculate response relevance (simplified)"""
        # This would need more context about the user's question
        # For now, return a baseline score
        response_text = raw_data.get('response_text', '')
        
        if not response_text:
            return 0.0
        
        # Simple heuristics
        score = 70.0  # Baseline assumption of relevance
        
        # Adjust based on response characteristics
        if 'sorry' in response_text.lower() and 'help' in response_text.lower():
            score -= 20  # Possible inability to help
        if any(word in response_text.lower() for word in ['specifically', 'exactly', 'precisely']):
            score += 10  # Specific answers
        
        return min(score, 100.0)
    
    def calculate_helpfulness_score(self, response_text: str) -> float:
        """Calculate how helpful the response appears"""
        if not response_text:
            return 0.0
        
        score = 60.0  # Base score
        
        helpful_indicators = [
            'here', 'steps', 'how to', 'you can', 'try', 'recommend', 
            'suggest', 'help', 'solution', 'answer'
        ]
        
        text_lower = response_text.lower()
        helpful_count = sum(1 for indicator in helpful_indicators if indicator in text_lower)
        
        score += min(helpful_count * 5, 30)  # Up to 30 bonus points
        
        # Deduct for unhelpful patterns
        if 'cannot' in text_lower or "can't" in text_lower:
            score -= 15
        if 'sorry' in text_lower:
            score -= 5
        
        return max(min(score, 100.0), 0.0)
    
    def assess_response_appropriateness(self, response_text: str, tokens_out: int) -> str:
        """Assess if response length is appropriate"""
        if not response_text:
            return 'empty'
        
        if tokens_out < 10:
            return 'too_short'
        elif tokens_out > 1000:
            return 'very_long'
        elif tokens_out > 500:
            return 'long'
        elif tokens_out > 100:
            return 'appropriate'
        else:
            return 'concise'
    
    def calculate_business_metrics(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate business value metrics"""
        tokens_in = int(raw_data.get('tokens_in', 0))
        tokens_out = int(raw_data.get('tokens_out', 0))
        response_text = raw_data.get('response_text', '')
        
        # Cost estimation (approximate)
        estimated_cost_usd = self.estimate_cost(tokens_in, tokens_out, raw_data.get('model', ''))
        
        # Value indicators
        conversion_indicators = self.detect_conversion_indicators(response_text)
        support_resolution = self.detect_support_resolution(response_text)
        
        return {
            'estimated_cost_usd': estimated_cost_usd,
            'cost_per_token': estimated_cost_usd / (tokens_in + tokens_out) if (tokens_in + tokens_out) > 0 else 0,
            'has_conversion_indicators': len(conversion_indicators) > 0,
            'conversion_indicators': conversion_indicators,
            'support_resolution_type': support_resolution,
            'business_value_score': self.calculate_business_value_score(raw_data)
        }
    
    def estimate_cost(self, tokens_in: int, tokens_out: int, model: str) -> float:
        """Estimate API cost (approximate rates)"""
        # Rough cost estimates per 1K tokens (adjust based on actual pricing)
        cost_per_1k_tokens = {
            'gpt-4': {'input': 0.03, 'output': 0.06},
            'gpt-3.5': {'input': 0.001, 'output': 0.002},
            'claude': {'input': 0.008, 'output': 0.024},
            'default': {'input': 0.01, 'output': 0.02}
        }
        
        model_family = self.extract_model_family(model)
        rates = cost_per_1k_tokens.get(model_family, cost_per_1k_tokens['default'])
        
        input_cost = (tokens_in / 1000) * rates['input']
        output_cost = (tokens_out / 1000) * rates['output']
        
        return round(input_cost + output_cost, 6)
    
    def detect_conversion_indicators(self, response_text: str) -> List[str]:
        """Detect indicators of potential conversions"""
        if not response_text:
            return []
        
        indicators = []
        text_lower = response_text.lower()
        
        conversion_patterns = {
            'purchase_intent': ['buy', 'purchase', 'order', 'cart', 'checkout'],
            'contact_request': ['contact', 'call', 'email', 'speak', 'talk'],
            'demo_request': ['demo', 'trial', 'preview', 'show'],
            'information_request': ['more info', 'details', 'specifications', 'pricing']
        }
        
        for pattern_type, keywords in conversion_patterns.items():
            if any(keyword in text_lower for keyword in keywords):
                indicators.append(pattern_type)
        
        return indicators
    
    def detect_support_resolution(self, response_text: str) -> str:
        """Detect type of support resolution"""
        if not response_text:
            return 'unknown'
        
        text_lower = response_text.lower()
        
        if any(word in text_lower for word in ['solved', 'fixed', 'resolved', 'working']):
            return 'resolved'
        elif any(word in text_lower for word in ['try', 'attempt', 'check']):
            return 'troubleshooting'
        elif any(word in text_lower for word in ['contact', 'escalate', 'specialist']):
            return 'escalation'
        elif any(word in text_lower for word in ['sorry', 'cannot', 'unable']):
            return 'unresolved'
        else:
            return 'informational'
    
    def calculate_business_value_score(self, raw_data: Dict[str, Any]) -> int:
        """Calculate business value score (0-100)"""
        score = 50  # Base score
        
        response_text = raw_data.get('response_text', '')
        tokens_out = int(raw_data.get('tokens_out', 0))
        
        # Positive indicators
        if self.detect_conversion_indicators(response_text):
            score += 30
        if tokens_out > 100:  # Substantial response
            score += 10
        if 'recommend' in response_text.lower():
            score += 15
        
        # Negative indicators
        if 'sorry' in response_text.lower():
            score -= 10
        if tokens_out < 20:  # Very short response
            score -= 15
        
        return max(min(score, 100), 0)