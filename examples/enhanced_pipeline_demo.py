#!/usr/bin/env python3
"""
Enhanced Pipeline Demonstration

Shows how the transform layer improves data quality and provides
better analytics compared to the original ELT approach.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any

# Import our enhanced components
from etl.transform import get_transformer, ValidationError, TransformationError
from etl.load.enhanced_kpi_consumer import EnhancedKPIConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PipelineDemo:
    """Demonstration of enhanced pipeline capabilities"""
    
    def __init__(self):
        self.transformers = {
            'marketo': get_transformer('marketo'),
            'frontend': get_transformer('frontend'),
            'text_agent': get_transformer('text_agent')
        }
    
    def create_sample_marketo_data(self) -> Dict[str, Any]:
        """Create sample raw Marketo lead data"""
        return {
            'id': 12345,
            'email': 'john.doe@acme-corp.com',
            'firstName': 'John',
            'lastName': 'Doe',
            'company': 'ACME Corporation',
            'title': 'VP of Engineering',
            'phone': '1-555-123-4567',
            'leadSource': 'Website',
            'originalSourceType': 'Organic Search',
            'leadStatus': 'New',
            'createdAt': '2024-01-15T10:30:00Z',
            'updatedAt': '2024-01-15T14:22:00Z',
            'country': 'United States',
            'state': 'California',
            'city': 'San Francisco',
            'postalCode': '94105'
        }
    
    def create_sample_frontend_data(self) -> Dict[str, Any]:
        """Create sample raw frontend interaction data"""
        return {
            'event_type': 'widget_open',
            'timestamp': 1705320000000,  # milliseconds
            'session_id': 'sess_abc123def456',
            'user_id': 'user_789xyz',
            'interaction_type': 'click',
            'widget_id': 'chat_widget_v2',
            'page_url': 'https://acme-corp.com/products/enterprise-solution',
            'page_title': 'Enterprise Solutions | ACME Corp',
            'referrer': 'https://google.com/search?q=enterprise+crm',
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0',
            'screen_resolution': '1920x1080',
            'viewport_size': '1200x800',
            'time_on_page': 45,  # seconds
            'element_id': 'chat-button',
            'element_class': 'widget-trigger'
        }
    
    def create_sample_text_agent_data(self) -> Dict[str, Any]:
        """Create sample raw text agent turn data"""
        return {
            'session_id': 'sess_abc123def456',
            'turn_id': 'turn_001',
            'user_id': 'user_789xyz',
            'channel': 'web_chat',
            'timestamp': 1705320060000,  # milliseconds
            'model_name': 'claude-3-sonnet',
            'tokens_in': 150,
            'tokens_out': 342,
            'latency_ms': 1250,
            'response_text': 'I can help you with our enterprise CRM solution. Based on your company size and requirements, I recommend our Premium tier which includes advanced automation features, custom reporting, and dedicated support. Would you like me to schedule a demo with our solutions team?',
            'tools_used': ['web_search', 'company_lookup', 'pricing_calculator'],
            'tool_results': {
                'web_search': {'success': True},
                'company_lookup': {'success': True, 'company_size': 'enterprise'},
                'pricing_calculator': {'success': True, 'recommended_tier': 'premium'}
            },
            'user_message': 'I need information about your CRM pricing for a 500+ employee company',
            'conversation_context': {
                'previous_turns': 0,
                'topic_history': [],
                'user_intent': 'product_inquiry'
            }
        }
    
    async def demonstrate_marketo_transformation(self):
        """Demonstrate Marketo lead transformation"""
        logger.info(\"\\n=== MARKETO TRANSFORMATION DEMO ===\")
        
        raw_data = self.create_sample_marketo_data()
        logger.info(f\"Raw Marketo Data: {json.dumps(raw_data, indent=2)}\")
        
        try:
            transformer = self.transformers['marketo']
            transformed = transformer.transform(raw_data)
            
            logger.info(\"\\n--- TRANSFORMED OUTPUT ---\")
            logger.info(f\"Event Type: {transformed['event_type']}\")
            logger.info(f\"Source: {transformed['source']}\")
            
            data = transformed['data']
            logger.info(f\"\\n--- ENHANCED LEAD DATA ---\")
            logger.info(f\"Lead Quality Score: {data['lead_quality_score']}/100\")
            logger.info(f\"Email Domain: {data['email_domain']}\")
            logger.info(f\"Full Name: {data['full_name']}\")
            logger.info(f\"Geographic Info: {json.dumps(data['geographic_info'], indent=2)}\")
            
            metadata = transformed['metadata']
            logger.info(f\"\\n--- TRANSFORMATION METADATA ---\")
            logger.info(f\"Transform Version: {metadata['transform_version']}\")
            logger.info(f\"Quality Checks: {metadata['quality_score']}/100\")
            logger.info(f\"Processing Time: {metadata['processing_time_ms']}ms\")
            
            return transformed
            
        except (ValidationError, TransformationError) as e:
            logger.error(f\"Transformation failed: {e}\")
            return None
    
    async def demonstrate_frontend_transformation(self):
        \"\"\"Demonstrate frontend interaction transformation\"\"\"
        logger.info(\"\\n=== FRONTEND TRANSFORMATION DEMO ===\")
        
        raw_data = self.create_sample_frontend_data()
        logger.info(f\"Raw Frontend Data: {json.dumps(raw_data, indent=2)}\")
        
        try:
            transformer = self.transformers['frontend']
            transformed = transformer.transform(raw_data)
            
            logger.info(\"\\n--- TRANSFORMED OUTPUT ---\")
            logger.info(f\"Event Type: {transformed['event_type']}\")
            
            data = transformed['data']
            logger.info(f\"\\n--- ENHANCED INTERACTION DATA ---\")
            logger.info(f\"User Segment: {data['user_segment']}\")
            logger.info(f\"Engagement Score: {data['engagement_score']}/100\")
            logger.info(f\"Conversion Stage: {data['conversion_stage']}\")
            logger.info(f\"Device Type: {data['device_info']['device_type']}\")
            logger.info(f\"Browser: {data['device_info']['browser']}\")
            logger.info(f\"Page Category: {data['page_info']['category']}\")
            logger.info(f\"Referrer Type: {data['referrer_info']['referrer_type']}\")
            
            return transformed
            
        except (ValidationError, TransformationError) as e:
            logger.error(f\"Transformation failed: {e}\")
            return None
    
    async def demonstrate_text_agent_transformation(self):
        \"\"\"Demonstrate text agent turn transformation\"\"\"
        logger.info(\"\\n=== TEXT AGENT TRANSFORMATION DEMO ===\")
        
        raw_data = self.create_sample_text_agent_data()
        logger.info(f\"Raw Agent Data: {json.dumps(raw_data, indent=2)}\")
        
        try:
            transformer = self.transformers['text_agent']
            transformed = transformer.transform(raw_data)
            
            logger.info(\"\\n--- TRANSFORMED OUTPUT ---\")
            logger.info(f\"Event Type: {transformed['event_type']}\")
            
            data = transformed['data']
            logger.info(f\"\\n--- ENHANCED AGENT ANALYTICS ---\")
            
            # Performance metrics
            perf = data['performance_metrics']
            logger.info(f\"Tokens per Second: {perf['tokens_per_second']:.1f}\")
            logger.info(f\"Efficiency Score: {perf['efficiency_score']}/100\")
            
            # Content analysis
            content = data['content_analysis']
            logger.info(f\"Response Length: {content['response_length']} chars\")
            logger.info(f\"Word Count: {content['word_count']}\")
            logger.info(f\"Language: {content['language']}\")
            logger.info(f\"Sentiment: {content['sentiment']}\")
            logger.info(f\"Topics: {content['topics']}\")
            
            # Quality metrics
            quality = data['quality_metrics']
            logger.info(f\"\\n--- QUALITY ANALYSIS ---\")
            logger.info(f\"Overall Quality Score: {quality['overall_quality_score']}/100\")
            logger.info(f\"Relevance Score: {quality['relevance_score']}/100\")
            logger.info(f\"Helpfulness Score: {quality['helpfulness_score']}/100\")
            
            # Business metrics
            business = data['business_metrics']
            logger.info(f\"\\n--- BUSINESS VALUE ---\")
            logger.info(f\"Business Value Score: {business['business_value_score']}/100\")
            logger.info(f\"Lead Quality Indicator: {business['lead_quality_indicator']}\")
            logger.info(f\"Conversion Probability: {business['conversion_probability']:.1%}\")
            logger.info(f\"Estimated Cost: ${business['estimated_cost_usd']:.4f}\")
            
            # Tool usage
            tools = data['tool_usage']
            logger.info(f\"\\n--- TOOL PERFORMANCE ---\")
            logger.info(f\"Tools Used: {tools['tools_count']}\")
            logger.info(f\"Success Rate: {tools['tool_success_rate']:.1%}\")
            logger.info(f\"Tools: {', '.join(tools['tools_used'])}\")
            
            return transformed
            
        except (ValidationError, TransformationError) as e:
            logger.error(f\"Transformation failed: {e}\")
            return None
    
    async def demonstrate_data_quality_improvements(self):
        \"\"\"Show how transforms improve data quality\"\"\"
        logger.info(\"\\n=== DATA QUALITY IMPROVEMENTS ===\")
        
        # Example with incomplete/messy data
        messy_lead_data = {
            'id': 99999,
            'email': '  JANE.SMITH@GMAIL.COM  ',  # Needs normalization
            'firstName': 'jane',  # Needs title case
            'lastName': 'smith',
            'company': '',  # Empty field
            'leadSource': 'web',  # Needs standardization
            'phone': '(555) 987-6543 ext 123',  # Needs formatting
            'country': 'usa',  # Needs standardization
            'createdAt': '2024-01-15T10:30:00.000Z'
        }
        
        logger.info(\"\\n--- BEFORE TRANSFORMATION (Messy Data) ---\")
        logger.info(f\"Raw Email: '{messy_lead_data['email']}'\")\n        logger.info(f\"Raw Name: '{messy_lead_data['firstName']} {messy_lead_data['lastName']}'\")\n        logger.info(f\"Raw Company: '{messy_lead_data['company']}'\")\n        logger.info(f\"Raw Lead Source: '{messy_lead_data['leadSource']}'\")\n        logger.info(f\"Raw Phone: '{messy_lead_data['phone']}'\")\n        \n        transformer = self.transformers['marketo']\n        transformed = transformer.transform(messy_lead_data)\n        data = transformed['data']\n        \n        logger.info(\"\\n--- AFTER TRANSFORMATION (Clean Data) ---\")\n        logger.info(f\"Normalized Email: '{data['email']}'\")\n        logger.info(f\"Full Name: '{data['full_name']}'\")\n        logger.info(f\"Company: '{data['company'] or 'N/A'}'\")\n        logger.info(f\"Lead Source: '{data['lead_source']}'\")\n        logger.info(f\"Formatted Phone: '{data['phone'] or 'N/A'}'\")\n        logger.info(f\"Email Domain: '{data['email_domain']}'\")\n        logger.info(f\"Quality Score: {data['lead_quality_score']}/100\")\n    \n    async def demonstrate_analytics_value(self):\n        \"\"\"Show analytics value from transforms\"\"\"  \n        logger.info(\"\\n=== ANALYTICS VALUE DEMONSTRATION ===\")\n        \n        # Transform all sample data\n        marketo_transformed = await self.demonstrate_marketo_transformation()\n        frontend_transformed = await self.demonstrate_frontend_transformation()\n        agent_transformed = await self.demonstrate_text_agent_transformation()\n        \n        if all([marketo_transformed, frontend_transformed, agent_transformed]):\n            logger.info(\"\\n--- CROSS-SOURCE ANALYTICS ---\")\n            \n            # Lead scoring\n            lead_score = marketo_transformed['data']['lead_quality_score']\n            engagement_score = frontend_transformed['data']['engagement_score']\n            agent_quality = agent_transformed['data']['quality_metrics']['overall_quality_score']\n            \n            logger.info(f\"Lead Quality Score: {lead_score}/100\")\n            logger.info(f\"Frontend Engagement: {engagement_score}/100\")\n            logger.info(f\"Agent Interaction Quality: {agent_quality}/100\")\n            \n            # Combined insights\n            combined_score = (lead_score + engagement_score + agent_quality) / 3\n            logger.info(f\"\\nCombined Customer Value Score: {combined_score:.1f}/100\")\n            \n            # Business recommendations\n            if combined_score >= 80:\n                logger.info(\"🔥 HIGH VALUE PROSPECT - Priority follow-up recommended\")\n            elif combined_score >= 60:\n                logger.info(\"⭐ QUALIFIED LEAD - Standard follow-up process\")\n            else:\n                logger.info(\"📧 NURTURE LEAD - Email campaign recommended\")\n\n\nasync def main():\n    \"\"\"Run the enhanced pipeline demonstration\"\"\"\n    logger.info(\"Starting Enhanced Data Pipeline Demonstration\")\n    logger.info(\"=\" * 60)\n    \n    demo = PipelineDemo()\n    \n    try:\n        # Run all demonstrations\n        await demo.demonstrate_marketo_transformation()\n        await demo.demonstrate_frontend_transformation()\n        await demo.demonstrate_text_agent_transformation()\n        await demo.demonstrate_data_quality_improvements()\n        await demo.demonstrate_analytics_value()\n        \n        logger.info(\"\\n\" + \"=\" * 60)\n        logger.info(\"✅ Enhanced Pipeline Demo Completed Successfully!\")\n        logger.info(\"\\n🚀 Key Benefits Demonstrated:\")\n        logger.info(\"   • Automated data quality improvements\")\n        logger.info(\"   • Rich analytics and scoring\")\n        logger.info(\"   • Cross-source data correlation\")\n        logger.info(\"   • Business-ready insights\")\n        logger.info(\"   • Standardized data formats\")\n        logger.info(\"   • Comprehensive metadata tracking\")\n        \n    except Exception as e:\n        logger.error(f\"Demo failed: {e}\")\n\n\nif __name__ == \"__main__\":\n    asyncio.run(main())\n