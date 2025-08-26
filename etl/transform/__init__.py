"""
Data Transformation Module for iheardAI Data Pipeline

This module provides specialized transformers for different data sources,
converting raw data into standardized, analytics-ready formats.
"""

from .base_transformer import BaseTransformer, ValidationError, TransformationError
from .marketo_transformer import MarketoTransformer, MarketoActivityTransformer
from .frontend_transformer import FrontendTransformer
from .text_agent_transformer import TextAgentTransformer

__all__ = [
    'BaseTransformer',
    'ValidationError', 
    'TransformationError',
    'MarketoTransformer',
    'MarketoActivityTransformer',
    'FrontendTransformer',
    'TextAgentTransformer'
]


# Factory function for easy transformer creation
def get_transformer(data_source: str) -> BaseTransformer:
    """
    Factory function to get appropriate transformer for data source
    
    Args:
        data_source: Source type ('marketo', 'frontend', 'text_agent')
    
    Returns:
        Appropriate transformer instance
    
    Raises:
        ValueError: If data source is not supported
    """
    transformers = {
        'marketo': MarketoTransformer,
        'marketo_activity': MarketoActivityTransformer,
        'frontend': FrontendTransformer,
        'text_agent': TextAgentTransformer
    }
    
    if data_source not in transformers:
        raise ValueError(f"Unsupported data source: {data_source}")
    
    return transformers[data_source]()