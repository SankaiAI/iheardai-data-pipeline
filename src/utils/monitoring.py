#!/usr/bin/env python3
"""
Monitoring and Observability Module for iheardAI Data Pipeline

This module provides Prometheus metrics, health checks, and alerting
capabilities for the data pipeline components.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

import aiohttp
from prometheus_client import Counter, Histogram, Gauge, start_http_server, CollectorRegistry, REGISTRY
from prometheus_client.core import REGISTRY as DEFAULT_REGISTRY
import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


@dataclass
class MetricConfig:
    """Configuration for metrics collection"""
    enabled: bool = True
    prometheus_port: int = 9090
    pushgateway_url: Optional[str] = None
    collection_interval: int = 15  # seconds
    retention_days: int = 7


@dataclass
class AlertConfig:
    """Configuration for alerting"""
    enabled: bool = True
    webhook_url: Optional[str] = None
    slack_webhook_url: Optional[str] = None
    email_smtp_server: Optional[str] = None
    email_recipients: List[str] = None


class PrometheusMetrics:
    """Prometheus metrics collection"""
    
    def __init__(self, registry: CollectorRegistry = None):
        self.registry = registry or DEFAULT_REGISTRY
        self._setup_metrics()
    
    def _setup_metrics(self) -> None:
        """Initialize Prometheus metrics"""
        
        # Kafka Producer Metrics
        self.kafka_messages_produced_total = Counter(
            'kafka_messages_produced_total',
            'Total number of messages produced to Kafka',
            ['topic', 'service'],
            registry=self.registry
        )
        
        self.kafka_produce_duration_seconds = Histogram(
            'kafka_produce_duration_seconds',
            'Time spent producing messages to Kafka',
            ['topic', 'service'],
            registry=self.registry
        )
        
        self.kafka_produce_errors_total = Counter(
            'kafka_produce_errors_total',
            'Total number of Kafka produce errors',
            ['topic', 'service', 'error_type'],
            registry=self.registry
        )
        
        # Kafka Consumer Metrics
        self.kafka_messages_consumed_total = Counter(
            'kafka_messages_consumed_total',
            'Total number of messages consumed from Kafka',
            ['topic', 'service', 'consumer_group'],
            registry=self.registry
        )
        
        self.kafka_consumer_lag_messages = Gauge(
            'kafka_consumer_lag_messages',
            'Current consumer lag in messages',
            ['topic', 'partition', 'consumer_group'],
            registry=self.registry
        )
        
        self.kafka_message_processing_duration_seconds = Histogram(
            'kafka_message_processing_duration_seconds',
            'Time spent processing Kafka messages',
            ['topic', 'service'],
            registry=self.registry
        )
        
        # Database Metrics
        self.database_operations_total = Counter(
            'database_operations_total',
            'Total number of database operations',
            ['operation', 'table', 'service'],
            registry=self.registry
        )
        
        self.database_operation_duration_seconds = Histogram(
            'database_operation_duration_seconds',
            'Time spent on database operations',
            ['operation', 'table', 'service'],
            registry=self.registry
        )
        
        self.database_connection_pool_active = Gauge(
            'database_connection_pool_active',
            'Number of active database connections',
            ['service'],
            registry=self.registry
        )
        
        # Redis Metrics
        self.redis_operations_total = Counter(
            'redis_operations_total',
            'Total number of Redis operations',
            ['operation', 'service'],
            registry=self.registry
        )
        
        self.redis_operation_duration_seconds = Histogram(
            'redis_operation_duration_seconds',
            'Time spent on Redis operations',
            ['operation', 'service'],
            registry=self.registry
        )
        
        # Application Metrics
        self.service_health_status = Gauge(
            'service_health_status',
            'Health status of services (1=healthy, 0=unhealthy)',
            ['service'],
            registry=self.registry
        )
        
        self.event_processing_errors_total = Counter(
            'event_processing_errors_total',
            'Total number of event processing errors',
            ['service', 'error_type'],
            registry=self.registry
        )
        
        self.active_sessions_total = Gauge(
            'active_sessions_total',
            'Total number of active sessions',
            ['channel'],
            registry=self.registry
        )
        
        # API Metrics
        self.api_requests_total = Counter(
            'api_requests_total',
            'Total number of API requests',
            ['method', 'endpoint', 'status_code', 'service'],
            registry=self.registry
        )
        
        self.api_request_duration_seconds = Histogram(
            'api_request_duration_seconds',
            'API request duration',
            ['method', 'endpoint', 'service'],
            registry=self.registry
        )


class MetricsCollector:
    """Centralized metrics collection and reporting"""
    
    def __init__(self, config: MetricConfig):
        self.config = config
        self.metrics = PrometheusMetrics()
        self.running = False
        self._start_time = time.time()
        
    async def start_prometheus_server(self) -> None:
        """Start Prometheus metrics server"""
        if not self.config.enabled:
            return
            
        try:
            start_http_server(self.config.prometheus_port)
            logger.info(f"Prometheus metrics server started on port {self.config.prometheus_port}")
        except Exception as e:
            logger.error(f"Failed to start Prometheus server: {e}")
    
    def record_kafka_produce(self, topic: str, service: str, duration: float, success: bool = True, error_type: str = None) -> None:
        """Record Kafka producer metrics"""
        if not self.config.enabled:
            return
            
        self.metrics.kafka_messages_produced_total.labels(topic=topic, service=service).inc()
        self.metrics.kafka_produce_duration_seconds.labels(topic=topic, service=service).observe(duration)
        
        if not success and error_type:
            self.metrics.kafka_produce_errors_total.labels(
                topic=topic, service=service, error_type=error_type
            ).inc()
    
    def record_kafka_consume(self, topic: str, service: str, consumer_group: str, processing_duration: float, lag: int = 0) -> None:
        """Record Kafka consumer metrics"""
        if not self.config.enabled:
            return
            
        self.metrics.kafka_messages_consumed_total.labels(
            topic=topic, service=service, consumer_group=consumer_group
        ).inc()
        
        self.metrics.kafka_message_processing_duration_seconds.labels(
            topic=topic, service=service
        ).observe(processing_duration)
        
        if lag > 0:
            self.metrics.kafka_consumer_lag_messages.labels(
                topic=topic, partition="0", consumer_group=consumer_group
            ).set(lag)
    
    def record_database_operation(self, operation: str, table: str, service: str, duration: float) -> None:
        """Record database operation metrics"""
        if not self.config.enabled:
            return
            
        self.metrics.database_operations_total.labels(
            operation=operation, table=table, service=service
        ).inc()
        
        self.metrics.database_operation_duration_seconds.labels(
            operation=operation, table=table, service=service
        ).observe(duration)
    
    def record_redis_operation(self, operation: str, service: str, duration: float) -> None:
        """Record Redis operation metrics"""
        if not self.config.enabled:
            return
            
        self.metrics.redis_operations_total.labels(operation=operation, service=service).inc()
        self.metrics.redis_operation_duration_seconds.labels(operation=operation, service=service).observe(duration)
    
    def set_service_health(self, service: str, healthy: bool) -> None:
        """Set service health status"""
        if not self.config.enabled:
            return
            
        self.metrics.service_health_status.labels(service=service).set(1 if healthy else 0)
    
    def record_processing_error(self, service: str, error_type: str) -> None:
        """Record event processing error"""
        if not self.config.enabled:
            return
            
        self.metrics.event_processing_errors_total.labels(service=service, error_type=error_type).inc()
    
    def set_active_sessions(self, channel: str, count: int) -> None:
        """Set active sessions count"""
        if not self.config.enabled:
            return
            
        self.metrics.active_sessions_total.labels(channel=channel).set(count)
    
    def record_api_request(self, method: str, endpoint: str, service: str, status_code: int, duration: float) -> None:
        """Record API request metrics"""
        if not self.config.enabled:
            return
            
        self.metrics.api_requests_total.labels(
            method=method, endpoint=endpoint, status_code=str(status_code), service=service
        ).inc()
        
        self.metrics.api_request_duration_seconds.labels(
            method=method, endpoint=endpoint, service=service
        ).observe(duration)


class HealthCheck:
    """Service health check implementation"""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.startup_time = datetime.now(timezone.utc)
        self.checks: Dict[str, Callable] = {}
        
    def add_check(self, name: str, check_func: Callable) -> None:
        """Add a health check function"""
        self.checks[name] = check_func
    
    async def check_health(self) -> Dict[str, Any]:
        """Perform all health checks"""
        health_status = {
            'service': self.service_name,
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'uptime_seconds': (datetime.now(timezone.utc) - self.startup_time).total_seconds(),
            'checks': {}
        }
        
        overall_healthy = True
        
        for check_name, check_func in self.checks.items():
            try:
                result = await check_func() if asyncio.iscoroutinefunction(check_func) else check_func()
                health_status['checks'][check_name] = {
                    'status': 'healthy' if result else 'unhealthy',
                    'result': result
                }
                if not result:
                    overall_healthy = False
            except Exception as e:
                health_status['checks'][check_name] = {
                    'status': 'error',
                    'error': str(e)
                }
                overall_healthy = False
        
        health_status['status'] = 'healthy' if overall_healthy else 'unhealthy'
        return health_status


class AlertManager:
    """Alert management and notification"""
    
    def __init__(self, config: AlertConfig):
        self.config = config
        self.session = None
        
    async def initialize(self) -> None:
        """Initialize HTTP session for alerts"""
        if self.config.enabled:
            self.session = aiohttp.ClientSession()
    
    async def close(self) -> None:
        """Close HTTP session"""
        if self.session:
            await self.session.close()
    
    async def send_alert(self, alert_type: str, message: str, severity: str = "warning", metadata: Dict[str, Any] = None) -> None:
        """Send an alert notification"""
        if not self.config.enabled:
            return
        
        alert_data = {
            'type': alert_type,
            'message': message,
            'severity': severity,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'metadata': metadata or {}
        }
        
        # Log the alert
        logger.warning(f"ALERT: {alert_type} - {message}", extra=alert_data)
        
        # Send to configured endpoints
        await asyncio.gather(
            self._send_webhook_alert(alert_data),
            self._send_slack_alert(alert_data),
            return_exceptions=True
        )
    
    async def _send_webhook_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send alert to generic webhook"""
        if not self.config.webhook_url or not self.session:
            return
        
        try:
            async with self.session.post(
                self.config.webhook_url,
                json=alert_data,
                timeout=10
            ) as response:
                if response.status == 200:
                    logger.debug("Alert sent to webhook successfully")
                else:
                    logger.error(f"Webhook alert failed: {response.status}")
        except Exception as e:
            logger.error(f"Error sending webhook alert: {e}")
    
    async def _send_slack_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send alert to Slack webhook"""
        if not self.config.slack_webhook_url or not self.session:
            return
        
        # Format message for Slack
        color = {
            'info': 'good',
            'warning': 'warning',
            'error': 'danger',
            'critical': 'danger'
        }.get(alert_data['severity'], 'warning')
        
        slack_payload = {
            'attachments': [{
                'color': color,
                'title': f"Pipeline Alert: {alert_data['type']}",
                'text': alert_data['message'],
                'fields': [
                    {'title': 'Severity', 'value': alert_data['severity'], 'short': True},
                    {'title': 'Timestamp', 'value': alert_data['timestamp'], 'short': True}
                ],
                'footer': 'iheardAI Data Pipeline'
            }]
        }
        
        try:
            async with self.session.post(
                self.config.slack_webhook_url,
                json=slack_payload,
                timeout=10
            ) as response:
                if response.status == 200:
                    logger.debug("Alert sent to Slack successfully")
                else:
                    logger.error(f"Slack alert failed: {response.status}")
        except Exception as e:
            logger.error(f"Error sending Slack alert: {e}")


class MonitoringService:
    """Main monitoring service that coordinates metrics and alerts"""
    
    def __init__(self, service_name: str, metrics_config: MetricConfig, alert_config: AlertConfig):
        self.service_name = service_name
        self.metrics_collector = MetricsCollector(metrics_config)
        self.alert_manager = AlertManager(alert_config)
        self.health_check = HealthCheck(service_name)
        self.running = False
        
    async def initialize(self) -> None:
        """Initialize monitoring service"""
        await self.metrics_collector.start_prometheus_server()
        await self.alert_manager.initialize()
        
        # Set initial service health
        self.metrics_collector.set_service_health(self.service_name, True)
        
        logger.info(f"Monitoring service initialized for {self.service_name}")
    
    async def close(self) -> None:
        """Close monitoring service"""
        await self.alert_manager.close()
        logger.info(f"Monitoring service closed for {self.service_name}")
    
    def get_metrics_collector(self) -> MetricsCollector:
        """Get metrics collector instance"""
        return self.metrics_collector
    
    def get_alert_manager(self) -> AlertManager:
        """Get alert manager instance"""
        return self.alert_manager
    
    def get_health_check(self) -> HealthCheck:
        """Get health check instance"""
        return self.health_check
    
    async def periodic_health_check(self, interval: int = 60) -> None:
        """Run periodic health checks"""
        while self.running:
            try:
                health_status = await self.health_check.check_health()
                
                # Update metrics
                is_healthy = health_status['status'] == 'healthy'
                self.metrics_collector.set_service_health(self.service_name, is_healthy)
                
                # Send alert if unhealthy
                if not is_healthy:
                    await self.alert_manager.send_alert(
                        alert_type="service_unhealthy",
                        message=f"Service {self.service_name} failed health check",
                        severity="error",
                        metadata=health_status
                    )
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in periodic health check: {e}")
                await asyncio.sleep(10)
    
    @asynccontextmanager
    async def monitor_operation(self, operation_type: str, **kwargs):
        """Context manager for monitoring operations with metrics and error handling"""
        start_time = time.time()
        success = True
        error_type = None
        
        try:
            yield
        except Exception as e:
            success = False
            error_type = type(e).__name__
            
            # Record error metric
            self.metrics_collector.record_processing_error(self.service_name, error_type)
            
            # Send alert for critical errors
            await self.alert_manager.send_alert(
                alert_type="operation_failed",
                message=f"Operation {operation_type} failed: {str(e)}",
                severity="error",
                metadata={'operation': operation_type, 'error': str(e), **kwargs}
            )
            
            raise
        finally:
            duration = time.time() - start_time
            
            # Record operation-specific metrics
            if operation_type == "kafka_produce":
                self.metrics_collector.record_kafka_produce(
                    kwargs.get('topic', 'unknown'),
                    self.service_name,
                    duration,
                    success,
                    error_type
                )
            elif operation_type == "database_operation":
                self.metrics_collector.record_database_operation(
                    kwargs.get('operation', 'unknown'),
                    kwargs.get('table', 'unknown'),
                    self.service_name,
                    duration
                )
            elif operation_type == "redis_operation":
                self.metrics_collector.record_redis_operation(
                    kwargs.get('operation', 'unknown'),
                    self.service_name,
                    duration
                )


# Utility functions for easy integration
def create_monitoring_service(service_name: str, config: Dict[str, Any]) -> MonitoringService:
    """Factory function to create monitoring service"""
    metrics_config = MetricConfig(**config.get('monitoring', {}).get('metrics', {}))
    alert_config = AlertConfig(**config.get('monitoring', {}).get('alerts', {}))
    
    return MonitoringService(service_name, metrics_config, alert_config)


def setup_service_monitoring(service_name: str, config_path: str = "config/config.yaml") -> MonitoringService:
    """Setup monitoring for a service with config file"""
    import yaml
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return create_monitoring_service(service_name, config)