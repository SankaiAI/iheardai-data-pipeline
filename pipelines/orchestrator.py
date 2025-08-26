#!/usr/bin/env python3
"""
Data Pipeline Orchestrator for iheardAI

This module orchestrates the entire data pipeline, managing the execution
of extractors, consumers, and monitoring components.
"""

import asyncio
import json
import logging
import os
import signal
import subprocess
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass
from enum import Enum

import aiohttp
import yaml
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ServiceStatus(Enum):
    """Service status enumeration"""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"
    UNHEALTHY = "unhealthy"


@dataclass
class ServiceInstance:
    """Represents a running service instance"""
    name: str
    process: Optional[subprocess.Popen]
    status: ServiceStatus
    start_time: Optional[datetime]
    restart_count: int = 0
    last_health_check: Optional[datetime] = None
    health_url: Optional[str] = None
    pid: Optional[int] = None


class ServiceConfig(BaseModel):
    """Service configuration model"""
    name: str
    command: str
    args: List[str] = Field(default_factory=list)
    env: Dict[str, str] = Field(default_factory=dict)
    working_dir: Optional[str] = None
    health_check_url: Optional[str] = None
    health_check_interval: int = Field(default=60)  # seconds
    restart_policy: str = Field(default="always")  # always, on-failure, never
    max_restarts: int = Field(default=3)
    restart_delay: int = Field(default=5)  # seconds
    dependencies: List[str] = Field(default_factory=list)
    priority: int = Field(default=100)  # Lower values start first


class OrchestrationConfig(BaseModel):
    """Orchestration configuration"""
    services: List[ServiceConfig]
    health_check_interval: int = Field(default=30)
    startup_timeout: int = Field(default=120)
    shutdown_timeout: int = Field(default=30)
    monitoring_enabled: bool = Field(default=True)
    auto_restart: bool = Field(default=True)


class HealthChecker:
    """Service health monitoring"""
    
    def __init__(self):
        self.session = None
        
    async def initialize(self) -> None:
        """Initialize HTTP session for health checks"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        )
        
    async def close(self) -> None:
        """Close HTTP session"""
        if self.session:
            await self.session.close()
    
    async def check_service_health(self, service: ServiceInstance) -> bool:
        """Check if a service is healthy"""
        try:
            if not service.health_url or not self.session:
                # If no health check URL, assume healthy if process is running
                return service.process and service.process.poll() is None
            
            async with self.session.get(service.health_url) as response:
                if response.status == 200:
                    health_data = await response.json()
                    return health_data.get('status') == 'healthy'
                else:
                    logger.warning(f"Health check failed for {service.name}: HTTP {response.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"Health check error for {service.name}: {e}")
            return False
    
    async def check_all_services(self, services: Dict[str, ServiceInstance]) -> Dict[str, bool]:
        """Check health of all services concurrently"""
        health_tasks = {}
        
        for name, service in services.items():
            if service.status == ServiceStatus.RUNNING:
                health_tasks[name] = self.check_service_health(service)
        
        if not health_tasks:
            return {}
            
        results = await asyncio.gather(*health_tasks.values(), return_exceptions=True)
        
        health_status = {}
        for name, result in zip(health_tasks.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"Health check exception for {name}: {result}")
                health_status[name] = False
            else:
                health_status[name] = result
                
        return health_status


class ServiceManager:
    """Manages individual service lifecycle"""
    
    def __init__(self, config: ServiceConfig, base_env: Dict[str, str]):
        self.config = config
        self.base_env = base_env
        
    def create_service_env(self) -> Dict[str, str]:
        """Create environment variables for service"""
        env = self.base_env.copy()
        env.update(self.config.env)
        return env
    
    async def start_service(self) -> ServiceInstance:
        """Start a service process"""
        logger.info(f"Starting service: {self.config.name}")
        
        try:
            env = self.create_service_env()
            
            # Build command
            command = [self.config.command] + self.config.args
            
            # Start process
            process = subprocess.Popen(
                command,
                env=env,
                cwd=self.config.working_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid if os.name != 'nt' else None  # Process group for Unix
            )
            
            service = ServiceInstance(
                name=self.config.name,
                process=process,
                status=ServiceStatus.STARTING,
                start_time=datetime.now(timezone.utc),
                health_url=self.config.health_check_url,
                pid=process.pid
            )
            
            logger.info(f"Started {self.config.name} with PID {process.pid}")
            return service
            
        except Exception as e:
            logger.error(f"Failed to start {self.config.name}: {e}")
            raise
    
    async def stop_service(self, service: ServiceInstance, timeout: int = 30) -> None:
        """Stop a service gracefully"""
        if not service.process:
            return
            
        logger.info(f"Stopping service: {service.name}")
        service.status = ServiceStatus.STOPPING
        
        try:
            # Send SIGTERM
            if os.name == 'nt':
                service.process.terminate()
            else:
                os.killpg(os.getpgid(service.process.pid), signal.SIGTERM)
            
            # Wait for graceful shutdown
            try:
                service.process.wait(timeout=timeout)
                logger.info(f"Service {service.name} stopped gracefully")
            except subprocess.TimeoutExpired:
                logger.warning(f"Service {service.name} did not stop gracefully, forcing termination")
                
                # Force kill
                if os.name == 'nt':
                    service.process.kill()
                else:
                    os.killpg(os.getpgid(service.process.pid), signal.SIGKILL)
                
                service.process.wait()
                
        except ProcessLookupError:
            # Process already terminated
            logger.info(f"Service {service.name} was already terminated")
        except Exception as e:
            logger.error(f"Error stopping {service.name}: {e}")
        
        service.status = ServiceStatus.STOPPED
        service.process = None
    
    def should_restart(self, service: ServiceInstance) -> bool:
        """Determine if service should be restarted"""
        if self.config.restart_policy == "never":
            return False
        elif self.config.restart_policy == "on-failure":
            # Only restart if process exited with non-zero code
            return service.process and service.process.returncode != 0
        elif self.config.restart_policy == "always":
            return service.restart_count < self.config.max_restarts
        else:
            return False


class PipelineOrchestrator:
    """Main pipeline orchestrator"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.load_config()
        self.services: Dict[str, ServiceInstance] = {}
        self.service_managers: Dict[str, ServiceManager] = {}
        self.health_checker = HealthChecker()
        self.running = False
        self.shutdown_requested = False
        
    def load_config(self) -> None:
        """Load orchestration configuration"""
        with open(self.config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Define services based on pipeline components
        services_config = [
            # Marketo Extractor (CronJob-style)
            ServiceConfig(
                name="marketo-extractor",
                command="python",
                args=["-m", "etl.extract.marketo_extractor"],
                working_dir=".",
                health_check_url="http://localhost:8001/health",
                restart_policy="on-failure",
                max_restarts=3,
                priority=10
            ),
            
            # Frontend Events Extractor
            ServiceConfig(
                name="frontend-events-extractor",
                command="python",
                args=["-m", "etl.extract.frontend_events_extractor", "--mode", "hybrid"],
                working_dir=".",
                health_check_url="http://localhost:8002/health",
                restart_policy="always",
                priority=20,
                dependencies=["marketo-extractor"]
            ),
            
            # Text Agent Events Extractor
            ServiceConfig(
                name="text-agent-events-extractor",
                command="python",
                args=["-m", "etl.extract.text_agent_events_extractor", "--mode", "hybrid"],
                working_dir=".",
                health_check_url="http://localhost:8003/health",
                restart_policy="always",
                priority=20,
                dependencies=["marketo-extractor"]
            ),
            
            # KPI Consumer
            ServiceConfig(
                name="kpi-consumer",
                command="python",
                args=["-m", "etl.load.kpi_consumer"],
                working_dir=".",
                health_check_url="http://localhost:8004/health",
                restart_policy="always",
                priority=30,
                dependencies=["frontend-events-extractor", "text-agent-events-extractor"]
            ),
            
            # Billing Consumer
            ServiceConfig(
                name="billing-consumer",
                command="python",
                args=["-m", "etl.load.billing_consumer"],
                working_dir=".",
                health_check_url="http://localhost:8005/health",
                restart_policy="always",
                priority=30,
                dependencies=["kpi-consumer"]
            ),
            
            # Archive Worker
            ServiceConfig(
                name="archive-worker",
                command="python",
                args=["-m", "etl.load.archive_worker"],
                working_dir=".",
                health_check_url="http://localhost:8006/health",
                restart_policy="always",
                priority=40,
                dependencies=["billing-consumer"]
            )
        ]
        
        self.orchestration_config = OrchestrationConfig(services=services_config)
        
        # Create service managers
        base_env = os.environ.copy()
        for service_config in self.orchestration_config.services:
            self.service_managers[service_config.name] = ServiceManager(service_config, base_env)
    
    def get_dependency_order(self) -> List[str]:
        """Get services in dependency order for startup"""
        services_by_priority = sorted(
            self.orchestration_config.services,
            key=lambda s: s.priority
        )
        return [s.name for s in services_by_priority]
    
    async def wait_for_dependencies(self, service_name: str) -> bool:
        """Wait for service dependencies to be running"""
        service_config = next(
            (s for s in self.orchestration_config.services if s.name == service_name),
            None
        )
        
        if not service_config or not service_config.dependencies:
            return True
        
        max_wait = self.orchestration_config.startup_timeout
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            dependencies_ready = True
            
            for dep_name in service_config.dependencies:
                dep_service = self.services.get(dep_name)
                if not dep_service or dep_service.status != ServiceStatus.RUNNING:
                    dependencies_ready = False
                    break
            
            if dependencies_ready:
                return True
            
            await asyncio.sleep(1)
        
        logger.error(f"Dependencies for {service_name} not ready within timeout")
        return False
    
    async def start_service(self, service_name: str) -> bool:
        """Start a specific service"""
        try:
            # Wait for dependencies
            if not await self.wait_for_dependencies(service_name):
                return False
            
            # Start the service
            service_manager = self.service_managers[service_name]
            service = await service_manager.start_service()
            self.services[service_name] = service
            
            # Wait for startup
            startup_wait = 10  # seconds
            await asyncio.sleep(startup_wait)
            
            # Check if still running
            if service.process and service.process.poll() is None:
                service.status = ServiceStatus.RUNNING
                logger.info(f"Service {service_name} started successfully")
                return True
            else:
                logger.error(f"Service {service_name} failed to start")
                service.status = ServiceStatus.ERROR
                return False
                
        except Exception as e:
            logger.error(f"Failed to start {service_name}: {e}")
            return False
    
    async def stop_service(self, service_name: str) -> None:
        """Stop a specific service"""
        service = self.services.get(service_name)
        if service:
            service_manager = self.service_managers[service_name]
            await service_manager.stop_service(service, self.orchestration_config.shutdown_timeout)
    
    async def restart_service(self, service_name: str) -> bool:
        """Restart a specific service"""
        logger.info(f"Restarting service: {service_name}")
        
        service = self.services.get(service_name)
        if service:
            service.restart_count += 1
            await self.stop_service(service_name)
            await asyncio.sleep(self.service_managers[service_name].config.restart_delay)
        
        return await self.start_service(service_name)
    
    async def start_all_services(self) -> None:
        """Start all services in dependency order"""
        logger.info("Starting all pipeline services")
        
        service_order = self.get_dependency_order()
        
        for service_name in service_order:
            if self.shutdown_requested:
                break
                
            logger.info(f"Starting service: {service_name}")
            success = await self.start_service(service_name)
            
            if not success:
                logger.error(f"Failed to start {service_name}, stopping orchestration")
                return
        
        logger.info("All services started successfully")
    
    async def stop_all_services(self) -> None:
        """Stop all services in reverse dependency order"""
        logger.info("Stopping all pipeline services")
        
        service_order = list(reversed(self.get_dependency_order()))
        
        for service_name in service_order:
            if service_name in self.services:
                logger.info(f"Stopping service: {service_name}")
                await self.stop_service(service_name)
        
        logger.info("All services stopped")
    
    async def monitor_services(self) -> None:
        """Monitor service health and restart if needed"""
        if not self.orchestration_config.monitoring_enabled:
            return
            
        logger.info("Starting service monitoring")
        
        while self.running and not self.shutdown_requested:
            try:
                # Check health of all services
                health_status = await self.health_checker.check_all_services(self.services)
                
                for service_name, is_healthy in health_status.items():
                    service = self.services[service_name]
                    service.last_health_check = datetime.now(timezone.utc)
                    
                    if not is_healthy and service.status == ServiceStatus.RUNNING:
                        logger.warning(f"Service {service_name} is unhealthy")
                        service.status = ServiceStatus.UNHEALTHY
                        
                        # Attempt restart if auto-restart is enabled
                        if (self.orchestration_config.auto_restart and
                            self.service_managers[service_name].should_restart(service)):
                            
                            logger.info(f"Auto-restarting unhealthy service: {service_name}")
                            await self.restart_service(service_name)
                    
                    elif is_healthy and service.status == ServiceStatus.UNHEALTHY:
                        logger.info(f"Service {service_name} is healthy again")
                        service.status = ServiceStatus.RUNNING
                
                await asyncio.sleep(self.orchestration_config.health_check_interval)
                
            except Exception as e:
                logger.error(f"Error in service monitoring: {e}")
                await asyncio.sleep(5)
    
    async def get_status(self) -> Dict[str, Any]:
        """Get overall pipeline status"""
        status = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'orchestrator_running': self.running,
            'services': {}
        }
        
        for name, service in self.services.items():
            status['services'][name] = {
                'status': service.status.value,
                'start_time': service.start_time.isoformat() if service.start_time else None,
                'restart_count': service.restart_count,
                'last_health_check': service.last_health_check.isoformat() if service.last_health_check else None,
                'pid': service.pid
            }
        
        return status
    
    async def run(self) -> None:
        """Run the orchestrator"""
        self.running = True
        logger.info("Starting Pipeline Orchestrator")
        
        try:
            # Initialize health checker
            await self.health_checker.initialize()
            
            # Start all services
            await self.start_all_services()
            
            if not self.shutdown_requested:
                # Start monitoring
                monitor_task = asyncio.create_task(self.monitor_services())
                
                # Wait for shutdown signal
                while self.running and not self.shutdown_requested:
                    await asyncio.sleep(1)
                
                # Cancel monitoring
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    pass
            
        except Exception as e:
            logger.error(f"Orchestrator error: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self) -> None:
        """Cleanup resources"""
        logger.info("Cleaning up orchestrator")
        
        # Stop all services
        await self.stop_all_services()
        
        # Close health checker
        await self.health_checker.close()
        
        self.running = False
        logger.info("Orchestrator cleanup completed")
    
    def signal_handler(self, signum: int) -> None:
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating shutdown")
        self.shutdown_requested = True
        self.running = False


async def main():
    """Main execution function"""
    orchestrator = PipelineOrchestrator()
    
    # Set up signal handlers
    if os.name != 'nt':  # Unix/Linux
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: orchestrator.signal_handler(s))
    else:  # Windows
        signal.signal(signal.SIGINT, lambda s, f: orchestrator.signal_handler(s))
        signal.signal(signal.SIGTERM, lambda s, f: orchestrator.signal_handler(s))
    
    await orchestrator.run()


if __name__ == "__main__":
    asyncio.run(main())