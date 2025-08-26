# iheardAI Data Pipeline - Deployment Guide

## Overview

This guide provides step-by-step instructions for deploying the iheardAI data pipeline architecture on DigitalOcean infrastructure, specifically designed to integrate with **iheardAI_Frontend** and **text-agent-server** components.

## Prerequisites

### Required Accounts & Access
- DigitalOcean account with billing enabled
- Marketo API access (client ID, client secret, base URL)
- Access to iheardAI_Frontend and text-agent-server deployments

### Local Tools Required
- `doctl` (DigitalOcean CLI)
- `kubectl` (Kubernetes CLI)
- `helm` (Kubernetes package manager)
- `docker` and `docker-compose`
- `python 3.11+`

## Infrastructure Setup

### 1. DigitalOcean Infrastructure

#### 1.1 Create Project and VPC
```bash
# Create project
doctl projects create --name "iheardai-data-pipeline" --description "Data pipeline infrastructure"

# Create VPC
doctl vpcs create --name iheardai-data-vpc --region nyc3 --ip-range 10.0.0.0/16
```

#### 1.2 Create DOKS Cluster
```bash
# Create Kubernetes cluster
doctl kubernetes cluster create iheardai-data-cluster \
  --region nyc3 \
  --version 1.28.2-do.0 \
  --node-pool "name=worker-pool;size=s-4vcpu-8gb;count=3;auto-scale=true;min-nodes=2;max-nodes=6" \
  --vpc-uuid <vpc-uuid>

# Get kubeconfig
doctl kubernetes cluster kubeconfig save iheardai-data-cluster
```

#### 1.3 Create Container Registry
```bash
# Create container registry
doctl registry create iheardai-pipeline-registry
```

#### 1.4 Create Managed Services
```bash
# Create managed Kafka cluster
doctl databases create kafka-cluster \
  --engine kafka \
  --region nyc3 \
  --size db-s-2vcpu-2gb \
  --num-nodes 3 \
  --vpc-uuid <vpc-uuid>

# Create managed PostgreSQL
doctl databases create postgres-cluster \
  --engine pg \
  --region nyc3 \
  --size db-s-4vcpu-8gb \
  --num-nodes 1 \
  --vpc-uuid <vpc-uuid> \
  --version 15

# Create managed Redis
doctl databases create redis-cluster \
  --engine redis \
  --region nyc3 \
  --size db-s-1vcpu-1gb \
  --num-nodes 1 \
  --vpc-uuid <vpc-uuid>

# Create Spaces bucket
doctl spaces create iheardai-events-archive --region nyc3
```

### 2. Configure Database Schemas

#### 2.1 PostgreSQL Setup
```bash
# Connect to PostgreSQL and create tables
psql $POSTGRES_CONNECTION_STRING -f scripts/create_tables.sql
```

#### 2.2 Redis Configuration
```bash
# Redis will be configured automatically by the application
# No manual setup required
```

### 3. Kafka Topic Setup

#### 3.1 Create Topics
```bash
# Set up Kafka topics using the configuration
python scripts/setup_kafka_topics.py --config config/config.yaml
```

## Application Deployment

### 1. Build and Push Docker Images

#### 1.1 Login to Registry
```bash
doctl registry login
```

#### 1.2 Build Images
```bash
# Build base image
docker build -t registry.digitalocean.com/iheardai-pipeline-registry/data-pipeline-base .

# Build extractor images
docker build -f Dockerfile.extractor -t registry.digitalocean.com/iheardai-pipeline-registry/data-pipeline-extractor .

# Build consumer images  
docker build -f Dockerfile.consumer -t registry.digitalocean.com/iheardai-pipeline-registry/data-pipeline-consumer .

# Build orchestrator image
docker build -f Dockerfile.orchestrator -t registry.digitalocean.com/iheardai-pipeline-registry/data-pipeline-orchestrator .
```

#### 1.3 Push Images
```bash
docker push registry.digitalocean.com/iheardai-pipeline-registry/data-pipeline-base
docker push registry.digitalocean.com/iheardai-pipeline-registry/data-pipeline-extractor
docker push registry.digitalocean.com/iheardai-pipeline-registry/data-pipeline-consumer
docker push registry.digitalocean.com/iheardai-pipeline-registry/data-pipeline-orchestrator
```

### 2. Kubernetes Deployment

#### 2.1 Create Namespace
```bash
kubectl create namespace data-pipeline
```

#### 2.2 Create Secrets
```bash
# Create secret with all environment variables
kubectl create secret generic data-pipeline-secrets \
  --from-env-file=config/.env \
  --namespace=data-pipeline
```

#### 2.3 Deploy Applications
```bash
# Deploy using Helm
helm install data-pipeline ./helm/data-pipeline \
  --namespace data-pipeline \
  --values helm/values.yaml
```

## Configuration

### 1. Environment Variables

Copy the example environment file and configure:
```bash
cp config/secrets.env.example config/.env
```

Edit `config/.env` with your actual values:
```bash
# Kafka Configuration
KAFKA_USER=your_kafka_user
KAFKA_PASSWORD=your_kafka_password

# PostgreSQL Configuration  
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=25060
POSTGRES_DB=iheardai_analytics
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password

# Redis Configuration
REDIS_HOST=your_redis_host
REDIS_PORT=25061
REDIS_PASSWORD=your_redis_password

# DigitalOcean Spaces
SPACES_ACCESS_KEY=your_spaces_key
SPACES_SECRET_KEY=your_spaces_secret

# Marketo API
MARKETO_BASE_URL=https://123-ABC-456.mktorest.com
MARKETO_CLIENT_ID=your_client_id
MARKETO_CLIENT_SECRET=your_client_secret

# iheardAI Integration
FRONTEND_API_URL=https://your-frontend.vercel.app
FRONTEND_WEBHOOK_SECRET=your_webhook_secret
TEXT_AGENT_API_URL=https://your-text-agent.railway.app
TEXT_AGENT_API_KEY=your_api_key
```

### 2. Application Configuration

Update `config/config.yaml` with your specific settings:
- Kafka broker addresses
- Database connection parameters
- Topic configurations
- Processing parameters

## Integration with Existing Services

### 1. iheardAI_Frontend Integration

#### 1.1 Add Event Publishing
Add to your frontend application:
```javascript
// In your analytics or event tracking code
const publishEvent = async (eventData) => {
  await fetch('/api/analytics', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(eventData)
  });
};

// Track user interactions
publishEvent({
  event_type: 'widget_open',
  session_id: sessionId,
  user_id: userId,
  page_url: window.location.href,
  widget_id: 'sales-assistant',
  interaction_type: 'open',
  timestamp: Date.now(),
  metadata: { source: 'chat_button' }
});
```

#### 1.2 Add Analytics Endpoint
Add to your frontend API routes (`app/api/analytics/route.ts`):
```typescript
export async function POST(request: Request) {
  const eventData = await request.json();
  
  // Store in database or forward to data pipeline
  await storeAnalyticsEvent(eventData);
  
  return Response.json({ success: true });
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const since = searchParams.get('since');
  const limit = parseInt(searchParams.get('limit') || '100');
  
  const events = await getAnalyticsEvents(since, limit);
  
  return Response.json({ events });
}
```

### 2. text-agent-server Integration

#### 2.1 Add Turn Completion Events
Add to your text agent completion logic:
```python
# In your agent completion handler
async def on_turn_completed(self, turn_data: dict):
    event = {
        'event_type': 'turn_completed',
        'session_id': turn_data['session_id'],
        'turn_id': turn_data['turn_id'],
        'user_id': turn_data.get('user_id'),
        'channel': 'text',
        'model': turn_data['model'],
        'tokens_in': turn_data['tokens_in'],
        'tokens_out': turn_data['tokens_out'],
        'latency_ms': turn_data['latency_ms'],
        'response_text': self.redact_pii(turn_data['response_text']),
        'timestamp': int(time.time() * 1000),
        'metadata': turn_data.get('metadata', {})
    }
    
    # Publish to internal analytics store
    await self.analytics_store.store_event(event)
```

#### 2.2 Add Analytics Endpoints
Add to your FastAPI application:
```python
@app.get("/api/analytics/turns")
async def get_turn_events(
    since: Optional[str] = None,
    limit: int = 100
):
    events = await analytics_store.get_events(
        event_type="turn_completed",
        since=since,
        limit=limit
    )
    return {"turns": events}

@app.get("/api/analytics/tools")
async def get_tool_events(
    since: Optional[str] = None,
    limit: int = 100
):
    events = await analytics_store.get_events(
        event_type="tool_invoked",
        since=since,
        limit=limit
    )
    return {"tool_invocations": events}
```

## Monitoring and Observability

### 1. Grafana Dashboards

Access Grafana at `http://your-cluster-ip:3000`:
- Username: `admin`
- Password: `admin` (change immediately)

Import the provided dashboards:
- Pipeline Overview
- Kafka Metrics
- Database Performance
- Service Health

### 2. Prometheus Metrics

Metrics are available at `http://your-cluster-ip:9090`:
- Kafka producer/consumer metrics
- Database operation metrics
- Service health status
- Processing errors and latency

### 3. Log Aggregation

Logs are collected by Promtail and stored in Loki:
- Service logs
- Application errors
- Kafka consumer lag
- Database query performance

## Scaling and Performance

### 1. Auto-scaling Configuration

```bash
# Enable cluster autoscaler
kubectl apply -f k8s/autoscaler.yaml

# Configure horizontal pod autoscaler
kubectl apply -f k8s/hpa.yaml
```

### 2. Performance Tuning

#### Kafka Optimization
- Increase partition count for high-throughput topics
- Tune consumer batch sizes
- Optimize serialization (consider Protobuf)

#### Database Optimization
- Add appropriate indexes
- Enable connection pooling
- Consider read replicas for analytics

#### Redis Optimization
- Configure memory policies
- Set appropriate TTLs
- Monitor memory usage

## Backup and Recovery

### 1. Database Backups
```bash
# Enable automated backups in DigitalOcean
doctl databases backup schedule postgres-cluster --hour 2 --minute 0
```

### 2. Configuration Backups
```bash
# Backup Kubernetes configurations
kubectl get all,secrets,configmaps -n data-pipeline -o yaml > backup/k8s-backup.yaml
```

### 3. Data Recovery
- PostgreSQL: Point-in-time recovery available
- Spaces: Versioning enabled for data archives
- Kafka: Configure retention based on recovery needs

## Troubleshooting

### 1. Common Issues

#### Service Start Failures
```bash
# Check pod logs
kubectl logs -n data-pipeline deployment/kpi-consumer

# Check service health
kubectl get pods -n data-pipeline
kubectl describe pod <pod-name> -n data-pipeline
```

#### Kafka Connection Issues
```bash
# Test Kafka connectivity
kubectl exec -it <kafka-pod> -- kafka-console-producer --topic test --bootstrap-server localhost:9092
```

#### Database Connection Issues
```bash
# Test PostgreSQL connection
kubectl exec -it <postgres-pod> -- psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB
```

### 2. Performance Issues

#### High Consumer Lag
- Increase consumer instances
- Optimize message processing
- Check database performance

#### Database Slow Queries
- Enable query logging
- Add missing indexes
- Optimize query patterns

## Security Considerations

### 1. Network Security
- All services deployed in VPC
- No public access to databases
- TLS encryption for all connections

### 2. Data Protection
- PII redaction at source
- Encryption at rest and in transit
- Regular security updates

### 3. Access Control
- RBAC configured in Kubernetes
- Service-specific database users
- API key rotation schedule

## Maintenance

### 1. Regular Tasks
- Update container images weekly
- Rotate secrets quarterly
- Review and optimize configurations monthly
- Archive old data based on retention policies

### 2. Monitoring Alerts
- Consumer lag > 1000 messages
- Database connection pool exhaustion
- Service health check failures
- Disk space utilization > 80%

## Support and Documentation

### 1. Architecture Documentation
- See `docs/architecture.md` for detailed system design
- API documentation available in each service directory
- Database schema documentation in `scripts/`

### 2. Runbooks
- Service restart procedures
- Data backfill processes  
- Incident response procedures
- Scaling procedures

### 3. Contact Information
- Development team: [team@iheardai.com]
- On-call rotation: [oncall@iheardai.com]
- Emergency escalation: [emergency@iheardai.com]