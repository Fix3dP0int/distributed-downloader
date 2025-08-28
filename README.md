# Distributed Hugging Face Downloader

## Overview

Due to bandwidth limitations imposed by proxies on a single machine, even with multi-threading it's difficult to exceed the maximum download speed (1 MB/s). To overcome the bandwidth constraints of a single machine, we need a distributed downloader.

The system works as follows: a master node retrieves a specified Hugging Face dataset name, fetches all files associated with that dataset, and publishes them to a Redis message queue. Each machine acts as a worker, consuming tasks from Redis. Based on the file names specified in each task, workers use the Hugging Face Python SDK to download the corresponding files.

In theory, if the number of machines matches the number of files, the total download speed for the entire repository is only limited by the size of individual files.

## Features

- **Distributed Architecture**: Scale downloads across multiple machines
- **Redis Message Queue**: Reliable task distribution and coordination
- **Automatic Retry**: Failed downloads are automatically retried with configurable limits
- **Progress Monitoring**: Real-time status updates and progress tracking
- **Graceful Shutdown**: Workers handle interruptions gracefully
- **Fault Tolerance**: System continues working even if individual workers fail
- **Dual Download Methods**: Uses both Hugging Face Hub API and direct HTTP downloads

## Architecture

### Components

1. **Master Node** (`MasterNode`)
   - Discovers all files in a Hugging Face dataset
   - Creates download tasks and publishes them to Redis queue
   - Monitors overall job progress
   - Provides status reporting

2. **Worker Nodes** (`WorkerNode`)
   - Consume download tasks from Redis queue
   - Download files using Hugging Face Hub or direct HTTP
   - Report progress and handle failures
   - Send periodic heartbeats to maintain alive status

3. **Redis Message Queue** (`RedisClient`)
   - Task queue management
   - Worker coordination and heartbeat tracking
   - Job status and progress tracking
   - Failed task retry management

### Data Models

- **DownloadTask**: Individual file download task
- **WorkerStatus**: Worker health and activity tracking
- **JobStatus**: Overall download job progress
- **TaskStatus**: Enumeration of task states (pending, in_progress, completed, failed, retrying)

## Installation

### Prerequisites

- Python 3.10+
- Redis server
- UV package manager

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd distributed_downloader

# Install dependencies with UV
uv sync

# Or install in development mode
uv pip install -e .
```

### Redis Setup

```bash
# Install and start Redis (Ubuntu/Debian)
sudo apt-get install redis-server
sudo systemctl start redis-server

# Or using Docker
docker run -d -p 6379:6379 redis:latest
```

## Usage

### Command Line Interface

The system provides a CLI tool `hf-downloader` with the following commands:

#### Start a Download Job (Master)

```bash
# Create a download job for a dataset
hf-downloader master "microsoft/DialoGPT-medium"

# Specify output directory
hf-downloader master "microsoft/DialoGPT-medium" --output-dir ./my-downloads
```

#### Start Worker Nodes

```bash
# Start a worker (run on each machine)
hf-downloader worker

# Start worker with custom ID
hf-downloader worker --worker-id "worker-01"
```

#### Monitor Progress

```bash
# Show overall status
hf-downloader status

# Monitor specific job
hf-downloader status <job-id>

# Watch status with live updates
hf-downloader status <job-id> --watch

# Check queue statistics
hf-downloader queue

# List active workers
hf-downloader workers
```

### Configuration Options

```bash
# Custom Redis configuration
hf-downloader --redis-host 192.168.1.100 --redis-port 6380 master "dataset-name"

# Set log level
hf-downloader --log-level DEBUG worker
```

## Example Workflow

1. **Start Redis server**
   ```bash
   redis-server
   ```

2. **Create a download job**
   ```bash
   hf-downloader master "microsoft/DialoGPT-medium" -o ./downloads
   # Returns: Job ID: abc123...
   ```

3. **Start workers on multiple machines**
   ```bash
   # Machine 1
   hf-downloader worker --worker-id worker-01
   
   # Machine 2
   hf-downloader worker --worker-id worker-02
   
   # Machine N
   hf-downloader worker --worker-id worker-N
   ```

4. **Monitor progress**
   ```bash
   hf-downloader status abc123 --watch
   ```

## Error Handling and Retry Logic

- **Automatic Retry**: Failed tasks are automatically retried up to 3 times
- **Exponential Backoff**: Retry delays increase with each attempt
- **Permanent Failure**: Tasks exceeding retry limits are moved to failed queue
- **Worker Recovery**: Workers automatically reconnect and resume after network issues
- **Graceful Shutdown**: Workers complete current downloads before shutting down

## Performance Considerations

- **Scaling**: Add more workers to increase download throughput
- **File Size**: Large files may bottleneck individual workers
- **Network**: Total throughput limited by aggregate network bandwidth
- **Redis**: Single Redis instance can handle hundreds of workers
- **Storage**: Ensure sufficient disk space on worker machines

## Development

### Project Structure

```
distributed_downloader/
├── __init__.py          # Package initialization
├── models.py            # Data models (Pydantic)
├── redis_client.py      # Redis operations
├── master.py            # Master node implementation
├── worker.py            # Worker node implementation
└── cli.py              # Command-line interface
```

### Running Tests

```bash
# Install test dependencies
uv add pytest pytest-asyncio

# Run tests
uv run pytest
```

## Troubleshooting

### Common Issues

1. **Redis Connection Failed**
   - Check Redis server is running
   - Verify host/port configuration
   - Check firewall rules

2. **No Workers Active**
   - Ensure workers are started with correct Redis config
   - Check worker logs for errors
   - Verify network connectivity

3. **Downloads Failing**
   - Check Hugging Face authentication
   - Verify dataset exists and is accessible
   - Check disk space on worker machines

4. **Slow Performance**
   - Add more worker machines
   - Check network bandwidth
   - Monitor Redis performance

### Logs

Workers and masters log detailed information. Increase log level for debugging:

```bash
hf-downloader --log-level DEBUG worker
```

## License

[License information here]