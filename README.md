# Distributed Hugging Face Downloader

## Overview

Due to bandwidth limitations imposed by proxies on a single machine, even with multi-threading it's difficult to exceed the maximum download speed (1 MB/s). To overcome the bandwidth constraints of a single machine, we need a distributed downloader.

The system works as follows: a master node retrieves a specified Hugging Face dataset name, fetches all files associated with that dataset, and publishes them to a Redis message queue. Each machine acts as a worker, consuming tasks from Redis. Based on the file names specified in each task, workers use the Hugging Face Python SDK to download the corresponding files.

In theory, if the number of machines matches the number of files, the total download speed for the entire repository is only limited by the size of individual files.

## Features

- **Distributed Architecture**: Scale downloads across multiple machines
- **Redis Message Queue**: Reliable task distribution and coordination
- **Repository Management**: Hierarchical task organization (repository → job → tasks)
- **Batch Job Creation**: Create multiple download jobs in parallel for different repositories
- **File Integrity Verification**: Mandatory verification with automatic re-download of corrupted files
- **3-Retry System**: Automatic retry with proper failure handling for all failed downloads
- **Configuration Management**: Flexible config files, environment variables, and CLI options
- **NAS Aggregation**: Automatically copy files to centralized NAS storage
- **SSL Bypass**: Optional SSL certificate verification bypass for corporate environments
- **Progress Monitoring**: Real-time status updates and progress tracking by repository
- **Graceful Shutdown**: Workers handle interruptions gracefully and can be stopped immediately
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
   - Copy files to NAS storage (if configured)

3. **Redis Message Queue** (`RedisClient`)
   - Task queue management with username/password authentication support
   - Worker coordination and heartbeat tracking
   - Job status and progress tracking
   - Failed task retry management

4. **Configuration System** (`ConfigManager`)
   - File-based configuration with automatic discovery
   - Environment variable overrides
   - CLI argument integration
   - Validation and error handling

5. **NAS Aggregator** (`NASAggregator`)
   - Automatic file copying to centralized storage
   - Configurable directory structure preservation
   - Asynchronous copying for performance
   - Smart deduplication and error handling

### Data Models

- **DownloadTask**: Individual file download task
- **WorkerStatus**: Worker health and activity tracking  
- **JobStatus**: Overall download job progress
- **TaskStatus**: Enumeration of task states (pending, in_progress, completed, failed, retrying)
- **AppConfig**: Application configuration settings
- **RedisConfig**: Redis connection settings
- **HuggingFaceConfig**: Hugging Face authentication and caching
- **NASConfig**: NAS aggregation settings

## Installation

### Prerequisites

- Python 3.10+
- Redis server (supports Redis 6.0+ ACL authentication)
- UV package manager

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd distributed_downloader

# Install dependencies with UV
uv sync

# Or with native TLS support
uv --native-tls sync

# Or install in development mode
uv pip install -e .

# For high-speed downloads (optional but recommended)
uv sync --extra hf_transfer
```

### Redis Setup

```bash
# Install and start Redis (Ubuntu/Debian)
sudo apt-get install redis-server
sudo systemctl start redis-server

# Or using Docker
docker run -d -p 6379:6379 redis:latest

# For Redis 6.0+ with ACL authentication
redis-cli
> ACL SETUSER myuser +@all ~* &mypassword
```

## Configuration

### Configuration Files

The system supports multiple configuration methods with the following priority order:
1. **CLI arguments** (highest priority)
2. **Environment variables**
3. **Configuration files**
4. **Default values** (lowest priority)

#### Create Configuration File

```bash
# Generate a sample configuration file
hf-downloader init-config

# Or create manually
hf-downloader init-config --output my-config.ini
```

#### Configuration File Locations

The system automatically searches for config files in this order:
1. `config.ini` (current directory)
2. `distributed_downloader.ini`
3. `~/.config/distributed_downloader/config.ini`
4. `~/.distributed_downloader.ini`
5. `/etc/distributed_downloader/config.ini`

#### Sample Configuration (`config.ini`)

```ini
[redis]
host = localhost
port = 6379
password = your_redis_password
username = your_redis_username  # For Redis 6.0+ ACL
db = 0

[huggingface]
token = your_huggingface_token
cache_dir = /path/to/cache/dir
disable_ssl_verify = false
enable_hf_transfer = true
endpoint = http://hf-mirror.com

[nas]
enabled = true
path = /mnt/nas/huggingface-datasets
copy_after_download = true
preserve_structure = true

[app]
log_level = INFO
output_dir = /path/to/downloads
```

### Environment Variables

```bash
# Redis settings
export REDIS_HOST=redis.example.com
export REDIS_PASSWORD=mypassword
export REDIS_USERNAME=myuser

# Hugging Face settings  
export HF_TOKEN=your_token_here
export HF_DISABLE_SSL_VERIFY=true
export HF_HUB_ENABLE_HF_TRANSFER=1
export HF_ENDPOINT=http://hf-mirror.com

# NAS settings
export NAS_ENABLED=true
export NAS_PATH=/mnt/nas/datasets

# Application settings
export LOG_LEVEL=DEBUG
export OUTPUT_DIR=/downloads
```

## Usage

### Command Line Interface

The system provides a CLI tool `hf-downloader` with the following commands:

#### Start a Download Job (Master)

```bash
# Create a download job for a single dataset
hf-downloader master "microsoft/DialoGPT-medium"

# Using configuration file
hf-downloader --config my-config.ini master "nvidia/Llama-Nemotron-VLM-Dataset-v1"

# Specify output directory
hf-downloader master "microsoft/DialoGPT-medium" --output-dir ./my-downloads

# With Redis authentication
hf-downloader --redis-password mypass --redis-username myuser master "dataset-name"
```

#### Batch Job Creation (Multiple Repositories)

```bash
# Create multiple download jobs in parallel
hf-downloader batch-master "microsoft/DialoGPT-medium,nvidia/Llama-Nemotron-VLM-Dataset-v1,openai/whisper-large-v3"

# From a file (one repository per line)
echo "microsoft/DialoGPT-medium" > datasets.txt
echo "nvidia/Llama-Nemotron-VLM-Dataset-v1" >> datasets.txt
hf-downloader batch-master datasets.txt --from-file

# With custom output directory
hf-downloader batch-master "repo1,repo2,repo3" --output-dir ./batch-downloads
```

#### Start Worker Nodes

```bash
# Start a worker (run on each machine)
hf-downloader worker

# Start worker with custom configuration
hf-downloader --config /path/to/config.ini worker --worker-id "worker-01"

# With SSL bypass for corporate environments
hf-downloader --disable-ssl-verify worker

# With custom Redis settings
hf-downloader --redis-host 192.168.1.100 --redis-password mypass worker
```

#### Monitor Progress

```bash
# Show overall system status
hf-downloader status

# Monitor specific job by job ID
hf-downloader status <job-id>

# Monitor repository by name
hf-downloader status "microsoft/DialoGPT-medium"

# Watch status with live updates
hf-downloader status "nvidia/Llama-Nemotron-VLM-Dataset-v1" --watch

# List all repositories and their progress
hf-downloader repos

# Check queue statistics
hf-downloader queue

# List active workers
hf-downloader workers
```

## Advanced Features

### NAS Aggregation

The NAS aggregation feature automatically copies downloaded files to a centralized storage location:

```ini
[nas]
enabled = true
path = /mnt/nas/huggingface-datasets
copy_after_download = true      # Copy files after successful download
preserve_structure = true       # Keep original directory structure
```

**Directory Structure Options:**
- `preserve_structure = true`: `/nas/dataset_name/path/to/file.txt`
- `preserve_structure = false`: `/nas/dataset_name_file.txt`

**Features:**
- Asynchronous copying (doesn't slow down downloads)
- Smart deduplication (skips existing files with matching size)
- Automatic directory creation
- Error tolerance (NAS failures don't break downloads)

### SSL Certificate Bypass

For corporate environments with proxy servers or custom certificates:

```ini
[huggingface]
disable_ssl_verify = true
```

Or via environment variable:
```bash
export HF_DISABLE_SSL_VERIFY=true
```

This disables SSL certificate verification for:
- Hugging Face Hub API calls
- Direct HTTP file downloads
- Suppresses urllib3 SSL warnings

### High-Speed Downloads with hf_transfer

For high-bandwidth environments, enable `hf_transfer` for significantly faster downloads:

```ini
[huggingface]
enable_hf_transfer = true
```

Or via environment variable:
```bash
export HF_HUB_ENABLE_HF_TRANSFER=1
```

**Benefits:**
- **Rust-based library** for maximum performance
- **Multi-threaded transfers** with optimal bandwidth utilization  
- **Automatic fallback** to standard downloads if unavailable
- **No configuration changes** required - works transparently

**Requirements:**
- High-bandwidth network connection (>100 Mbps recommended)
- The `hf_transfer` library is automatically installed with `huggingface-hub[hf_transfer]`

### Mirror Sites and Custom Endpoints

For users in regions with limited access to Hugging Face or those wanting to use mirror sites:

```ini
[huggingface]
endpoint = http://hf-mirror.com
```

Or via environment variable:
```bash
export HF_ENDPOINT=http://hf-mirror.com
```

**Popular Mirror Sites:**
- **China**: `http://hf-mirror.com`
- **Custom Enterprise**: `https://your-company-hf-mirror.com`
- **Default**: `https://huggingface.co` (if not specified)

**Features:**
- **Automatic URL rewriting**: All download URLs use the specified endpoint
- **API compatibility**: Full compatibility with Hugging Face Hub API
- **Transparent operation**: Works with both `hf_hub_download` and direct HTTP downloads
- **SSL support**: Works with both HTTP and HTTPS endpoints

### Redis Authentication

Supports both traditional and modern Redis authentication:

**Traditional password-only (Redis < 6.0):**
```ini
[redis]
password = your_password
```

**Username + password (Redis 6.0+ ACL):**
```ini
[redis]
username = your_username
password = your_password
```

## Example Workflows

### Basic Setup

1. **Create configuration file**
   ```bash
   hf-downloader init-config
   # Edit config.ini with your settings
   ```

2. **Start Redis server**
   ```bash
   redis-server
   ```

3. **Create download jobs**
   ```bash
   # Single repository
   hf-downloader master "microsoft/DialoGPT-medium" -o ./downloads
   
   # Multiple repositories in parallel
   hf-downloader batch-master "microsoft/DialoGPT-medium,nvidia/Llama-Nemotron-VLM-Dataset-v1,openai/whisper-large-v3"
   ```

4. **Start workers on multiple machines**
   ```bash
   # Machine 1
   hf-downloader worker --worker-id worker-01
   
   # Machine 2
   hf-downloader worker --worker-id worker-02
   
   # Machine N
   hf-downloader worker --worker-id worker-N
   ```

5. **Monitor progress**
   ```bash
   # List all repositories
   hf-downloader repos
   
   # Monitor specific repository
   hf-downloader status "microsoft/DialoGPT-medium" --watch
   
   # Check overall system status
   hf-downloader status
   ```

### Corporate Environment Setup

```bash
# 1. Create config with SSL bypass and authentication
cat > config.ini << EOF
[redis]
host = redis.corporate.com
username = downloader_user
password = secure_password

[huggingface]
token = hf_your_token_here
disable_ssl_verify = true
enable_hf_transfer = true
endpoint = http://hf-mirror.com

[nas]
enabled = true
path = /corporate/nas/ml-datasets
EOF

# 2. Start batch jobs for multiple datasets
hf-downloader --config config.ini batch-master "dataset1,dataset2,dataset3"

# 3. Start workers across the corporate network
hf-downloader --config config.ini worker
```

### China Mirror Setup

```bash
# For users in China using hf-mirror.com
cat > config.ini << EOF
[huggingface]
token = your_hf_token
endpoint = http://hf-mirror.com
enable_hf_transfer = true
disable_ssl_verify = false
EOF

# Start downloading with mirror
hf-downloader --config config.ini batch-master "microsoft/DialoGPT-medium,nvidia/Llama-Nemotron-VLM-Dataset-v1"
hf-downloader --config config.ini worker
```

### High-Performance Setup with NAS

```bash
# Configure for maximum throughput with centralized storage
cat > config.ini << EOF
[huggingface]
enable_hf_transfer = true
endpoint = http://hf-mirror.com

[nas]
enabled = true
path = /high-speed-nas/datasets
copy_after_download = true
preserve_structure = true
delete_after_copy = true

[app]
output_dir = /local-ssd/temp-downloads
EOF

# Workers download to local SSD with hf_transfer, then move to NAS
hf-downloader --config config.ini worker
```

## Repository Management & File Integrity

### Repository-Based Organization

The system now uses a hierarchical approach for better organization:

- **Repository Level**: Each HuggingFace repository gets its own job
- **Job Level**: Contains all tasks for downloading a specific repository
- **Task Level**: Individual file download tasks within a job

### File Integrity Verification

**Mandatory Verification System:**
- **Size Verification**: Files must match expected size exactly
- **Corruption Detection**: Identifies HTML error pages, empty files, and unreadable content
- **Automatic Re-download**: Corrupted or incomplete files are automatically deleted and re-downloaded
- **Read Test**: Ensures downloaded files are actually readable

### 3-Retry System

- **Mandatory Retries**: All failed downloads are automatically retried up to 3 times
- **Comprehensive Failure Handling**: Covers network errors, corruption, size mismatches, and timeouts
- **Smart Retry Logic**: Each retry attempt gets a fresh download attempt
- **Permanent Failure Tracking**: Tasks that fail all 3 retries are marked as permanently failed
- **Worker Recovery**: Workers automatically reconnect and resume after network issues
- **Immediate Shutdown**: Ctrl+C stops workers immediately, cleaning up partial downloads
- **Task Requeuing**: Interrupted tasks are requeued for other workers

## Performance Considerations

- **Scaling**: Add more workers to increase download throughput
- **File Size**: Large files may bottleneck individual workers
- **Network**: Total throughput limited by aggregate network bandwidth
- **Redis**: Single Redis instance can handle hundreds of workers
- **Storage**: Ensure sufficient disk space on worker machines
- **NAS Performance**: Asynchronous copying minimizes download impact

## Development

### Project Structure

```
distributed_downloader/
├── __init__.py          # Package initialization
├── models.py            # Data models (Pydantic)
├── config.py            # Configuration management
├── redis_client.py      # Redis operations
├── master.py            # Master node implementation
├── worker.py            # Worker node implementation
├── ssl_config.py        # SSL bypass configuration
├── nas_aggregator.py    # NAS file aggregation
└── cli.py              # Command-line interface
```

## Troubleshooting

### Common Issues

1. **Redis Connection Failed**
   - Check Redis server is running: `redis-cli ping`
   - Verify host/port configuration in config file
   - Test authentication: `redis-cli -u redis://user:pass@host:port`
   - Check firewall rules

2. **SSL Certificate Errors**
   - Set `disable_ssl_verify = true` in config
   - Or use environment variable: `export HF_DISABLE_SSL_VERIFY=true`
   - Check corporate proxy settings

3. **Hugging Face Authentication**
   - Set token in config: `token = your_hf_token`
   - Or login via CLI: `huggingface-cli login`
   - Verify token has dataset access permissions

4. **Worker Not Responding to Ctrl+C**
   - Updated workers respond immediately to interruption
   - For old processes: `pkill -f "hf-downloader"`
   - Force kill: `pkill -9 -f "hf-downloader"`

5. **NAS Copy Failures**
   - Check NAS path exists and is writable
   - Verify network connectivity to NAS
   - Check disk space on NAS
   - Review worker logs for detailed errors

6. **Configuration Not Loading**
   - Check config file syntax: `cat config.ini`
   - Verify file location in search paths
   - Use `--config` to specify exact path
   - Check file permissions

### Logs

Workers and masters log detailed information. Increase log level for debugging:

```bash
# In config file
[app]
log_level = DEBUG

# Or via CLI
hf-downloader --log-level DEBUG worker

# Or via environment
export LOG_LEVEL=DEBUG
```

### Process Management

```bash
# Find running processes
ps aux | grep hf-downloader

# Kill specific process
kill <PID>

# Kill all downloader processes
pkill -f "hf-downloader"

# Force kill all
pkill -9 -f "hf-downloader"
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.