"""Command-line interface for the distributed downloader."""

import sys
from typing import Optional
from pathlib import Path

import click
from loguru import logger

from .config import get_config_manager, ConfigManager
from .redis_client import RedisClient
from .master import MasterNode
from .worker import WorkerNode


@click.group()
@click.option('--config', '-c', help='Configuration file path')
@click.option('--redis-host', default=None, help='Redis server host')
@click.option('--redis-port', default=None, help='Redis server port')
@click.option('--redis-password', default=None, help='Redis server password')
@click.option('--redis-username', default=None, help='Redis server username (Redis 6.0+ ACL)')
@click.option('--log-level', default=None, help='Log level (DEBUG, INFO, WARNING, ERROR)')
@click.pass_context
def main(ctx, config, redis_host, redis_port, redis_password, redis_username, log_level):
    """Distributed Hugging Face Downloader."""
    # Initialize configuration manager
    config_manager = get_config_manager(config)
    
    # Update config with CLI args
    config_manager.update_from_cli_args(
        redis_host=redis_host,
        redis_port=redis_port,
        redis_password=redis_password,
        redis_username=redis_username,
        log_level=log_level
    )
    
    app_config = config_manager.get_config()
    
    # Configure logging
    logger.remove()
    logger.add(
        sys.stderr,
        level=app_config.log_level.upper(),
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    
    # Store config in context
    ctx.ensure_object(dict)
    ctx.obj['config'] = app_config
    ctx.obj['config_manager'] = config_manager


@main.command()
@click.argument('dataset_name')
@click.option('--output-dir', '-o', help='Output directory for downloads')
@click.pass_context
def master(ctx, dataset_name, output_dir):
    """Start master node to create download jobs."""
    app_config = ctx.obj['config']
    
    # Use output_dir from CLI or config
    final_output_dir = output_dir or app_config.output_dir
    
    redis_client = RedisClient(
        host=app_config.redis.host,
        port=app_config.redis.port,
        password=app_config.redis.password,
        username=app_config.redis.username,
        db=app_config.redis.db
    )
    
    # Test Redis connection
    if not redis_client.ping():
        logger.error("Cannot connect to Redis server")
        sys.exit(1)
    
    master_node = MasterNode(redis_client, app_config.huggingface)
    
    logger.info(f"Creating download job for dataset: {dataset_name}")
    job_id = master_node.create_download_job(dataset_name, final_output_dir)
    
    if job_id:
        logger.info(f"Successfully created job {job_id}")
        logger.info("Use 'hf-downloader status <job_id>' to monitor progress")
        logger.info("Start workers with 'hf-downloader worker' on each machine")
        print(f"Job ID: {job_id}")
    else:
        logger.error("Failed to create download job")
        sys.exit(1)


@main.command()
@click.argument('datasets')
@click.option('--output-dir', '-o', help='Output directory for downloads')
@click.option('--from-file', '-f', is_flag=True, help='Read dataset names from file (one per line)')
@click.pass_context
def batch_master(ctx, datasets, output_dir, from_file):
    """Create multiple download jobs in parallel."""
    app_config = ctx.obj['config']
    
    # Parse dataset names
    if from_file:
        try:
            with open(datasets, 'r') as f:
                dataset_names = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logger.error(f"Failed to read datasets file {datasets}: {e}")
            sys.exit(1)
    else:
        # Parse comma-separated list
        dataset_names = [name.strip() for name in datasets.split(',') if name.strip()]
    
    if not dataset_names:
        logger.error("No datasets provided")
        sys.exit(1)
    
    # Use output_dir from CLI or config
    final_output_dir = output_dir or app_config.output_dir
    
    redis_client = RedisClient(
        host=app_config.redis.host,
        port=app_config.redis.port,
        password=app_config.redis.password,
        username=app_config.redis.username,
        db=app_config.redis.db
    )
    
    # Test Redis connection
    if not redis_client.ping():
        logger.error("Cannot connect to Redis server")
        sys.exit(1)
    
    master_node = MasterNode(redis_client, app_config.huggingface)
    
    logger.info(f"Creating batch download jobs for {len(dataset_names)} datasets")
    job_ids = master_node.create_batch_download_jobs(dataset_names, final_output_dir)
    
    # Report results
    successful_jobs = [(job_id, dataset_names[i]) for i, job_id in enumerate(job_ids) if job_id is not None]
    failed_datasets = [dataset_names[i] for i, job_id in enumerate(job_ids) if job_id is None]
    
    if successful_jobs:
        logger.info(f"Successfully created {len(successful_jobs)} jobs:")
        for job_id, dataset_name in successful_jobs:
            print(f"  {dataset_name}: {job_id}")
        
        logger.info("Use 'hf-downloader repos' to list all repositories")
        logger.info("Use 'hf-downloader status <repo-name>' to monitor progress")
        logger.info("Start workers with 'hf-downloader worker' on each machine")
    
    if failed_datasets:
        logger.error(f"Failed to create jobs for {len(failed_datasets)} datasets:")
        for dataset_name in failed_datasets:
            print(f"  {dataset_name}")
    
    if not successful_jobs:
        logger.error("No jobs were created successfully")
        sys.exit(1)


@main.command()
@click.option('--worker-id', help='Worker ID (auto-generated if not provided)')
@click.pass_context
def worker(ctx, worker_id):
    """Start worker node to process download tasks."""
    app_config = ctx.obj['config']
    
    redis_client = RedisClient(
        host=app_config.redis.host,
        port=app_config.redis.port,
        password=app_config.redis.password,
        username=app_config.redis.username,
        db=app_config.redis.db
    )
    
    # Test Redis connection
    if not redis_client.ping():
        logger.error("Cannot connect to Redis server")
        sys.exit(1)
    
    worker_node = WorkerNode(redis_client, app_config, worker_id)
    
    try:
        worker_node.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        worker_node.stop()


@main.command()
@click.argument('identifier', required=False)
@click.option('--watch', '-w', is_flag=True, help='Watch status updates')
@click.option('--interval', default=5, help='Watch interval in seconds')
@click.pass_context
def status(ctx, identifier, watch, interval):
    """Show status of jobs, workers, and queues.
    
    IDENTIFIER can be either a job ID or repository name.
    """
    import time
    from datetime import datetime
    
    app_config = ctx.obj['config']
    
    redis_client = RedisClient(
        host=app_config.redis.host,
        port=app_config.redis.port,
        password=app_config.redis.password,
        username=app_config.redis.username,
        db=app_config.redis.db
    )
    
    # Test Redis connection
    if not redis_client.ping():
        logger.error("Cannot connect to Redis server")
        sys.exit(1)
    
    master_node = MasterNode(redis_client, app_config.huggingface)
    
    def show_status():
        click.clear()
        click.echo(f"Distributed Downloader Status - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        click.echo("=" * 60)
        
        # Queue stats
        queue_stats = master_node.get_queue_stats()
        click.echo(f"Queue Statistics:")
        click.echo(f"  Pending tasks: {queue_stats['pending_tasks']}")
        click.echo(f"  Failed tasks: {queue_stats['failed_tasks']}")
        click.echo(f"  Active workers: {queue_stats['active_workers']}")
        click.echo()
        
        # Job/repo status if specified
        if identifier:
            # First try as job ID
            job_status = master_node.get_job_status(identifier)
            
            # If not found, try as repository name
            if not job_status:
                job_id = redis_client.get_job_by_repo(identifier)
                if job_id:
                    job_status = master_node.get_job_status(job_id)
            
            if job_status:
                click.echo(f"Repository: {job_status.dataset_name}")
                click.echo(f"  Job ID: {job_status.job_id}")
                click.echo(f"  Status: {job_status.status}")
                click.echo(f"  Progress: {job_status.completed_files}/{job_status.total_files} files")
                if job_status.failed_files > 0:
                    click.echo(f"  Failed: {job_status.failed_files} files")
                
                progress_pct = (job_status.completed_files / job_status.total_files) * 100 if job_status.total_files > 0 else 0
                click.echo(f"  Progress: {progress_pct:.1f}%")
            else:
                click.echo(f"Repository or job '{identifier}' not found")
            click.echo()
        
        # Active workers
        active_workers = master_node.list_active_workers()
        if active_workers:
            click.echo(f"Active Workers ({len(active_workers)}):")
            for worker_id in active_workers:
                click.echo(f"  - {worker_id}")
        else:
            click.echo("No active workers")
    
    if watch:
        try:
            while True:
                show_status()
                time.sleep(interval)
        except KeyboardInterrupt:
            pass
    else:
        show_status()


@main.command()
@click.pass_context
def repos(ctx):
    """List all repositories and their status."""
    app_config = ctx.obj['config']
    
    redis_client = RedisClient(
        host=app_config.redis.host,
        port=app_config.redis.port,
        password=app_config.redis.password,
        username=app_config.redis.username,
        db=app_config.redis.db
    )
    
    # Test Redis connection
    if not redis_client.ping():
        logger.error("Cannot connect to Redis server")
        sys.exit(1)
    
    repositories = redis_client.list_repositories()
    
    if repositories:
        click.echo(f"Repositories ({len(repositories)}):")
        click.echo("=" * 60)
        
        for repo_name in sorted(repositories):
            repo_status = redis_client.get_repo_status(repo_name)
            if repo_status:
                status = repo_status.get('status', 'UNKNOWN')
                total_files = int(repo_status.get('total_files', 0))
                completed_files = int(repo_status.get('completed_files', 0))
                failed_files = int(repo_status.get('failed_files', 0))
                
                progress_pct = (completed_files / total_files) * 100 if total_files > 0 else 0
                
                click.echo(f"  {repo_name}")
                click.echo(f"    Status: {status}")
                click.echo(f"    Progress: {completed_files}/{total_files} files ({progress_pct:.1f}%)")
                if failed_files > 0:
                    click.echo(f"    Failed: {failed_files} files")
                click.echo()
            else:
                click.echo(f"  {repo_name} - Status unknown")
                click.echo()
    else:
        click.echo("No repositories found")


@main.command()
@click.pass_context
def workers(ctx):
    """List active workers."""
    app_config = ctx.obj['config']
    
    redis_client = RedisClient(
        host=app_config.redis.host,
        port=app_config.redis.port,
        password=app_config.redis.password,
        username=app_config.redis.username,
        db=app_config.redis.db
    )
    
    # Test Redis connection
    if not redis_client.ping():
        logger.error("Cannot connect to Redis server")
        sys.exit(1)
    
    active_workers = redis_client.get_active_workers()
    
    if active_workers:
        click.echo(f"Active Workers ({len(active_workers)}):")
        for worker_id in active_workers:
            click.echo(f"  {worker_id}")
    else:
        click.echo("No active workers")


@main.command()
@click.pass_context
def queue(ctx):
    """Show queue statistics."""
    app_config = ctx.obj['config']
    
    redis_client = RedisClient(
        host=app_config.redis.host,
        port=app_config.redis.port,
        password=app_config.redis.password,
        username=app_config.redis.username,
        db=app_config.redis.db
    )
    
    # Test Redis connection
    if not redis_client.ping():
        logger.error("Cannot connect to Redis server")
        sys.exit(1)
    
    pending = redis_client.get_queue_size()
    failed = redis_client.get_failed_queue_size()
    active_workers = len(redis_client.get_active_workers())
    
    click.echo("Queue Statistics:")
    click.echo(f"  Pending tasks: {pending}")
    click.echo(f"  Failed tasks: {failed}")
    click.echo(f"  Active workers: {active_workers}")


@main.command()
@click.option('--output', '-o', default='config.ini', help='Output file path')
def init_config(output):
    """Create a sample configuration file."""
    config_manager = ConfigManager()
    config_manager.create_sample_config(output)
    click.echo(f"Created sample configuration file: {output}")
    click.echo("Edit the file and uncomment the settings you want to use.")


if __name__ == '__main__':
    main()