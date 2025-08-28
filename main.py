"""Simple example of using the distributed downloader programmatically."""

from distributed_downloader.redis_client import RedisClient
from distributed_downloader.master import MasterNode
from distributed_downloader.worker import WorkerNode


def main():
    """Example of using the distributed downloader."""
    print("Distributed Hugging Face Downloader")
    print("Use the CLI for production usage: hf-downloader --help")
    print()
    print("Example CLI usage:")
    print("1. Start master: hf-downloader master 'microsoft/DialoGPT-small'")
    print("2. Start worker: hf-downloader worker")
    print("3. Check status: hf-downloader status")


if __name__ == "__main__":
    main()
