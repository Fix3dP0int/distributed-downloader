# Distributed Hugging Face Downloader

## Overview

Due to bandwidth limitations imposed by proxies on a single machine, even with multi-threading it's difficult to exceed the maximum download speed (1 MB/s). To overcome the bandwidth constraints of a single machine, we need a distributed downloader.

The system works as follows: a master node retrieves a specified Hugging Face dataset name, fetches all files associated with that dataset, and publishes them to a Redis message queue. Each machine acts as a worker, consuming tasks from Redis. Based on the file names specified in each task, workers use the Hugging Face Python SDK to download the corresponding files.

In theory, if the number of machines matches the number of files, the total download speed for the entire repository is only limited by the size of individual files.

For design completeness, features such as exception handling, retry mechanisms for failed tasks, and status querying will be implemented. The project will be managed using `uv`.