# sjq

Simple job queue based on Redis.

`sjq` is a Python-based job queue system designed for creating dynamic data processing pipelines. It uses Redis for message queuing and provides a command-line interface for managing and executing jobs.

## Goals

The primary goals of `sjq` are to:

- Enable easy creation of dynamic, multi-stage data processing pipelines.
- Ensure each job runs in its own isolated process. This facilitates reproducibility and simplifies monitoring.
  - During development, jobs can be easily re-run and debugged using the data stored in the `job_data` directory.
- Provide fault-tolerant and persistent queues. Jobs are not lost, allowing for incremental pipeline implementation without fear of data loss.
- Implement topic-specific queues for organizing jobs.
- Allow topic-specific Python scripts to handle job processing and determine the next step in a pipeline.
- Ensure reproducibility by capturing execution metadata for each job.
- Provide a basic framework for error handling.

## Architecture Overview

The system consists of the following main components:

1.  **Redis Queues**: Each topic has a dedicated queue in Redis. Jobs are stored as JSON messages.
2.  **`sjq` CLI / Main Consumer**: A Python application that:
  - Monitors Redis queues for jobs.
  - Uses Redis locks to ensure a topic is processed by only one worker at a time.
  - Executes topic-specific handler scripts (`topics/<topic_name>.py`) using `uv run`.
  - Manages job lifecycle, including input/output file handling and metadata logging.
  - Enqueues jobs for the next stage based on the output of the current topic handler.
3.  **Topic Handlers**: Python scripts located in the `topics/` directory. Each script:
  - Is decorated with `@sjq.job`.
  - Receives input data via a JSON file.
  - Performs its specific data transformation.
  - Outputs a JSON file indicating the `next_topic` (if any) and the `data` for the next job.
  - Topic handlers are executed as isolated processes.

### Data Flow

1.  A job is created using `sjq create <topic> <input_json_file_or_string>`. This places a job message on the `incoming:<topic>` Redis list and stores job data.
2.  A `sjq worker` process monitors specified topics.
3.  When a job ID is retrieved from an `incoming:<topic>` list, it's moved to a `processing:<topic>` list.
4.  The worker prepares an input file (`job_data/<job_id>-input.json`) for the topic handler.
5.  The corresponding `topics/<topic_name>.py` script is executed.
6.  The topic handler reads the input file, processes the data, and writes an output file (`job_data/<job_id>-output.json`).
7.  The worker reads the output file. If `next_topic` is specified, a new job is created for that topic with the `data` from the output.
8.  Metadata about the job execution (start time, end time, exit code, stdout, stderr) is saved to `job_data/<job_id>-metadata.json`.
9.  The job ID is removed from the `processing:<topic>` list and its data is deleted from Redis upon successful completion. Failed jobs are moved to a `failed:<topic>` list.

## Project Structure

`sjq` is intended to be used as a library within your project. A typical project using `sjq` will have the following structure:

```
your_project_root/
├── config.json         # sjq configuration (e.g., Redis connection)
├── topics/             # Directory for topic-specific handler scripts
│   ├── topic1.py
│   ├── topic2.py
│   └── ...
└── job_data/           # Directory where sjq stores job inputs, outputs, and metadata (created automatically)
```

## Usage

### Running Workers

To process jobs, you run `sjq worker` in your project's root directory. This command starts a daemon process that listens for and processes jobs.

```bash
sjq worker
```

You can specify which topics a worker should handle:

```bash
sjq worker --topics topic1 topic3
```

- Only one worker can listen per topic at any given time due to a locking mechanism.
- Workers for different topics do not need to run on the same machine. This allows you to run workers for resource-intensive topics (e.g., requiring GPUs or significant RAM) on appropriately equipped machines.

### Creating Jobs

The `sjq create` command is primarily for manually initiating a pipeline, often during development or for testing.

```bash
sjq create <topic_name> '{"key": "initial_data"}'
```

or

```bash
sjq create <topic_name> /path/to/input_data.json
```

In a production environment, you would typically trigger pipelines programmatically, for example, in response to webhooks, by connecting to Redis directly and enqueuing jobs.

## Command-Line Interface

`sjq` provides a command-line interface for interacting with the job queue:

- **`sjq create <topic> <input_file_or_json_string> [--parent-job-id <id>]`**: Creates a new job for the specified topic.
- **`sjq worker [--topics <topic1> <topic2> ...]`**: Starts a worker process to monitor and process jobs from the specified topics (or all topics if none are specified).
- **`sjq unlock [<topic1> <topic2> ...]`**: Releases locks for the specified topics (or all topics if none are specified). This is useful if a worker crashed and did not release its lock.
- **`sjq retry [<topic1> <topic2> ...]`**: Moves all jobs from the _failed_ queue back into the _incoming_ queue.
- **`sjq dev <topic> [index]`**: Runs the job handler for `<topic>` at `index` (defaults to 1) without creating follow-up jobs.

Configuration, such as Redis connection details, is managed via a `config.json` file in the root of the project using `sjq`.
