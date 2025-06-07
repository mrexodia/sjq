import os
import sys
import json
import time
import argparse
import subprocess

from datetime import datetime
from typing import TypedDict, Optional
from redis import Redis
from glob import glob

class JobMessage(TypedDict):
    job_id: str
    parent_job_id: Optional[str]
    data: dict

def create_job(redis: Redis, topic: str, job_data: dict, parent_job_id: Optional[str] = None) -> str:
    """Creates a job and returns a unique job_id."""

    # Create a unique job ID from the current time
    while True:
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S.%fZ")
        job_id = f"{timestamp}-{topic}"
        if redis.get(f"job_data:{job_id}") is None:
            break
        time.sleep(0.002)

    job_message: JobMessage = {
        "job_id": job_id,
        "parent_job_id": parent_job_id,
        "data": job_data,
    }
    redis.set(f"job_data:{job_id}", json.dumps(job_message)) # Store job data
    redis.rpush(f"incoming:{topic}", job_id) # Add to tail (right) of incoming queue

    return job_id

def process_job(redis: Redis, job_id: str, topic: str):
    # Get job data from Redis
    job_data = redis.get(f"job_data:{job_id}")
    if job_data is None:
        raise ValueError(f"No data found for job {job_id}")

    job_message: JobMessage = json.loads(job_data)
    data = job_message["data"]
    parent_job_id = job_message.get("parent_job_id")
    start_time = datetime.now()

    # Write input data to file
    input_file = f"job_data/{job_id}-input.json"
    with open(input_file, "w") as f:
        json.dump(data, f, indent=2)

    # Execute topic handler
    topic_script = f"topics/{topic}.py"
    output_file = f"job_data/{job_id}-output.json"
    print(f"Running job: {job_id}")
    process = subprocess.run(
        ["uv", "run", topic_script, "--input", input_file, "--output", output_file],
        capture_output=True,
        text=True
    )

    # Create metadata
    now = datetime.now()
    elapsed_time = (now - start_time).total_seconds()
    metadata = {
        "job_id": job_id,
        "parent_job_id": parent_job_id,
        "topic": topic,
        "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_time": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "elapsed_time": elapsed_time,
        "exit_code": process.returncode,
        "stdout": process.stdout,
        "stderr": process.stderr,
    }
    with open(f"job_data/{job_id}-metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    if process.returncode != 0:
        raise ValueError(f"Job {job_id} failed with exit code {process.returncode}")

    print(f"Successful job: {job_id} ({elapsed_time:.2f} seconds)")

    # Process output and enqueue next job if needed
    with open(output_file, "r") as f:
        output = json.load(f)
    next_topic = output.get("next_topic")
    if next_topic:
        next_job_id = create_job(redis, next_topic, output["data"], job_id)
        print(f"Created next job: {job_id} -> {next_job_id}")

def process_topic(redis: Redis, topic: str):
    """Process jobs from the given topic."""
    # Implement a reliable FIFO queue by moving jobs from the head of the incoming
    # list to the tail of the processing list.
    # Reference: https://redis.io/glossary/redis-queue/
    # Without timeout Ctrl+C will not work
    # https://github.com/redis/redis-py/issues/1305#issuecomment-597305775

    job_id = redis.blmove(
        f"incoming:{topic}",
        f"processing:{topic}",
        src="LEFT", # head
        dest="RIGHT", # tail
        timeout=1,
    )
    if job_id is None:
        return

    try:
        process_job(redis, job_id, topic)

        # Remove from processing queue and delete job data
        redis.lrem(
            f"processing:{topic}",
            -1, # tail -> head (remove from the right)
            job_id
        )
        redis.delete(f"job_data:{job_id}")
    except Exception as e:
        print(f"Error processing job: {e}")
        # Add the job to the failed queue and remove it from processing
        redis.rpush(f"failed:{topic}", job_id)
        redis.lrem(
            f"processing:{topic}",
            -1, # tail -> head (remove from the right)
            job_id
        )

def lock_topics(redis: Redis, topics: list[str]):
    """Acquire locks for the given topics."""
    success = True
    for topic in topics:
        value = redis.set(f"lock:{topic}", os.getpid(), nx=True, get=True)
        if value is not None:
            print(f"Failed to acquire lock for topic: {topic} (pid={value})")
            success = False
    if not success:
        sys.exit(1)

def unlock_topics(redis: Redis, topics: list[str]):
    """Release locks for the given topics."""
    print(f"Releasing locks for topics: {', '.join(topics)}")
    for topic in topics:
        redis.delete(f"lock:{topic}")

def recover_topics(redis: Redis, topics: list[str]):
    """Recover processing jobs from the given topics."""
    for topic in topics:
        count = redis.llen(f"processing:{topic}")
        if count == 0:
            continue
        print(f"Recovering {count} jobs from topic: {topic}")
        for _ in range(count):
            # Move the failed jobs in front of the processing queue
            job_id = redis.lmove(
                f"processing:{topic}",
                f"incoming:{topic}",
                src="RIGHT", # tail
                dest="LEFT", # head
            )
            if job_id is None:
                break
            print(f"  {job_id}")

def process_topics(redis: Redis, topics: list[str]):
    """Process jobs from the given topics."""
    lock_topics(redis, topics)
    recover_topics(redis, topics)

    print(f"Processing topics: {', '.join(topics)}")
    try:
        while True:
            for topic in topics:
                process_topic(redis, topic)
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        unlock_topics(redis, topics)

def enumerate_topics(filter: list[str]) -> list[str]:
    topics = []
    for file in sorted(glob("*.py", root_dir="topics")):
        topic, _ = os.path.splitext(file)
        topics.append(topic)
    if not filter:
        return topics

    for topic in filter:
        if topic not in topics:
            print(f"Topic {topic} not found")
            sys.exit(1)
    return filter

def main():
    os.makedirs("job_data", exist_ok=True)
    try:
        with open("config.json", "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        print("config.json not found")
        sys.exit(1)

    # Create Redis client
    redis = Redis(
        **config["redis"],
        socket_timeout=30,
        decode_responses=True
    )
    if not redis.ping():
        print("Redis connection failed")
        sys.exit(1)

    # Create main parser
    main_parser = argparse.ArgumentParser(description="Simple job queue.")
    command_parser = main_parser.add_subparsers(dest="command", help="Available commands", required=True)

    # Worker subcommand
    worker_parser = command_parser.add_parser("worker", help="Start worker process")
    worker_parser.add_argument("--topics", nargs="*", default=None, help="Topics to monitor (defaults to all in this workspace)")

    # Create subcommand
    create_parser = command_parser.add_parser("create", help="Create a new job")
    create_parser.add_argument("topic", help="Topic to send job to")
    create_parser.add_argument("input", help="Input JSON file or JSON string")
    create_parser.add_argument("--parent-job-id", help="Parent job ID if this is a child job", required=False)

    # Unlock subcommand
    unlock_parser = command_parser.add_parser("unlock", help="Unlock topics")
    unlock_parser.add_argument("topics", nargs="*", help="Topics to unlock (defaults to all in this workspace)")

    # Parse arguments
    args = main_parser.parse_args()
    if args.command == "worker":
        topics = enumerate_topics(args.topics)
        process_topics(redis, topics)
    elif args.command == "create":
        # Try to parse as JSON string first, then as file path
        try:
            job_data = json.loads(args.input)
        except json.JSONDecodeError:
            with open(args.input, "r") as f:
                job_data = json.load(f)

        # Create and enqueue job using redis config from config file
        job_id = create_job(redis, args.topic, job_data, args.parent_job_id)
        print(f"Created job: {job_id}")
    elif args.command == "unlock":
        topics = enumerate_topics(args.topics)
        unlock_topics(redis, topics)
