import os
import re
import sys
import json
import time
import argparse
import traceback
import subprocess

from datetime import datetime, timezone
from typing import TypedDict, Optional, NotRequired
from redis import Redis
from glob import glob

class JobMessage(TypedDict):
    job_id: str
    parent_job_id: NotRequired[str]
    data: dict
    attachment: NotRequired[bool]

def create_job(redis: Redis, topic: str, job_data: dict, attachment: Optional[str], parent_job_id: Optional[str] = None) -> str:
    """Creates a job and returns a unique job_id."""

    while True:
        # Create a unique job ID from the current time
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        job_id = f"{timestamp}:{topic}"
        has_attachment = attachment and os.path.exists(attachment)

        # Try to atomically set the job data key only if it does not exist
        job_message: JobMessage = {
            "job_id": job_id,
            "parent_job_id": parent_job_id,
            "data": job_data,
        }
        if has_attachment:
            job_message["attachment"] = True

        if redis.set(f"job_data:{job_id}", json.dumps(job_message), nx=True):
            # Store attachment if provided
            if has_attachment:
                with open(attachment, "rb") as f:
                    attachment_data = f.read()
                redis.set(f"job_attachment:{job_id}", attachment_data)

            # Add to tail (right) of incoming queue
            redis.rpush(f"incoming:{topic}", job_id)
            return job_id

        # Sleep for a short time before trying again
        time.sleep(0.000001)

def process_job(redis: Redis, job_id: str, topic: str):
    # Filename safe job id
    job_id_safe = re.sub(r"[^0-9a-zA-Z-_.]", "-", job_id)

    # Get job data from Redis
    job_data = redis.get(f"job_data:{job_id}")
    if job_data is None:
        raise KeyError(f"No data found for job {job_id}")

    job_message: JobMessage = json.loads(job_data.decode("utf-8"))
    data = job_message["data"]
    parent_job_id = job_message.get("parent_job_id")

    # Write attachment to file if it exists
    if job_message.get("attachment"):
        attachment_data = redis.get(f"job_attachment:{job_id}")
        if attachment_data is None:
            raise KeyError(f"No attachment found for job {job_id}")
        attachment_file = f"job_data/{job_id_safe}-attachment.bin"
        with open(attachment_file, "wb") as f:
            f.write(attachment_data)
    else:
        attachment_file = None

    # Write input data to file
    input_file = f"job_data/{job_id_safe}-input.json"
    with open(input_file, "w") as f:
        json.dump(data, f, indent=2)

    # Execute topic handler
    start_time = datetime.now()
    topic_script = f"topics/{topic}.py"
    if not os.path.exists(topic_script):
        raise FileNotFoundError(f"Topic script not found: {topic_script}")
    output_file = f"job_data/{job_id_safe}-output.json"
    print(f"Running job: {job_id}")

    # Build command arguments
    cmd_args = [sys.executable, topic_script, "--input", input_file, "--output", output_file]
    if attachment_file:
        cmd_args.append("--attachment")
        cmd_args.append(attachment_file)
    process = subprocess.run(
        cmd_args,
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
    with open(f"job_data/{job_id_safe}-metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    if process.returncode != 0:
        raise ChildProcessError(f"Job {job_id} failed with exit code {process.returncode}")

    print(f"Successful job: {job_id} ({elapsed_time:.2f} seconds)")

    # Process output and enqueue next job if needed
    with open(output_file, "r") as f:
        output = json.load(f)
    next_topics = output.get("next_topics", [])
    for next_topic in next_topics:
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
    job_id = job_id.decode("utf-8")

    try:
        process_job(redis, job_id, topic)

        # Remove from processing queue and delete job data
        redis.lrem(
            f"processing:{topic}",
            -1, # tail -> head (remove from the right)
            job_id
        )
        redis.delete(f"job_data:{job_id}")
        # Clean up attachment if it exists
        redis.delete(f"job_binary:{job_id}")
    except Exception as e:
        traceback.print_exc()
        if isinstance(e, KeyError):
            error_queue = "fatal"
            print(f"Job integrity error (fatal): {e}")
        else:
            error_queue = "failed"
            print(f"Job processing error: {e}")
        # Add the job to the failed queue and remove it from processing
        redis.rpush(f"{error_queue}:{topic}", job_id)
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
            value = value.decode("utf-8")
            print(f"Failed to acquire lock for topic: {topic} (pid={value})")
            success = False
    if not success:
        sys.exit(1)

def unlock_topics(redis: Redis, topics: list[str]):
    """Release locks for the given topics."""
    print(f"Releasing locks for topics: {', '.join(topics)}")
    for topic in topics:
        redis.delete(f"lock:{topic}")

def move_queue(redis: Redis, src: str, dst: str):
    count = redis.llen(src)
    for _ in range(count):
        # Move entries in front of the destination queue
        entry: bytes = redis.lmove(
            src,
            dst,
            src="RIGHT", # tail
            dest="LEFT", # head
        )
        if entry is None:
            break
        yield entry

def recover_topics(redis: Redis, topics: list[str]):
    """Recover processing jobs from the given topics."""
    for topic in topics:
        print(f"Recovering jobs from topic: {topic}")
        for job_id in move_queue(redis, f"processing:{topic}", f"incoming:{topic}"):
            print(f"  {job_id.decode('utf-8')}")

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

def retry_topics(redis: Redis, topics: list[str]):
    """Retry failed jobs from the given topics."""
    for topic in topics:
        print(f"Retrying jobs from topic: {topic}")
        for job_id in move_queue(redis, f"failed:{topic}", f"incoming:{topic}"):
            print(f"  {job_id.decode('utf-8')}")

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
    create_parser.add_argument("input", help="Input JSON file, JSON string or attachment file")
    create_parser.add_argument("attachment", help="Attachment file path (optional)", nargs="?", default=None)
    create_parser.add_argument("--parent-job-id", help="Parent job ID if this is a child job", required=False)

    # Unlock subcommand
    unlock_parser = command_parser.add_parser("unlock", help="Unlock topics")
    unlock_parser.add_argument("topics", nargs="*", help="Topics to unlock (defaults to all in this workspace)")

    # Retry subcommand
    retry_parser = command_parser.add_parser("retry", help="Retry failed jobs")
    retry_parser.add_argument("topics", nargs="*", help="Topics to retry (defaults to all in this workspace)")

    # Parse arguments
    args = main_parser.parse_args()
    if args.command == "worker":
        topics = enumerate_topics(args.topics)
        process_topics(redis, topics)
    elif args.command == "create":
        if os.path.exists(args.input):
            _, extension = os.path.splitext(args.input)
            if extension == ".json":
                with open(args.input, "r") as f:
                    job_data = json.load(f)
            else:
                job_data = {}
                args.attachment = args.input
        else:
            try:
                job_data = json.loads(args.input)
            except json.JSONDecodeError:
                print(f"Input is not a valid JSON string or file path: {args.input}")
                sys.exit(1)

        # Create and enqueue job using redis config from config file
        job_id = create_job(redis, args.topic, job_data, args.attachment, args.parent_job_id)
        print(f"Created job: {job_id}")
    elif args.command == "unlock":
        topics = enumerate_topics(args.topics)
        unlock_topics(redis, topics)
    elif args.command == "retry":
        topics = enumerate_topics(args.topics)
        retry_topics(redis, topics)
