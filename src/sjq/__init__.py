import sys
import json
import inspect
import argparse
from typing import TypeAlias, Callable, Optional

JsonData: TypeAlias = dict | str | bool | None | int | float | list

JobFunc: TypeAlias = Callable[[dict, Optional[str]], tuple[JsonData, list[str]] | tuple[JsonData, str] | JsonData]
MainFunc: TypeAlias = Callable[[], None]

# @job decorator function to create a main function that handles the job
def job(func: JobFunc) -> MainFunc:
    def job_main() -> None:
        # Parse the --input and --output arguments
        parser = argparse.ArgumentParser(description="Process a job for this topic")
        parser.add_argument("--input", required=True, help="Path to input file")
        parser.add_argument("--output", required=True, help="Path to output file")
        parser.add_argument("--attachment", required=False, help="Attachment file path (optional)")
        args = parser.parse_args()

        # Read input file - contains just the raw input data
        try:
            with open(args.input, "r") as f:
                input_data = json.load(f)
        except Exception as e:
            print(f"Error reading input file: {e}", file=sys.stderr)
            sys.exit(1)

        # Process the data
        signature = inspect.signature(func)
        if len(signature.parameters) == 2:
            result = func(input_data, args.attachment)
        else:
            result = func(input_data)
        if isinstance(result, tuple):
            data, next_topics = result
            if isinstance(next_topics, str):
                next_topics = [next_topics]
            elif isinstance(next_topics, list) or next_topics is None:
                pass
            else:
                print(f"Invalid type for next_topics: {type(next_topics)}", file=sys.stderr)
                sys.exit(1)
            output = {
                "next_topics": next_topics,
                "data": data,
            }
        else:
            output = {
                "next_topics": [],
                "data": result,
            }

        # Write output file
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)

    return job_main
