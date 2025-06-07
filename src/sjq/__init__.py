import sys
import json
import argparse
from typing import TypeAlias, Callable

Result: TypeAlias = dict | str | bool | None | int | float | list

JobFunc: TypeAlias = Callable[[dict], tuple[Result, str] | Result]
MainFunc: TypeAlias = Callable[[], None]

# @job decorator function to create a main function that handles the job
def job(func: JobFunc) -> MainFunc:
    def job_main() -> None:
        # Parse the --input and --output arguments
        parser = argparse.ArgumentParser(description="Process a job for this topic")
        parser.add_argument("--input", required=True, help="Path to input file")
        parser.add_argument("--output", required=True, help="Path to output file")
        args = parser.parse_args()

        # Read input file - contains just the raw input data
        try:
            with open(args.input, "r") as f:
                input_data = json.load(f)
        except Exception as e:
            print(f"Error reading input file: {e}", file=sys.stderr)
            sys.exit(1)

        # Process the data
        result = func(input_data)
        if isinstance(result, tuple):
            data, next_topic = result
            output = {
                "next_topic": next_topic,
                "data": data
            }
        else:
            data = result
            output = {
                "next_topic": None,
                "data": data
            }

        # Write output file
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)

    return job_main
