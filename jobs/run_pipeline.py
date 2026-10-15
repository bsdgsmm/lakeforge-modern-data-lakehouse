"""spark-submit entry point for the incremental pipeline.

    spark-submit --master "local[4]" jobs/run_pipeline.py [--full-refresh] [--reprocess DATE ...]

Configuration is read from the LAKEFORGE_* environment variables.
"""

import sys

from lakeforge.cli import main

if __name__ == "__main__":
    sys.exit(main(["run", *sys.argv[1:]]))
