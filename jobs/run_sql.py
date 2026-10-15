"""spark-submit entry point that runs SQL files against the Gold tables.

spark-submit jobs/run_sql.py sql/01_top_products_by_revenue.sql
"""

import sys

from lakeforge.cli import main

if __name__ == "__main__":
    sys.exit(main(["sql", *sys.argv[1:]]))
