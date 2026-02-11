import asyncio
from progressivis.core.scheduler import Scheduler
from progressivis.io.simple_csv_loader import SimpleCSVLoader

async def main():
    print("▶ Starting Progressive CSV Loader")

    scheduler = Scheduler()

    csv = SimpleCSVLoader(
        "incident_log.csv",
        chunksize=5000,
        scheduler=scheduler
    )

    # Run progressively
    while await scheduler.step():
        table = csv.table   # ✅ CORRECT OUTPUT SLOT
        if table is not None:
            print(f"Rows loaded so far: {len(table)}")

    print("✔ CSV loading completed")

if __name__ == "__main__":
    asyncio.run(main())


# This script demonstrates standalone progressive CSV loading using ProgressiVis.
# It incrementally loads the incident_log.csv file in chunks (5000 rows at a time).
# The Scheduler executes the dataflow step-by-step instead of processing everything in batch.
# As rows are progressively ingested, the script prints the number of rows loaded so far.
# This validates that progressive computation is working correctly before adding analytics.

