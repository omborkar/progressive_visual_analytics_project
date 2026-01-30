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
