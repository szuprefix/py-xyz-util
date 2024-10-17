import asyncio

async def one_task(sem, func, value, interval=1):
    async with sem:
        print('calling:', value, '@', func.__name__)
        await asyncio.to_thread(func, value)
        await asyncio.sleep(interval)
        print('done:', value, '@', func.__name__)


def run_tasks(values, func, concurrency=2):
    async def start():
        sem = asyncio.Semaphore(concurrency)
        tasks = [one_task(sem, func, v) for v in values]
        await asyncio.gather(*tasks)
    asyncio.run(start())
