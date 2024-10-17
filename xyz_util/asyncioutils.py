import asyncio

def run_tasks(values, func, concurrency=2, interval=0, timeout=60):
    async def start():
        async def one_task(value):
            try:
                async with sem:
                    print(f'calling: {func.__name__}({value})')
                    await asyncio.wait_for(asyncio.to_thread(func, value),  timeout=timeout)
                    await asyncio.sleep(interval)
                    print(f'done:{func.__name__}({value})')
            except asyncio.TimeoutError:
                print(f'Timeout: {func.__name__}({value}) timed out after {timeout} seconds.')
            except Exception:
                import traceback
                print(traceback.format_exc())
                print(f'ERROR:{func.__name__}({value})')
        sem = asyncio.Semaphore(concurrency)
        tasks = [one_task(v) for v in values]
        await asyncio.gather(*tasks)
    asyncio.run(start())
