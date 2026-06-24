import asyncio
import aiohttp
import time
import sys
import os

MOJANG_URL = "https://api.mojang.com/users/profiles/minecraft/{}"

CONCURRENCY = 10        
REQUESTS_PER_SECOND = 8 


def print_banner():
    banner = r"""
           ___             _     _    
         .' ..]           / |_  / |_  
 .---.  _| |_  _   _   __`| |-'`| |-' 
/ /'`\]'-| |-'[ \ [ \ [  ]| |   | |   
| \__.   | |   \ \/\ \/ / | |,  | |,  
'.___.' [___]   \__/\__/  \__/  \__/ on ig

"""
    print(banner)


class RateLimiter:
    """Simple token-bucket style limiter shared across all workers."""
    def __init__(self, rate_per_sec):
        self.interval = 1.0 / rate_per_sec
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            wait_time = self._last + self.interval - now
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last = time.monotonic()


def is_valid_username(name):
    if not (3 <= len(name) <= 16):
        return False
    return all(c.isalnum() or c == "_" for c in name)


def load_usernames(path):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def load_done(output_path):
    done = set()
    if os.path.exists(output_path):
        with open(output_path, "r", encoding="utf-8") as f:
            for line in f:
                if ":" in line:
                    done.add(line.split(":", 1)[0].strip())
    return done


async def check_username(session, username, limiter, retries=3):
    url = MOJANG_URL.format(username)
    for attempt in range(retries):
        await limiter.wait()
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return username, "unavailable"
                elif resp.status == 404:
                    return username, "available"
                elif resp.status == 429:
                    wait = 5 * (attempt + 1)
                    await asyncio.sleep(wait)
                    continue
                else:
                    return username, f"http_{resp.status}"
        except asyncio.TimeoutError:
            continue
        except aiohttp.ClientError as e:
            return username, f"error:{e}"
    return username, "rate_limited_gaveup"


async def worker(name_queue, results_queue, session, limiter):
    while True:
        username = await name_queue.get()
        if username is None:
            name_queue.task_done()
            break
        result = await check_username(session, username, limiter)
        await results_queue.put(result)
        name_queue.task_done()


async def writer(results_queue, out_path, avail_path, total):
    out_f = open(out_path, "a", encoding="utf-8")
    avail_f = open(avail_path, "a", encoding="utf-8")
    checked = 0
    available_count = 0
    start = time.time()

    while True:
        item = await results_queue.get()
        if item is None:
            break
        username, status = item
        out_f.write(f"{username}: {status}\n")
        out_f.flush()
        if status == "available":
            avail_f.write(username + "\n")
            avail_f.flush()
            available_count += 1

        checked += 1
        if checked % 100 == 0:
            elapsed = time.time() - start
            rate = checked / elapsed
            eta_hours = ((total - checked) / rate) / 3600 if rate > 0 else float("inf")
            print(f"[{checked}/{total}] {rate:.1f} req/s | ETA {eta_hours:.2f}h | available: {available_count}")

    out_f.close()
    avail_f.close()
    print(f"\nDone. Checked {checked}. Available: {available_count}")


async def main(input_path, output_path="results.txt", available_path="available.txt"):
    print_banner()
    all_names = load_usernames(input_path)
    done = load_done(output_path)

    valid = [u for u in all_names if u not in done and is_valid_username(u)]
    skipped_invalid = len(all_names) - len(done) - len(valid)
    print(f"Total: {len(all_names)} | Already done: {len(done)} | "
          f"Skipped invalid: {skipped_invalid} | To check: {len(valid)}")

    name_queue = asyncio.Queue()
    results_queue = asyncio.Queue()
    limiter = RateLimiter(REQUESTS_PER_SECOND)

    for name in valid:
        name_queue.put_nowait(name)
    for _ in range(CONCURRENCY):
        name_queue.put_nowait(None)

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        writer_task = asyncio.create_task(writer(results_queue, output_path, available_path, len(valid)))
        workers = [asyncio.create_task(worker(name_queue, results_queue, session, limiter))
                   for _ in range(CONCURRENCY)]

        await asyncio.gather(*workers)
        await results_queue.put(None)
        await writer_task


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_names.py usernames.txt [results.txt] [available.txt]")
        sys.exit(1)
    input_file = sys.argv[1]
    out_file = sys.argv[2] if len(sys.argv) > 2 else "results.txt"
    avail_file = sys.argv[3] if len(sys.argv) > 3 else "available.txt"

    try:
        asyncio.run(main(input_file, out_file, avail_file))
    except KeyboardInterrupt:
        print("\nStopped by user. Progress is saved in results.txt — rerun the same command to resume.")