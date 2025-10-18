import asyncio
import aiohttp
import pandas as pd
import json
import random
import os
import time
import logging
from bs4 import BeautifulSoup

INPUT_FILE = "API_ids/id_tiki_part_1.csv"
OUTPUT_DIR = "tiki_batches_use_log"
LOG_FILE = "tiki_crawl.log"
FAILED_FILE = "failed_ids.csv"
BASE_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"
MAX_CONCURRENCY = 100
MAX_RETRIES = 3
BATCH_SIZE = 1000
DELAY_RANGE = (0.5, 1.0)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

#logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(os.path.join("logs", LOG_FILE), encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


os.makedirs(OUTPUT_DIR, exist_ok=True)
failed_ids = []
consecutive_429 = 0
semaphore = asyncio.BoundedSemaphore(MAX_CONCURRENCY)

#fetch function
async def fetch_product(session, pid):
    global consecutive_429
    url = BASE_URL.format(pid)
    for attempt in range(1, MAX_RETRIES + 1):
        async with semaphore:
            try:
                await asyncio.sleep(random.uniform(*DELAY_RANGE))
                async with session.get(url, headers=HEADERS, timeout=10) as resp:
                    if resp.status == 429:
                        consecutive_429 += 1
                        wait = 5 * attempt
                        logger.warning(f"[429] Too many requests for {pid}, retry in {wait}s")
                        await asyncio.sleep(wait)
                        continue

                    if resp.status == 404:
                        logger.warning(f"[404] Product {pid} not found.")
                        failed_ids.append((pid, 404))
                        return None

                    if resp.status != 200:
                        logger.warning(f"[WARN] Product {pid} failed: HTTP {resp.status}")
                        failed_ids.append((pid, resp.status))
                        return None

                    data = await resp.json()
                    html_desc = data.get("description", "")
                    text_desc = BeautifulSoup(html_desc, "html.parser").get_text(separator ="\n").strip()

                    logger.info(f"[SUCCESS] Crawled product ID: {pid}")
                    return {
                        "id": data.get("id"),
                        "name": data.get("name"),
                        "url_key": data.get("url_key"),
                        "price": data.get("price"),
                        "description": text_desc,
                        "image_url": data.get("thumbnail_url"),
                    }

            except asyncio.TimeoutError:
                logger.warning(f"[TIMEOUT] {pid}, retry {attempt}/{MAX_RETRIES}")
                await asyncio.sleep(2 * attempt)
            except Exception as e:
                logger.error(f"[ERROR] {pid}: {e}")
                failed_ids.append((pid, "Exception"))
                return None

    logger.error(f"[FAIL] {pid} after {MAX_RETRIES} retries")
    failed_ids.append((pid, "Failed"))
    return None


#main function
async def main():
    start_time = time.time()
    df = pd.read_csv(INPUT_FILE)
    ids = df["id"].astype(str).tolist()
    total_successful_count = 0

#checkpoint
    done_batches = {
        int(f.split("_")[-1].split(".")[0])
        for f in os.listdir(OUTPUT_DIR)
        if f.startswith("tiki_batch_") and f.endswith(".json")
    }
    start_batch = max(done_batches) + 1 if done_batches else 1
    logger.info(f" Resume: starting from batch {start_batch}")

    async with aiohttp.ClientSession() as session:
        total_batches = (len(ids) + BATCH_SIZE - 1) // BATCH_SIZE
        for batch_index in range(start_batch, total_batches + 1):
            start = (batch_index - 1) * BATCH_SIZE
            end = min(start + BATCH_SIZE, len(ids))
            batch_ids = ids[start:end]

            logger.info(f" Processing batch {batch_index}/{total_batches} ({len(batch_ids)} IDs)")
            results = []
            tasks = [asyncio.create_task(fetch_product(session, pid)) for pid in batch_ids]
            for i, task in enumerate(asyncio.as_completed(tasks), 1):
                product = await task
                if product:
                    results.append(product)
                if i % 200 == 0:
                    logger.info(f" {i}/{len(batch_ids)} done in batch {batch_index}")

            #Save batch

            out_file = os.path.join(OUTPUT_DIR, f"tiki_batch_{batch_index}.json")
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=4)
            successful_in_batch = len(results)
            total_successful_count += successful_in_batch
            logger.info(f" Saved {successful_in_batch} products to {out_file} (Total successful: {total_successful_count})")

            if failed_ids:
                df_failed = pd.DataFrame(failed_ids, columns=["id", "error"])
                file_exists = os.path.exists(FAILED_FILE)
                write_header = not file_exists or os.path.getsize(FAILED_FILE) == 0
                df_failed.to_csv(
                    FAILED_FILE,
                    index=False,
                    mode='a',
                    header=write_header
                )
                logger.warning(f" Appended {len(failed_ids)} failed IDs to {FAILED_FILE}")
                failed_ids.clear()
            del results, tasks
            await asyncio.sleep(3)

    end_time = time.time()
    logger.info(f" Done! Total time: {end_time - start_time:.2f}s")
    #Tổng số id crawl thành công
    logger.info(f" Total successful IDs crawled: {total_successful_count}")
    if os.path.exists(FAILED_FILE) and os.path.getsize(FAILED_FILE) > 0:
        try:
            total_failed_on_disk = len(pd.read_csv(FAILED_FILE))
            logger.warning(f" Total failed IDs logged on disk: {total_failed_on_disk} (see {FAILED_FILE})")
        except pd.errors.EmptyDataError:
            pass

if __name__ == "__main__":
    asyncio.run(main())

