#! /usr/bin/env python3
"""
Async HTTP fetching for NCBI SRA runinfo and biosample endpoints.
Handles batching, retries, concurrency limiting, and manifest recording.
"""

import time
import asyncio
import logging
from io import StringIO
from pathlib import Path

import httpx
import polars as pl

from config import Config
from manifest import Manifest

log = logging.getLogger(__name__)

RUNINFO_URL = "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/runinfo"
BIOSAMPLE_URL = "https://api.ncbi.nlm.nih.gov/datasets/v2/biosample/accession/{accessions}/biosample_report"


_rate_lock = asyncio.Lock()
_last_request = 0.0


async def rate_limit(rate: float):
    global _last_request

    async with _rate_lock:
        now = time.monotonic()
        min_interval = 1.0 / rate

        wait = min_interval - (now - _last_request)
        if wait > 0:
            await asyncio.sleep(wait)

        _last_request = time.monotonic()


def _batch_id(batch: list[str]) -> str:
    return f"{batch[0]}_{batch[-1]}"


def _chunked(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


async def _fetch_runinfo_batch(
    client: httpx.AsyncClient,
    accessions: list[str],
) -> pl.DataFrame | None:
    acc_string = ",".join(accessions)
    resp = await client.get(RUNINFO_URL, params={"acc": acc_string}, timeout=60)
    resp.raise_for_status()

    if not resp.text.strip():
        return None

    df = pl.read_csv(StringIO(resp.text), infer_schema_length=None)
    return df if df.height else None


async def _fetch_biosample_batch(
    client: httpx.AsyncClient,
    accessions: list[str],
) -> pl.DataFrame | None:
    url = BIOSAMPLE_URL.format(accessions=",".join(accessions))
    resp = await client.get(url, timeout=60)
    resp.raise_for_status()

    reports = resp.json().get("reports", [])
    if not reports:
        return None

    rows = []
    for r in reports:
        acc = r.get("accession")
        organism = r.get("organism", {}).get("organism_name")
        for attr in r.get("attributes", []):
            rows.append(
                {
                    "biosample": acc,
                    "organism": organism,
                    "attribute_name": attr.get("name"),
                    "attribute_value": attr.get("value"),
                }
            )

    return pl.DataFrame(rows) if rows else None


async def _fetch_with_retry(
    client: httpx.AsyncClient,
    accessions: list[str],
    fetch_fn,
    attempts: int,
    rate: float,
) -> pl.DataFrame | None:
    delay = 1
    last_exc = None

    for i in range(attempts):
        try:
            await rate_limit(rate)

            return await fetch_fn(client, accessions)

        except Exception as e:
            last_exc = e

            if i == attempts - 1:
                break

            log.warning(
                "Batch %s attempt %d/%d failed: %s — retrying in %ds",
                _batch_id(accessions),
                i + 1,
                attempts,
                e,
                delay,
            )

            await asyncio.sleep(delay)
            delay *= 2

    raise RuntimeError(
        f"Batch {_batch_id(accessions)} failed after {attempts} attempts"
    ) from last_exc


async def _fetch_batches(
    accessions: list[str],
    fetch_fn,
    outpath: Path,
    manifest: Manifest,
    batch_size: int,
    max_concurrency: int,
    max_connections: int,
    rate: int,
    attempts: int,
) -> list[str]:
    """
    Core fetch loop. Splits accessions into batches, fetches each concurrently,
    writes results to disk, and records successes in the manifest.
    Returns a list of accessions from permanently failed batches.
    """
    limits = httpx.Limits(max_connections=max_connections)
    failed_accessions: set[str] = set()
    completed = manifest.completed()

    queue: asyncio.Queue[list[str]] = asyncio.Queue()

    async with httpx.AsyncClient(limits=limits) as client:

        async def worker():
            while True:
                batch = await queue.get()

                try:
                    bid = _batch_id(batch)
                    outfile = outpath / f"{bid}.csv"

                    if bid in completed:
                        log.debug("Skipping already-completed batch %s", bid)
                        continue

                    if outfile.exists():
                        log.debug(
                            "File exists but not in manifest, recording: %s", outfile
                        )
                        manifest.record(bid, outfile)
                        continue

                    df = await _fetch_with_retry(
                        client, batch, fetch_fn, attempts, rate
                    )
                    if df is not None:
                        df.write_csv(outfile)
                        manifest.record(bid, outfile)
                    else:
                        log.warning("Batch %s returned no data", bid)

                except Exception as e:
                    log.error("Batch %s failed permanently: %s", bid, e)
                    failed_accessions.update(batch)

                finally:
                    queue.task_done()

        batches = list(_chunked(sorted(accessions), batch_size))

        for batch in batches:
            queue.put_nowait(batch)

        workers = [asyncio.create_task(worker()) for _ in range(max_concurrency)]

        await queue.join()

        for w in workers:
            w.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

    total_batches = len(batches)
    summary = manifest.summary()
    log.info(
        "Fetch complete: %d/%d batches recorded, %d accessions requested",
        summary["recorded"],
        total_batches,
        len(accessions),
    )

    return sorted(failed_accessions)


async def fetch_runinfo(
    accessions: list[str],
    outpath: Path,
    manifest: Manifest,
    config: Config,
) -> list[str]:
    """
    Fetch SRA run info for a list of accessions.
    Returns any accessions that failed permanently.
    """
    log.info("Fetching runinfo for %d accessions", len(accessions))
    return await _fetch_batches(
        accessions=accessions,
        fetch_fn=_fetch_runinfo_batch,
        outpath=outpath,
        manifest=manifest,
        batch_size=config.runinfo_batch_size,
        max_concurrency=config.max_concurrency,
        max_connections=config.max_connections,
        rate=config.rate,
        attempts=3,
    )


async def fetch_biosample(
    accessions: list[str],
    outpath: Path,
    manifest: Manifest,
    config: Config,
) -> list[str]:
    """
    Fetch biosample metadata for a list of accessions.
    Returns any accessions that failed permanently.
    """
    log.info("Fetching biosample metadata for %d accessions", len(accessions))
    return await _fetch_batches(
        accessions=accessions,
        fetch_fn=_fetch_biosample_batch,
        outpath=outpath,
        manifest=manifest,
        batch_size=config.biosample_batch_size,
        max_concurrency=config.max_concurrency,
        max_connections=config.max_connections,
        rate=config.rate,
        attempts=3,
    )
