#! /usr/bin/env python3
"""
Manifest tracks which batches have been successfully fetched and written to disk.

File format (TSV):
    batch_id    filepath    completed_at
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

_HEADER = "batch_id\tfilepath\tcompleted_at\n"
_SEP = "\t"


class Manifest:
    def __init__(self, path: Path):
        self.path = path
        self._ensure()

    def _ensure(self):
        """Create the manifest file with a header if it does not exist."""
        if not self.path.exists():
            self.path.write_text(_HEADER, encoding="utf-8")

    def record(self, batch_id: str, filepath: Path):
        """
        Record a successfully completed batch.
        Appends a single line, safe to call from concurrent workers
        because each append is a single write syscall on all major platforms.
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        line = f"{batch_id}{_SEP}{filepath}{_SEP}{timestamp}\n"
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line)

    def completed(self) -> set[str]:
        """Return the set of batch IDs that have been successfully recorded."""
        entries = self._read()
        return {e["batch_id"] for e in entries}

    def filepaths(self) -> list[Path]:
        """
        Return the list of output file paths recorded in the manifest,
        in the order they were written.
        Only includes paths that still exist on disk, warns if any are missing.
        """
        entries = self._read()
        paths = []
        for e in entries:
            p = Path(e["filepath"])
            if p.exists():
                paths.append(p)
            else:
                log.warning("Manifest references missing file: %s", p)
        return paths

    def failed(self, failed_path: Path) -> list[str]:
        """
        Return accessions listed in the failed.txt file, if it exists.
        Each line in failed.txt is a single accession ID.
        """
        if not failed_path.exists():
            return []
        accessions = failed_path.read_text(encoding="utf-8").splitlines()
        log.info("Loaded %d failed accessions from %s", len(accessions), failed_path)
        return accessions

    def write_failed(self, failed_path: Path, accessions: list[str]):
        """Persist failed accessions to disk for retry on the next run."""
        failed_path.write_text("\n".join(accessions), encoding="utf-8")
        log.warning("Wrote %d failed accessions to %s", len(accessions), failed_path)

    def clear_failed(self, failed_path: Path):
        """Remove the failed accessions file after a successful retry."""
        if failed_path.exists():
            failed_path.unlink()
            log.info("Cleared failed accessions file: %s", failed_path)

    def summary(self) -> dict:
        """Return a summary dict for logging at the end of a fetch stage."""
        entries = self._read()
        missing = [e for e in entries if not Path(e["filepath"]).exists()]
        return {
            "recorded": len(entries),
            "on_disk": len(entries) - len(missing),
            "missing_from_disk": len(missing),
        }

    def _read(self) -> list[dict]:
        """Parse the manifest TSV into a list of dicts, skipping the header."""
        lines = self.path.read_text(encoding="utf-8").splitlines()
        entries = []
        for line in lines[1:]:  # skip header
            if not line.strip():
                continue
            parts = line.split(_SEP)
            if len(parts) != 3:
                log.warning("Malformed manifest line, skipping: %r", line)
                continue
            entries.append(
                {
                    "batch_id": parts[0],
                    "filepath": parts[1],
                    "completed_at": parts[2],
                }
            )
        return entries
