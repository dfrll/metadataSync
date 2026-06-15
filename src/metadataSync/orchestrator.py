#! /usr/bin/env python3
"""
Owns environment loading, directory setup, and stage sequencing.
Each stage delegates entirely to fetch, transform, manifest, or storage.
"""

import logging
import os

import polars as pl
from pyrecount.accessor import Metadata

from config import Config, Credentials
from manifest import Manifest
from metadataSync import fetch, transform, storage

log = logging.getLogger(__name__)


class MetadataSync:
    def __init__(self, config: Config, credentials: Credentials):
        self.config = config
        self.credentials = credentials

    def _setup_dirs(self):
        self.config.data_dir.mkdir(exist_ok=True)
        self.config.runinfo_dir.mkdir(exist_ok=True)
        self.config.biosample_dir.mkdir(exist_ok=True)

    def _load_accessions(self) -> tuple[list[str], list[str]]:
        """Load SRA accessions from the pyrecount metadata cache."""
        log.info(
            "Loading accessions from pyrecount (organism=%s)", self.config.organism
        )

        # XXX: heavily tied to recount
        recount_metadata = Metadata(organism=self.config.organism)
        recount_metadata.cache()

        df = recount_metadata.load().filter(
            pl.col("file_source") == os.path.join("data_sources", self.config.db_source)
        )

        projects = df.get_column("project").unique().to_list()
        external_ids = df.get_column("external_id").unique().to_list()

        log.info(
            "Loaded %d projects and %d external IDs",
            len(projects),
            len(external_ids),
        )
        return projects, external_ids

    async def _stage_runinfo(
        self, projects: list[str], external_ids: list[str]
    ) -> pl.DataFrame:
        """Fetch runinfo, handle failures, and return a normalised DataFrame."""
        runinfo_manifest = Manifest(self.config.runinfo_dir / "manifest.tsv")
        failed_path = self.config.runinfo_dir / "failed.txt"

        accessions = runinfo_manifest.failed(failed_path) or projects

        failed = await fetch.fetch_runinfo(
            accessions=accessions,
            outpath=self.config.runinfo_dir,
            manifest=runinfo_manifest,
            config=self.config,
        )

        if failed:
            runinfo_manifest.write_failed(failed_path, failed)
            log.warning(
                "%d accessions failed and will be retried on next run", len(failed)
            )
        else:
            runinfo_manifest.clear_failed(failed_path)

        paths = runinfo_manifest.filepaths()
        if not paths:
            raise RuntimeError("No runinfo data available. Check fetch logs.")

        df = transform.concatenate_csvs(paths)
        df = transform.normalize_colnames(df)

        lookup = pl.DataFrame({"run": list(external_ids)})

        df = df.join(
            lookup,
            on="run",
            how="semi",
        )

        log.info("Runinfo rows after recount filtering: %d", df.height)
        return df

    async def _stage_biosample(self, runinfo: pl.DataFrame) -> pl.DataFrame:
        """Fetch biosample metadata and return a pivoted DataFrame."""
        biosample_ids = runinfo.get_column("biosample").unique().to_list()

        biosample_manifest = Manifest(self.config.biosample_dir / "manifest.tsv")
        failed_path = self.config.biosample_dir / "failed.txt"

        accessions = biosample_manifest.failed(failed_path) or biosample_ids

        failed = await fetch.fetch_biosample(
            accessions=accessions,
            outpath=self.config.biosample_dir,
            manifest=biosample_manifest,
            config=self.config,
        )

        if failed:
            biosample_manifest.write_failed(failed_path, failed)
            log.warning(
                "%d biosample accessions failed and will be retried on next run",
                len(failed),
            )
        else:
            biosample_manifest.clear_failed(failed_path)

        paths = biosample_manifest.filepaths()
        if not paths:
            raise RuntimeError("No biosample data available, check fetch logs")

        raw = transform.parse_biosample_csvs(paths)
        pivoted = transform.pivot_biosample(raw)
        filterd = transform.filter_sparse_columns(
            pivoted, min_coverage=self.config.biosample_min_coverage
        )
        return transform.normalize_colnames(filterd)

    def _stage_join(
        self, runinfo: pl.DataFrame, biosample: pl.DataFrame
    ) -> pl.DataFrame:
        """Join runinfo and biosample, then apply final column renames."""
        return transform.join_runinfo_biosample(runinfo, biosample)
        # return transform.rename_columns(joined)

    async def _stage_upload(self, df: pl.DataFrame):
        """Write D1-compatible SQL and upload to Cloudflare via Wrangler."""

        storage.ensure_d1(
            self.credentials.d1_db_name,
            self.credentials.cf_api_token,
            self.credentials.cf_account_id,
        )

        await storage.import_to_d1(
            df=df,
            db_name=self.credentials.d1_db_name,
            api_token=self.credentials.cf_api_token,
        )

    async def run(
        self,
        sync_runinfo: bool = False,
        sync_biosample: bool = False,
        d1_upload: bool = False,
    ):
        self._setup_dirs()
        projects, external_ids = self._load_accessions()

        # --- Load or fetch runinfo ---
        if sync_runinfo:
            runinfo = await self._stage_runinfo(projects, external_ids)
        else:
            log.info("Skipping runinfo fetch, loading from manifest")
            runinfo_manifest = Manifest(self.config.runinfo_dir / "manifest.tsv")
            paths = runinfo_manifest.filepaths()
            if not paths:
                raise RuntimeError("No runinfo data on disk and --sync-runinfo not set")
            runinfo = transform.normalize_colnames(transform.concatenate_csvs(paths))

        # --- Load or fetch biosample ---
        if sync_biosample:
            biosample = await self._stage_biosample(runinfo)
        else:
            log.info("Skipping biosample fetch, loading from manifest")
            biosample_manifest = Manifest(self.config.biosample_dir / "manifest.tsv")
            paths = biosample_manifest.filepaths()
            if not paths:
                raise RuntimeError(
                    "No biosample data on disk and --sync-biosample not set"
                )
            # Parse, pivot, and filter sparse biosample CSVs
            raw = transform.parse_biosample_csvs(paths)
            pivoted = transform.pivot_biosample(raw)
            filtered = transform.filter_sparse_columns(
                pivoted, min_coverage=self.config.biosample_min_coverage
            )
            biosample = transform.normalize_colnames(filtered)

        # --- Join runinfo and biosample ---
        df = self._stage_join(runinfo, biosample)
        df = transform.rename_columns(df).filter(
            pl.col("external_id").is_in(external_ids)
        )

        existing_ids = set(df.get_column("external_id").unique())
        missing = [eid for eid in external_ids if eid not in existing_ids]

        m = {col: [None] * len(missing) for col in df.columns}
        m["external_id"] = missing

        m_df = pl.DataFrame(m)
        df = pl.concat([df, m_df])

        log.info("Final dataset: %d rows, %d columns", df.height, df.width)

        # --- Optional D1 upload ---
        if d1_upload:
            await self._stage_upload(df)
