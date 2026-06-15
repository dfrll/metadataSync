# config.py
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Config:
    organism: str = "human"
    db_source: str = "sra"

    runinfo_batch_size: int = 10
    biosample_batch_size: int = 100
    biosample_min_coverage: float = 0.2
    max_concurrency: int = 2
    max_connections: int = 2
    rate: float = 2.0

    project_root: Path = field(default_factory=Path.cwd)

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def runinfo_dir(self) -> Path:
        return self.data_dir / "runinfo"

    @property
    def biosample_dir(self) -> Path:
        return self.data_dir / "biosample"


@dataclass(frozen=True)
class Credentials:
    d1_db_name: str
    d1_db_id: str
    cf_api_token: str
    cf_account_id: str

    @classmethod
    def from_env(cls) -> "Credentials":
        d1_db_name = os.getenv("D1_DB_NAME")
        d1_db_id = os.getenv("D1_DB_ID")
        token = os.getenv("CLOUDFLARE_API_TOKEN")
        account_id = os.getenv("CLOUDFLARE_ACCOUNT_ID")

        missing = [
            k
            for k, v in [
                ("D1_DB_NAME", d1_db_name),
                ("D1_DB_ID", d1_db_id),
                ("CLOUDFLARE_API_TOKEN", token),
                ("CLOUDFLARE_ACCOUNT_ID", account_id),
            ]
            if not v
        ]

        if missing:
            raise EnvironmentError(f"Missing env vars: {missing}")

        return cls(
            d1_db_name=d1_db_name,
            d1_db_id=d1_db_id,
            cf_api_token=token,
            cf_account_id=account_id,
        )
