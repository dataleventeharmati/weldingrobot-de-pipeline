from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime

@dataclass(frozen=True)
class OutputPaths:
    staged_dir: Path = Path("data/staged")
    reports_dir: Path = Path("data/reports")

    def ensure(self) -> None:
        self.staged_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def stamp() -> str:
        return datetime.now().strftime("%Y%m%d_%H%M%S")
