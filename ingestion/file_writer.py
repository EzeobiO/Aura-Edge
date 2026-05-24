"""
Rotating JSONL writer. Writes one JSON object per line. Rotates files
each wall-clock minute. Files are named with their open timestamp so
the pipeline can sort and process them deterministically.
"""

from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import Optional, TextIO, Type
import json
import logging

logger = logging.getLogger(__name__)


class RotatingJsonlWriter:
    def __init__(self, output_dir: Path, name_prefix: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.name_prefix = name_prefix
        
        # Explicit type hints so the type checker knows these can hold values later
        self._current_minute: Optional[str] = None
        self._current_file: Optional[TextIO] = None
        self._current_path: Optional[Path] = None

    def write(self, record: dict) -> None:
        now = datetime.now(timezone.utc)
        minute_key = now.strftime("%Y-%m-%dT%H-%M")
        
        if minute_key != self._current_minute:
            self._rotate(minute_key)
            
        # Local variable assignment acts as a flawless Type Guard
        current_file = self._current_file
        if current_file is None:
            raise RuntimeError("No active file available for writing.")
            
        json.dump(record, current_file, separators=(",", ":"))
        current_file.write("\n")
        current_file.flush()

    def _rotate(self, new_minute: str) -> None:
        if self._current_file is not None:
            self._current_file.close()
            logger.info("Closed %s", self._current_path)
            
        self._current_minute = new_minute
        self._current_path = self.output_dir / f"{self.name_prefix}_{new_minute}.jsonl"
        
        # Open with explicit encoding (good practice!)
        self._current_file = open(self._current_path, "a", encoding="utf-8")
        logger.info("Opened %s", self._current_path)

    def close(self) -> None:
        if self._current_file is not None:
            self._current_file.close()
        
        # Reset everything so the writer can safely be reused if needed
        self._current_file = None
        self._current_minute = None
        self._current_path = None

    # Adding context manager support
    def __enter__(self) -> "RotatingJsonlWriter":
        return self

    def __exit__(
        self, 
        exc_type: Optional[Type[BaseException]], 
        exc_val: Optional[BaseException], 
        exc_tb: Optional[TracebackType]
    ) -> None:
        self.close()