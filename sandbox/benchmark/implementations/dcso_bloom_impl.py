"""
Wrapper for DCSO/bloom Go implementation.
Uses subprocess to call the compiled Go CLI.
"""

import os
import subprocess
import tempfile
from pathlib import Path

from .base import BloomFilterConfig, BloomFilterImplementation


class DCSOBloomImplementation(BloomFilterImplementation):
    """DCSO/bloom implementation wrapper."""

    def __init__(self):
        self._filter_path = None
        self._num_items = 0
        self._expected_fpp = 0.0

    @property
    def name(self) -> str:
        return "dcso-bloom"

    def _get_cli_path(self) -> Path:
        """Get path to the compiled CLI binary."""
        impl_dir = Path(__file__).parent / "dcso-bloom"
        return impl_dir / "bloom-cli"

    def is_available(self) -> bool:
        cli_path = self._get_cli_path()
        if not cli_path.exists():
            # Try to build it
            try:
                self._build()
                return cli_path.exists()
            except Exception:
                return False
        return True

    def _build(self) -> None:
        """Build the Go CLI."""
        impl_dir = Path(__file__).parent / "dcso-bloom"

        # Check if Go is available
        result = subprocess.run(["which", "go"], capture_output=True)
        if result.returncode != 0:
            raise RuntimeError("Go is not installed")

        # Build the CLI
        result = subprocess.run(
            ["go", "build", "-o", "bloom-cli", "."],
            cwd=impl_dir,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to build: {result.stderr}")

    def _run_cli(self, *args, input_data: bytes = None) -> subprocess.CompletedProcess:
        """Run the CLI with given arguments."""
        cli_path = self._get_cli_path()
        result = subprocess.run(
            [str(cli_path)] + list(args),
            input=input_data,
            capture_output=True
        )
        return result

    def create_filter(self, config: BloomFilterConfig) -> None:
        # Create a temporary file for the filter
        fd, path = tempfile.mkstemp(suffix=".bloom")
        os.close(fd)
        self._filter_path = Path(path)

        result = self._run_cli(
            "create",
            "-n", str(config.expected_items),
            "-p", str(config.false_positive_rate),
            "-o", str(self._filter_path)
        )

        if result.returncode != 0:
            raise RuntimeError(f"Failed to create filter: {result.stderr.decode()}")

        self._expected_fpp = config.false_positive_rate
        self._num_items = 0

    def add_items(self, items: list[bytes]) -> None:
        if self._filter_path is None:
            raise RuntimeError("Filter not created")

        # Write items to stdin, one per line
        input_data = b'\n'.join(items)

        result = self._run_cli(
            "add",
            "-f", str(self._filter_path),
            input_data=input_data
        )

        if result.returncode != 0:
            raise RuntimeError(f"Failed to add items: {result.stderr.decode()}")

        self._num_items += len(items)

    def check_items(self, items: list[bytes]) -> list[bool]:
        if self._filter_path is None:
            raise RuntimeError("Filter not created")

        input_data = b'\n'.join(items)

        result = self._run_cli(
            "check",
            "-f", str(self._filter_path),
            input_data=input_data
        )

        if result.returncode != 0:
            raise RuntimeError(f"Failed to check items: {result.stderr.decode()}")

        # Parse output: each line is "1" or "0"
        lines = result.stdout.decode().strip().split('\n')
        return [line == "1" for line in lines if line]

    def get_size_bytes(self) -> int:
        if self._filter_path is None or not self._filter_path.exists():
            return 0
        return self._filter_path.stat().st_size

    def save(self, path: Path) -> None:
        if self._filter_path is None:
            raise RuntimeError("Filter not created")

        # Copy the filter file
        import shutil
        shutil.copy(self._filter_path, path)

    def load(self, path: Path) -> None:
        import shutil
        # Create a temp copy to work with
        fd, temp_path = tempfile.mkstemp(suffix=".bloom")
        os.close(fd)
        shutil.copy(path, temp_path)
        self._filter_path = Path(temp_path)

    def __del__(self):
        # Clean up temp file
        if self._filter_path is not None and self._filter_path.exists():
            try:
                self._filter_path.unlink()
            except Exception:
                pass
