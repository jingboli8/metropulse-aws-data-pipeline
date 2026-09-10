"""Download and extract MetroPT-3 from the official UCI source."""

from __future__ import annotations

import argparse
from pathlib import Path

from metropulse_feasibility.download import (
    EXPECTED_CSV,
    UCI_DATASET_URL,
    download_archive,
    extract_archive,
    inspect_zip,
    read_manifest_checksum,
    sha256_file,
    write_manifest,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--manifest", type=Path, default=Path("artifacts/download-manifest.json"))
    args = parser.parse_args()

    previous_checksum = read_manifest_checksum(args.manifest)
    archive_path = args.data_dir / "metropt-3-dataset.zip"
    if archive_path.exists():
        print(f"Verifying existing archive: {archive_path}")
        members = inspect_zip(archive_path)
        checksum = sha256_file(archive_path)
    else:
        print(f"Downloading official UCI archive: {UCI_DATASET_URL}")
        checksum = download_archive(UCI_DATASET_URL, archive_path)
        members = inspect_zip(archive_path)

    on_disk_checksum = sha256_file(archive_path)
    if on_disk_checksum != checksum:
        raise RuntimeError(
            "Archive SHA-256 changed between streaming download and on-disk verification: "
            f"{checksum} != {on_disk_checksum}"
        )
    if previous_checksum is not None and on_disk_checksum != previous_checksum:
        raise RuntimeError(
            "Archive SHA-256 differs from the previous verified manifest; refusing to "
            f"continue: {previous_checksum} != {on_disk_checksum}"
        )

    print(f"SHA-256: {on_disk_checksum}")
    for member in members:
        print(
            f"ZIP member: {member.name} "
            f"compressed={member.compressed_size} uncompressed={member.uncompressed_size}"
        )
    if EXPECTED_CSV not in {Path(member.name).name for member in members}:
        raise RuntimeError(f"Expected CSV not found in archive: {EXPECTED_CSV}")

    extracted = extract_archive(archive_path, args.data_dir)
    write_manifest(args.manifest, archive_path, on_disk_checksum)
    for path in extracted:
        print(f"Extracted: {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
