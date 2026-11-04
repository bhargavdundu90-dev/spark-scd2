# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-11-04

### Added

- `SCD2Config` describing key, Type 2, Type 1 and metadata columns, with validation.
- `apply_snapshot` for Type 2 loads from full snapshots: hash-based change detection with null
  coalescing, insert / update / delete / reopen / unchanged classification and half-open
  validity.
- `delete_mode="ignore"` for partial extracts.
- `apply_type1` for in-place corrections on every version.
- `validate_snapshot` refusing empty, null-key, duplicate-key and incomplete snapshots;
  `apply_snapshot` also refuses out-of-order loads and a different snapshot for a day already
  loaded.
- `as_of`, `current` and `empty_dimension` helpers.
- Optional deterministic surrogate key column.
- `iceberg_merge_sql` generating the equivalent Spark SQL for Apache Iceberg tables.

[Unreleased]: https://github.com/bhargavdundu90-dev/spark-scd2/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/bhargavdundu90-dev/spark-scd2/releases/tag/v0.1.0
