# Contributing

Thanks for taking the time to help. Bug reports, documentation fixes and new features are all
welcome.

## Development setup

You need Python 3.9 or newer and a Java runtime that your PySpark version supports (Java 17
works for every supported version).

```bash
git clone https://github.com/bhargavdundu90-dev/spark-scd2.git
cd spark-scd2
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

The tests start a local Spark session, so the first run takes a little while.

### Iceberg integration tests

`tests/test_sql.py` checks that the generated Iceberg SQL gives the same rows as the DataFrame
API. These tests are skipped unless Spark can load the Iceberg runtime:

```bash
SPARK_SCD2_ICEBERG_PACKAGES=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 pytest tests/test_sql.py
```

Use the runtime that matches your Spark version. CI runs them on Spark 3.5.

## Lint and format

```bash
ruff check .
ruff format .
```

CI runs both, plus the tests on Python 3.9 (PySpark 3.3) and Python 3.11 (PySpark 3.5).

## Pull requests

- Open an issue first for anything larger than a small fix, so we can agree on the approach.
- Keep each pull request to one change, with tests that fail without it.
- Behaviour changes need a line in `CHANGELOG.md` under "Unreleased".
- Keep the public API small. New options go on `SCD2Config` with a default that keeps current
  behaviour.
- The DataFrame API and the generated SQL must stay equivalent. If you change one, change the
  other and extend the integration test.

Issues labelled [`good first issue`](https://github.com/bhargavdundu90-dev/spark-scd2/labels/good%20first%20issue)
are a good place to start. Comment on one before you begin so two people do not work on it
at once.

## Reporting bugs

Please include the PySpark and Python versions, your `SCD2Config`, and the smallest snapshot
(a few rows built with `spark.createDataFrame`) that shows the problem.

## License

By contributing you agree that your contributions are licensed under the Apache License 2.0.
