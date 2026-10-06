## Support
Databricks does not offer official support for Databricks Solutions and its repository.
For any issue with this assets or the demos installed, please open an issue using github and the team will have a look on a best effort basis.

## Synthetic data

This repository contains no external or customer dataset. `src/notebooks/stream.py` uses Apache Spark's built-in `rate` source and a deterministic schema to create synthetic grid-telemetry records at runtime. The generated values are fictional, contain no personal information, and are not derived from a real utility or customer.
