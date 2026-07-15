# Configuration approach

This project follows a **no-hardcoded-paths, environment-variable-driven**
configuration approach rather than a separate YAML config loader:

- `pipelines/config.py` -- all filesystem paths (derived from the project
  root, so the project works if cloned/moved anywhere) and all Postgres
  connection settings (host/port/db/user/password), every one overridable
  via an environment variable (`UMIP_PG_HOST`, `UMIP_PG_PORT`, etc.)
- `scripts/generate_mock_data.py` -- every data volume (drivers, customers,
  trips, ...) is overridable via an environment variable
  (`UMIP_N_DRIVERS`, `UMIP_N_TRIPS`, ...), see the README for examples
- `pipelines/recommendation_engine.py` -- business-rule thresholds are
  plain module-level constants at the top of the file, deliberately kept
  as code (not externalized to YAML) since they're the kind of business
  judgment call an analyst would want to see and tune directly alongside
  the rule they affect, not hidden in a separate config file
- `docker-compose.yml` / `Dockerfile.airflow` -- container-level
  environment variables for Postgres credentials and Airflow settings

This directory is kept as a placeholder for a future YAML-based config
layer if the project outgrows environment variables (e.g., per-environment
config files for dev/staging/prod) -- not currently needed at this
project's scale.
