# Superset dashboard backup

## Export

`dashboard_export_20261004T063550 (1).zip`

Contains dashboard layout, charts, dataset definitions, and filters.
It does not contain PostgreSQL lap data.

## Restore

1. Start PostgreSQL and Superset from the repository root:

   docker compose --env-file .env -f docker/docker-compose.yml up -d --no-deps postgres superset

2. Open http://localhost:8088 and sign in to an initialized Superset instance.
3. Open Dashboards and choose Import.
4. Upload the export ZIP.
5. Supply the PostgreSQL password when prompted.

For Superset running in Docker, the database connection uses:
- Host: postgres
- Port: 5432
- Database: f1_db
- User: f1_user

The matching PostgreSQL tables and data must already exist.

## Validation

- Season cards: 24 races, 6 sprints, 28,952 positive-duration laps,
  and 29,052 recorded laps.
- Session filter affects the median, progression, sector, and consistency charts.
- Driver filter affects progression, sector, and consistency only.
- Season cards and the quality-exception card remain outside both filters.
- Quality exceptions remain 100.
- Driver portraits are static and may show current team imagery.

## Environment files

For a new setup, copy `.env.example` to `.env` and
`docker/superset.env.example` to `docker/superset.env`.
Replace password and secret placeholders before starting services.

Keep existing environment files when restoring an existing installation.
Keep SUPERSET_SECRET_KEY stable.
A fresh Superset installation also requires metadata migration,
administrator creation, and initialization before sign-in.