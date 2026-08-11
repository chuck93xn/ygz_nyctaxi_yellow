# NYC Taxi — Medallion Data Pipeline on Databricks

An end-to-end data engineering project on Azure Databricks using NYC TLC Yellow Taxi trip data, built to explore medallion architecture, incremental processing, SCD Type 2 and Delta Lake — developed progressively across four stages, from a full-load pipeline to a modularised, Git-tracked project with governed external data sharing.

## Tech Stack

Azure · Databricks · PySpark · Delta Lake · Unity Catalog · Databricks Jobs · Python · SQL · Git

## What I Built

- **Medallion pipeline** (Landing → Bronze → Silver → Gold) using PySpark and Delta Lake, sourced from the NYC TLC public dataset.
- **Incremental loading** — evolved the pipeline from full-overwrite to append-based processing at Bronze/Silver/Gold, with existence-check logic at Landing to detect new data.
- **SCD Type 2** — implemented history tracking for the Taxi Zone Lookup reference table using Delta Lake `MERGE` (check-for-updates → close out changed rows → insert new current versions → insert brand-new rows).
- **Orchestration** — built a multi-task Databricks Job using parameters and Task Values to drive conditional downstream execution.
- **Modularisation** — refactored transformation logic out of notebooks into reusable Python modules; set up Git-based version control.
- **Unity Catalog & governance** — configured external tables and external locations, with an Azure Storage integration to simulate governed data sharing with other teams.
- **Data quality** — schema validation at Bronze; null, duplicate and invalid-record checks at Silver.

## Data

NYC TLC Yellow Taxi trip records, Dec 2025 – May 2026 (~400K records/month), plus the Taxi Zone Lookup reference table.

## Project Approach

This project was built in four progressive stages rather than all at once, mirroring how I'd ramp up on a new stack on the job:

1. Full-load medallion pipeline (Landing → Gold)
2. Incremental loading + SCD Type 2
3. Modularisation + Git
4. External data export via Unity Catalog

## What I'd Improve Next

- Bring in more data to properly exercise dimensional/fact-and-dimension modelling at a more realistic scale.
- Add business-rule-level data quality checks (e.g. flagging implausible trip durations or pickup/dropoff time gaps).
