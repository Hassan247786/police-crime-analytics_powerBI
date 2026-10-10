# Crime Analytics Pipeline and Dashboard

This project turns raw UK police crime records into a single reporting dataset, then presents that dataset as a Power BI dashboard for police force leadership. It covers four forces over 35 months, built as an end to end data engineering pipeline with validation at every stage.

## Overview

The pipeline ingests around two million raw crime records across four police forces, cleans then validates them, enriches them with population figures plus deprivation data, then aggregates everything to a reporting grain of force by month by crime type. The result is one tidy dataset of 1,960 rows that drives the dashboard. Every transformation is checked by an assertion, so a fault stops the run rather than reaching the output.

## What is in this repository

- `crime_data_pipeline.py` is the Python pipeline that builds the reporting dataset from the raw sources.
- `reporting_dataset_for_powerBI.csv` is the finished output, one row per force, month and crime type.
- `crime_dashboard.pbix` is the Power BI dashboard built on that reporting dataset.
- `data_dictionary_including_links_to_data_sources.pdf` defines every column, lists the data sources, records the assumptions.
- `dashboard_information.pdf` is the written summary of the dashboard, its audience, its limitations.

## The reporting dataset

One row is a unique combination of police force, month and crime type, giving 1,960 rows across four forces for the period July 2023 to May 2026. Each row carries the crime count, the resident population, a crime rate per 1,000 residents, a mean deprivation decile for the force. Forces should be compared on the rate rather than raw counts, because the four differ greatly in size. Full column definitions sit in the data dictionary.

## Key findings

Adjusted for population, West Midlands records the highest crime rate at 80.4 per 1,000 residents, with Essex the lowest at 60.9. Higher rates track the more deprived force areas, so deprivation is useful context for reading the figures. Recorded crime follows a repeating seasonal pattern, with the busier months returning each full year.

## How to reproduce

1. Download the four source datasets from the links in the data dictionary (police.uk crime data, ONS population estimates, English Indices of Deprivation 2025, the ONS local authority to police force lookup).
2. Save them where the pipeline expects, then update the file paths in the CONFIGURATION block near the top of `crime_data_pipeline.py` to point at your own copies.
3. Run the pipeline with Python 3 and pandas installed.
4. The script prints a validation report, then writes `reporting_dataset_final.csv`, which is the same dataset provided here.
5. Open `crime_dashboard.pbix` in Power BI Desktop to explore the dashboard. Repoint its data source at the reporting CSV if it asks, since the original path is specific to the machine that built it.

## How to validate

Reproduction is self checking. The pipeline asserts that row counts are preserved through every join, that the reporting grain is unique, that required fields hold no nulls, that categories match the expected forces, that every measure falls inside a sensible range. Any breach stops the run with a clear message, so a clean run is itself the evidence that the output is sound. The printed summary at the end reports the row count, the forces, the period, the total crimes, which you can match against the figures quoted above.

## Requirements

Python 3 with pandas for the pipeline. Power BI Desktop to open the dashboard file.