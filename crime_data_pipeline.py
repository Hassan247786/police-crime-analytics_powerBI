"""
Crime Data Pipeline and Reporting Dataset
=========================================
Police Force Analytics Unit, Phase 1 (Data Engineering).

Purpose
-------
Build one reporting ready CSV, aggregated to a single reporting grain of
police force x month x crime type, for downstream consumption in Power BI.

Pipeline stages
---------------
1. Ingestion                 read the crime CSVs file by file, plus three enrichment sources
2. Cleaning and validation   profile the raw data, then apply explicit integrity checks
3. Feature engineering       derive time attributes, standardise categories, prepare enrichment
4. Aggregation               collapse crime records to the reporting grain and join enrichment
5. Export                    validate the final table and write the reporting dataset

The chosen 4 police forces are:
    - West Midlands
    - Staffordshire
    - Nottinghamshire
    - Essex

Enrichment datasets and why they were chosen
--------------------------------------------
- ONS population estimates by police force area. Chosen because the four forces
  differ greatly in size, so raw crime counts alone would rank forces by
  population rather than by crime. Provides the denominator for the normalised
  crimes per 1,000 residents measure.
- English Indices of Deprivation 2025 (File 7). Chosen to provide the
  socioeconomic context needed to relate crime to deprivation.
- ONS Local Authority District to Police Force Area lookup. Not an analytical
  dataset. Used only to introduce police forces to deprivation and in turn to 
  the working crime dataset.

According to ONS: 'Unrounded estimates are published to enable and encourage further
calculations and analysis.' The estimates are therefore held unrounded here so
that the rate calculation uses full precision. 
"""

# %%
import glob
import pandas as pd

# =====================================================================
# CONFIGURATION
# =====================================================================
# Held in one place so the pipeline can be repointed, or rerun for a different
# set of forces, without editing the logic below.

# file paths
file_path_crime_dataset = 'E:/Rockborne work/python/pandas/crime_files/*/*-street.csv'
file_path_populations_dataset = 'E:/Rockborne work/python/pandas/populations_by_area.csv'
file_path_deprivation_dataset = 'E:/Rockborne work/python/pandas/deprivation.csv'
file_path_lad_codes_to_pfa = 'E:/Rockborne work/python/pandas/lad_codes_to_PFA.csv'
file_path_reporting_dataset = 'E:/Rockborne work/python/pandas/reporting_dataset_final.csv'
# the 4 police forces, named as they appear in the two enrichment datasets
police_forces = ['West Midlands', 'Staffordshire', 'Nottinghamshire', 'Essex']
# the crime data appends ' Police' to each force name, so the expected raw values differ
police_forces_as_reported = []
for one_force in police_forces:
    police_forces_as_reported.append(f'{one_force} Police')
# the reporting grain: one row is a unique combination of these three columns
reporting_grain = ['police_force_name', 'month', 'crime_type']

# columns that must never be null in the exported reporting dataset
required_reporting_fields = [
    'police_force_name', 'month', 'crime_type', 'crime_count',
    'year', 'month_number', 'total_population',
    'crime_rate_per_1000', 'mean_imd_decile',
]
# latest year present in the ONS population estimates, used for the carry forward
latest_population_year = 2024

# =====================================================================
# VALIDATION HELPERS
# =====================================================================
# Every check raises an AssertionError carrying a message that explains what a
# failure would mean. A problem therefore stops the pipeline rather than
# silently reaching the exported dataset.

def check_row_count_unchanged(rows_before, rows_after, stage_name):
    """Row counts: confirm a transformation preserved the row count."""
    assert rows_before == rows_after, (
        f"[FAIL] {stage_name}: row count changed, {rows_before:,} - {rows_after:,}. "
        f"A left join that changes the row count indicates an unintended many to "
        f"many join on the key columns."
    )
    print(f"[PASS] {stage_name}: row count preserved at {rows_after:,}")
def check_unique_key(df, key_columns, stage_name):
    """Duplicate check: confirm the given columns uniquely identify a row."""
    duplicate_count = df.duplicated(subset=key_columns).sum()  # count rows that duplicate an existing key combination
    assert duplicate_count == 0, (
        f"[FAIL] {stage_name}: {duplicate_count:,} duplicate rows found on "
        f"{key_columns}. Measures would be double counted at this grain."
    )
    print(f"[PASS] {stage_name}: {key_columns} uniquely identifies all {len(df):,} rows")
def check_no_nulls(df, columns, stage_name):
    """Null check: confirm required fields contain no missing values."""
    null_counts = df[columns].isna().sum()
    columns_with_nulls = null_counts[null_counts > 0]
    assert columns_with_nulls.empty, (
        f"[FAIL] {stage_name}: missing values found in required fields:\n"
        f"{columns_with_nulls}"
    )
    print(f"[PASS] {stage_name}: no missing values across {len(columns)} required fields")
def check_expected_categories(df, column, expected_values, stage_name):
    """Category validation: compare the observed categories with the expected set to find any unexpected values."""
    unexpected_values = set(df[column].dropna().unique()) - set(expected_values)
    assert not unexpected_values, (
        f"[FAIL] {stage_name}: unexpected values in '{column}': "
        f"{sorted(unexpected_values)}. Categorical values must be consistent."
    )
    print(f"[PASS] {stage_name}: '{column}' contains only the expected values")

def check_value_range(series, minimum, maximum, stage_name):
    """Range check: confirm a numeric measure falls within its plausible bounds."""
    out_of_range = (~series.between(minimum, maximum)).sum()
    assert out_of_range == 0, (
        f"[FAIL] {stage_name}: {out_of_range:,} values fall outside the expected "
        f"range [{minimum}, {maximum}]."
    )
    print(f"[PASS] {stage_name}: all values within [{minimum}, {maximum}]")

# %%
# =====================================================================
# STAGE 1: INGESTION LAYER
# =====================================================================
# The crime data is read iteratively, one monthly file per force at a time,
# rather than as a single load. Only the required columns are kept from each
# file and the frames are collected for one concatenation at the end. Peak
# memory is therefore driven by the largest single file rather than by the full
# dataset of roughly 2 million rows.
all_dfs = []
crime_file_row_counts = {}
for one_filename in glob.glob(file_path_crime_dataset):
    one_df = pd.read_csv(
        one_filename,
        usecols=[1, 2, 7, 8, 9],
        header=0,  # row which contains the column names
        names=['month', 'police_force_name',
               'lsoa_code', 'lsoa_name', 'crime_type'],  # rename - positional according to original column positions
    )
    crime_file_row_counts[one_filename] = len(one_df)  # captured for the row count below
    all_dfs.append(one_df)
# fail immediately if the glob pattern matched nothing, rather than producing an empty dataset
assert all_dfs, (
    f"[FAIL] Ingestion: no crime files matched '{file_path_crime_dataset}'. "
    f"Check the folder path and that the files end in '-street.csv'."
)
crime_rows_read = sum(crime_file_row_counts.values())
print(f"Ingestion: read {len(all_dfs)} crime files, {crime_rows_read:,} rows in total")
# stack the monthly files vertically into a single crime dataset
crime_df = pd.concat(all_dfs)
# row counts: concatenation must neither drop nor invent rows
check_row_count_unchanged(crime_rows_read, len(crime_df), "Concatenate crime files")

# population
population_df = pd.read_csv(file_path_populations_dataset, skiprows=3)
population_df = population_df.rename(columns={'PFA 2023 Name': 'police_force_name'})

# deprivation
# imd_score, lad_name and lsoa_population are read but not carried into the final
# dataset. They are retained here so that the alternative force level metrics
# considered (a population weighted mean decile, or a mean score) can be produced
# without changing the ingestion step.
list_of_columns_deprivation = [
    'LSOA code (2021)',
    'Local Authority District code (2024)',
    'Local Authority District name (2024)',
    'Index of Multiple Deprivation (IMD) Score',
    'Index of Multiple Deprivation (IMD) Decile (where 1 is most deprived 10% of LSOAs)',
    'Total population: mid 2022']
deprivation_df = pd.read_csv(file_path_deprivation_dataset,
                             usecols=list_of_columns_deprivation)
deprivation_df.columns = ['lsoa_code', 'lad_code', 'lad_name',
                          'imd_score', 'imd_decile', 'lsoa_population']

# LAD to PFA lookup dataset
lad_to_pfa_df = pd.read_csv(file_path_lad_codes_to_pfa, usecols=['LAD21CD', 'PFA21NM'])

print(f"Ingestion: population {len(population_df):,} rows, "
      f"deprivation {len(deprivation_df):,} rows, "
      f"LAD to PFA lookup {len(lad_to_pfa_df):,} rows")

# %%
# =====================================================================
# STAGE 2: CLEANING AND VALIDATION LAYER
# =====================================================================

# ---------------------------------------------------------------------
# Data quality assessment
# ---------------------------------------------------------------------
# The raw crime data is profiled before any cleaning, so that the issues being
# handled are recorded rather than silently corrected.

print("DATA QUALITY ASSESSMENT: raw crime records")
print(f"Rows: {len(crime_df):,}")
print(f"Columns: {list(crime_df.columns)}")
print("\nMissing values per column:")
print(crime_df.isna().sum())
print("\nRows per police force:")
print(crime_df['police_force_name'].value_counts())
print(f"\nDistinct crime types: {crime_df['crime_type'].nunique()}")
print(crime_df['crime_type'].value_counts())
print(f"\nDistinct months: {crime_df['month'].nunique()}")
print(f"Reporting period: {crime_df['month'].min()} to {crime_df['month'].max()}")

# Assessment findings and the decisions taken:
#
# 1. Missing values. lsoa_code and lsoa_name contain nulls, because a proportion
#    of recorded crimes have no location assigned. Neither column forms part of
#    the reporting grain and both are dropped by the aggregation, so these nulls
#    cannot affect any reported measure. They are read in only so that the data
#    quality position is measured and documented here.
# 2. Duplicates. No de-duplication is applied to the crime records. Repeated
#    identical rows are genuine separate incidents of the same crime type, in
#    the same force, month and LSOA, not data duplication. Removing them would
#    undercount crime. Duplication is instead controlled at the reporting grain,
#    which is where it would actually cause double counting.
# 3. Inconsistent categories. crime_type is drawn from the standardised
#    police.uk category list and is validated below rather than cleaned, since
#    no inconsistent spellings are expected across files.
# 4. crime_type has no missing values, which supports the assumption used in the
#    aggregation that one row equals one recorded crime.

# ---------------------------------------------------------------------
# Explicit validation checks on the ingested crime data
# ---------------------------------------------------------------------
check_no_nulls(crime_df, ['month', 'police_force_name', 'crime_type'],
               "Crime records: fields required for the reporting grain")
# category validation: only the four expected forces should be present, which
# also confirms the correct files were downloaded
check_expected_categories(crime_df, 'police_force_name', police_forces_as_reported,
                          "Crime records: police force names")
# confirm every force covers the same set of months. A force covering fewer
# months would mean a monthly download is missing, which would bias every
# cross force comparison in the final dataset.
months_per_force = crime_df.groupby('police_force_name')['month'].nunique()
print("\nDistinct months per force:")
print(months_per_force)
assert months_per_force.nunique() == 1, (
    "[FAIL] Crime records: the forces do not cover the same number of months. "
    "A monthly download is probably missing for at least one force, which would "
    "bias any comparison between forces."
)
print(f"[PASS] Crime records: all {len(police_forces)} forces cover the same "
      f"{months_per_force.iloc[0]} months")

# %%
# =====================================================================
# STAGE 3: FEATURE ENGINEERING AND TRANSFORMATION
# =====================================================================

# ---------------------------------------------------------------------
# Crime records: parse dates, then optimise memory
# ---------------------------------------------------------------------
# The month field is parsed into timestamp objects so that the year and month
# can be derived individually further down. The source value is a year and month
# only, so every value parses to the first of the month. The day component is
# therefore a convention of the datetime type and carries no meaning, and is
# documented as such in the data dictionary.
crime_df['month'] = pd.to_datetime(crime_df['month'], format='%Y-%m')
# The four text columns are low cardinality (4 forces, roughly 14 crime types)
# repeated across roughly 2 million rows, so the category dtype stores each
# distinct value once. This is a memory optimisation only and does not change
# any value.
initial_memory_usage = crime_df.memory_usage(deep=True).sum()
crime_df[['police_force_name', 'lsoa_code', 'lsoa_name', 'crime_type']] = (
    crime_df[['police_force_name', 'lsoa_code', 'lsoa_name', 'crime_type']]
    .astype('category'))
memory_saved = int((initial_memory_usage - crime_df.memory_usage(deep=True).sum()) / (1024 ** 2)) 
print(f"\nMemory optimisation: category dtype saved approximately {memory_saved} MB "
      f"across {len(crime_df):,} rows")
# ---------------------------------------------------------------------
# Population: convert to numeric, total by force and year, filter to 4 forces
# ---------------------------------------------------------------------
# The ONS file holds one row per force per year, with the population split
# across single year of age and sex columns (F0, F1 ... M84, M85). The total
# population for a force in a year is therefore the sum across every age and sex
# column on that row.
#
# The columns are selected by pattern rather than by a fixed label slice (originally 
# this was set using .loc[:, 'F0':'M85'] but is changed here), so that
# the oldest age bands cannot be clipped if the file layout changes. Clipping
# them would silently understate the denominator and overstate every rate.
age_sex_columns = population_df.filter(regex=r'^[FM]\d+').columns 
print(f"\nPopulation: {len(age_sex_columns)} age and sex columns identified, "
      f"'{age_sex_columns[0]}' to '{age_sex_columns[-1]}'")
assert len(age_sex_columns) > 100, (
    f"[FAIL] Population: only {len(age_sex_columns)} age and sex columns found. "
    f"Roughly 180 are expected (about 90 single years of age for each sex), so "
    f"the file layout has changed and the totals would be understated."
)
population_df['total_population'] = (
    population_df[age_sex_columns]  # every sex-age population column
    .apply(lambda s: s.str.replace(',', '')  # use lambda as its a one-off use case for this task
           .astype('int'))
    .sum(axis=1))  # sum ACROSS the rows, getting 1 sum per row

population_df = (
    population_df
    .loc[population_df['police_force_name']
         .isin(police_forces),
         ['police_force_name', 'Year', 'total_population']]
    .reset_index(drop=True))  # remove old index

# validation: the population lookup must hold exactly one row per force per year,
# otherwise the join onto the crime data would duplicate crime rows
check_unique_key(population_df, ['police_force_name', 'Year'],
                 "Population: one row per force per year")
check_no_nulls(population_df, ['police_force_name', 'Year', 'total_population'],
               "Population: required fields")
assert set(population_df['police_force_name']) == set(police_forces), (
    f"[FAIL] Population: expected {sorted(police_forces)} but found "
    f"{sorted(set(population_df['police_force_name']))}."
)
print(f"[PASS] Population: all {len(police_forces)} forces present, "
      f"years {population_df['Year'].min()} to {population_df['Year'].max()}")

# ---------------------------------------------------------------------
# Deprivation: roll LSOA level deprivation up to police force level
# ---------------------------------------------------------------------
# The deprivation file is published at LSOA level and carries a Local Authority
# District code, but no police force. The ONS lookup is used as a bridge so that
# each LSOA can be tagged with the force it falls within.
#
# The lookup is published at LAD to Community Safety Partnership to PFA level,
# so one LAD can appear on several rows. It is reduced to unique LAD to force
# pairs first. Without this the join would multiply LSOA rows (an unintended
# many to many join) and distort the force averages.
lad_to_pfa_df = lad_to_pfa_df.drop_duplicates()
check_unique_key(lad_to_pfa_df, ['LAD21CD'],
                 "LAD to PFA lookup: one row per local authority district")

# set the indexes of the two dataframes before joining
deprivation_rows_before_join = len(deprivation_df)
lad_to_pfa_df = lad_to_pfa_df.set_index('LAD21CD')
deprivation_df = deprivation_df.set_index('lad_code')
deprivation_df = deprivation_df.join(lad_to_pfa_df, how='left')

# row counts: confirms no many to many join occurred
check_row_count_unchanged(deprivation_rows_before_join, len(deprivation_df),
                          "Deprivation joined to LAD to PFA lookup")

# filter out to only those rows matching the police forces we need. LSOAs outside
# the four forces are expected to be unmatched by the lookup and are removed here.
deprivation_df = (
    deprivation_df
    .loc[deprivation_df['PFA21NM']
         .isin(police_forces)]).reset_index()

assert set(deprivation_df['PFA21NM']) == set(police_forces), (
    f"[FAIL] Deprivation: expected {sorted(police_forces)} after filtering but "
    f"found {sorted(set(deprivation_df['PFA21NM']))}. Check the force name "
    f"spellings used in the lookup."
)
print(f"[PASS] Deprivation: {len(deprivation_df):,} LSOAs retained across "
      f"{len(police_forces)} forces")

# get a single imd_decile value per police force area.
# METRIC: the unweighted mean IMD decile of the LSOAs within each force.
# The decile is used rather than the raw score because it is nationally
# referenced and therefore comparable between forces, and because the official
# research report recommends ranks and deciles over the score for interpretation.
# Deprivation is treated as static across the reporting period, since the
# Indices of Deprivation 2025 are a single snapshot.
# Note on direction: a LOWER value means MORE deprived, because decile 1 is the
# most deprived 10% of LSOAs nationally.
deprivation_df_by_pfa = deprivation_df.groupby('PFA21NM')['imd_decile'].mean()

# validation: one value per force, each within the valid decile range
assert len(deprivation_df_by_pfa) == len(police_forces), (
    f"[FAIL] Deprivation: expected one value per force, got "
    f"{len(deprivation_df_by_pfa)}."
)
check_value_range(deprivation_df_by_pfa, 1, 10,
                  "Deprivation: mean IMD decile per force")
print("\nMean IMD decile per force (lower means more deprived):")
print(deprivation_df_by_pfa.round(3))


# %%
# =====================================================================
# STAGE 4: AGGREGATION FOR REPORTING
# =====================================================================
"""The final output should not remain at individual crime-record level;
instead, it should be aggregated to a single, clearly defined reporting grain (e.g. police force
x month x crime category)."""

# make it so that 1 row is a UNIQUE combination of the police force, month and the crime_type.
# observed=True is set explicitly so that only combinations that actually occur
# are returned. With categorical grouping columns the alternative is the full
# cartesian product, which would create rows for combinations that never happened.
crime_summary_df = (crime_df
                    .groupby(reporting_grain,
                             as_index=False,  # remove the multi-indexing
                             observed=True)
                    .size()                   # assumption: number of rows per crime category = number of crimes committed in that category
                    .rename(columns={'size': 'crime_count'}))

# row counts: every raw crime record must be accounted for exactly
# once in the aggregated counts
check_row_count_unchanged(len(crime_df), crime_summary_df['crime_count'].sum(),
                          "Aggregation to reporting grain: total crimes preserved")
check_unique_key(crime_summary_df, reporting_grain,
                 "Aggregation to reporting grain: uniqueness")

# ---------------------------------------------------------------------
# Derive time attributes
# ---------------------------------------------------------------------
# year supports filtering and the population join. month_number (1 to 12)
# supports the seasonal questions, since it lines up the same calendar month
# across different years, which the full date cannot do on its own.
crime_summary_df['year'] = crime_summary_df['month'].dt.year
crime_summary_df['month_number'] = crime_summary_df['month'].dt.month

# ---------------------------------------------------------------------
# Handling the temporal mismatch between crime and population
# ---------------------------------------------------------------------
# The crime data is monthly, the population estimates are annual (mid year,
# 30 June). Each crime month is therefore matched to the population estimate for
# its own calendar year.
#
# The published estimates end at mid 2024, so crime months in 2025 and 2026 have
# no estimate of their own. The latest available estimate is carried forward for
# those months, which is recorded as an assumption in the data dictionary.
# Population drifts by roughly 1 per cent per year, so the effect on the rates
# for the most recent months is under 2 per cent. Rows are carried forward rather
# than filtered out, because filtering would discard roughly a third of the
# reporting period and break the questions about change over time.
crime_summary_df['population_year'] = (
    crime_summary_df['year']
    .clip(upper=latest_population_year))  # any year value > 2024 = 2024, due to population data not extending to 2026

# ---------------------------------------------------------------------
# Standardise the police force name, then join the population enrichment
# ---------------------------------------------------------------------
# The two datasets name the same forces differently: the crime data appends
# ' Police' (for example 'West Midlands Police') while the ONS data does not.
# The crime values are standardised to the ONS form so that the join key matches.
# Without this the join would return nulls for every row.
crime_summary_df['police_force_name'] = (
    crime_summary_df['police_force_name'].str.replace(' Police', '', regex=False))

check_expected_categories(crime_summary_df, 'police_force_name', police_forces,
                          "Reporting dataset: standardised force names")

# perform the join of the two dataframes using unique combinations of police_force_name and the year.
# The population lookup is unique on force and year (validated above) while the
# crime data has many rows per force and year, so this is a safe one to many join.
crime_rows_before_population_join = len(crime_summary_df)
crime_summary_df = crime_summary_df.join(
    population_df.set_index(['police_force_name', 'Year'])['total_population'],
    on=['police_force_name', 'population_year'],  # what columns to match against the above index
    how='left'  # maintain crime_summary rows, attach population where it matches, NaN otherwise
)
check_row_count_unchanged(crime_rows_before_population_join, len(crime_summary_df),
                          "Population joined to reporting dataset")
check_no_nulls(crime_summary_df, ['total_population'],
               "Population join: every row matched a population figure")

# make a column for number of crimes per 1000 residents
# crimes per 1,000 residents for this force/month/category = (crime_count / total_population) * 1000
crime_summary_df['crime_rate_per_1000'] = (
    crime_summary_df['crime_count'] / crime_summary_df['total_population'] * 1000)

# sanity check that the crime rates are plausible rather than absurd values
print(f"\nCrime rate per 1,000 residents: "
      f"min {crime_summary_df['crime_rate_per_1000'].min():.3f}, "
      f"median {crime_summary_df['crime_rate_per_1000'].median():.3f}, "
      f"max {crime_summary_df['crime_rate_per_1000'].max():.3f}")

# ---------------------------------------------------------------------
# Join the deprivation enrichment
# ---------------------------------------------------------------------
# Deprivation is a single value per force and does not vary by month or crime
# type, so it is joined on the force alone. This is again one to many: one
# deprivation value fans out across all of that force's rows.
crime_rows_before_deprivation_join = len(crime_summary_df)
crime_summary_df = crime_summary_df.join(
    deprivation_df_by_pfa.rename('mean_imd_decile'),  # name the Series so it becomes a column
    on='police_force_name',
    how='left')
check_row_count_unchanged(crime_rows_before_deprivation_join, len(crime_summary_df),
                          "Deprivation joined to reporting dataset")
check_no_nulls(crime_summary_df, ['mean_imd_decile'],
               "Deprivation join: every row matched a deprivation figure")


# %%
# =====================================================================
# STAGE 5: EXPORT LAYER
# =====================================================================

# population_year was scaffolding for the carry forward join only. It is not a
# fact about the crime record and would be confusing downstream (a 2026 row
# showing 2024), so it is dropped before export. The carry forward itself is
# recorded in the data dictionary instead.
crime_summary_df = crime_summary_df.drop(columns=['population_year'])
crime_summary_df = crime_summary_df.reset_index(drop=True)  # a precautionary reset to a clean 0,1,2,3 counter


def run_final_validation(df):
    """Run every integrity check required of the final reporting dataset."""
    print("\n" + "=" * 70)
    print("FINAL VALIDATION: reporting dataset")
    # no unintended duplication at the reporting grain
    check_unique_key(df, reporting_grain, "Reporting grain uniqueness")
    # no missing values in required reporting fields
    check_no_nulls(df, required_reporting_fields, "Required reporting fields")
    # clean, consistent categorical values
    check_expected_categories(df, 'police_force_name', police_forces,
                              "Police force names")
    # measures fall within plausible bounds
    check_value_range(df['crime_count'], 1, df['crime_count'].max(),
                      "Crime counts are positive")
    check_value_range(df['mean_imd_decile'], 1, 10,
                      "Mean IMD decile within the valid decile range")
    check_value_range(df['month_number'], 1, 12,
                      "Month number within 1 to 12")
    print("\nAll validation checks passed.")


run_final_validation(crime_summary_df)

# ---------------------------------------------------------------------
# High level statistics for the exported dataset
# ---------------------------------------------------------------------
print("REPORTING DATASET SUMMARY")
print(f"Grain                : {' x '.join(reporting_grain)}")
print(f"Rows                 : {len(crime_summary_df):,}")
print(f"Columns              : {len(crime_summary_df.columns)}")
print(f"Police forces        : {crime_summary_df['police_force_name'].nunique()}")
print(f"Months covered       : {crime_summary_df['month'].nunique()}")
print(f"Reporting period     : {crime_summary_df['month'].min():%Y-%m} to "
      f"{crime_summary_df['month'].max():%Y-%m}")
print(f"Crime types          : {crime_summary_df['crime_type'].nunique()}")
print(f"Total crimes covered : {crime_summary_df['crime_count'].sum():,}")
print(f"Source records read  : {crime_rows_read:,}")
print("\nColumn data types:")
print(crime_summary_df.dtypes)
print("\nTotal crimes by police force:")
print(crime_summary_df.groupby('police_force_name', observed=True)['crime_count'].sum())

# export the final dataframe to a csv ready for analysis.
# index=False so that the row counter is not written as an unnamed column.
crime_summary_df.to_csv(file_path_reporting_dataset, index=False)
print(f"\nReporting dataset exported to: {file_path_reporting_dataset}")