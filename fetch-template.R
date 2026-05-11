# This script holds a template to load death data
# It fetches data across all years
# It's filtered on DEPDEC (department) 972

# using union_by_name ensures that even if the schema change, it will still execute.


# Setup -------------------------------------------------------------------

library(DBI)
library(duckdb)
library(tidyverse)

setwd('C:/Users/97253/Documents/fr-data')

con <- dbConnect(duckdb())
dbExecute(con, "INSTALL httpfs; LOAD httpfs;")

dbExecute(con, sprintf("
  CREATE SECRET s3_creds (
    TYPE s3,
    KEY_ID '%s',
    SECRET '%s',
    REGION '%s'
  )",
                       Sys.getenv("AWS_ACCESS_KEY_ID"),
                       Sys.getenv("AWS_SECRET_ACCESS_KEY"),
                       Sys.getenv("AWS_DEFAULT_REGION")
))


# Deaths view -------------------------------------------------------------

dbExecute(con, "
  CREATE VIEW deaths AS
  SELECT * FROM read_parquet(
    's3://yann-fr-data/deaths/**/*.parquet',
    hive_partitioning = true,
    union_by_name = true
  )
")

deaths_972 <- tbl(con, "deaths") |>
  filter(DEPDEC == "972") |> # show_query() -- the sql qury thats being executed under the hood
  collect()

# weddings ----------------------------------------------------------------

dbExecute(con, "
  CREATE VIEW weddings AS
  SELECT * FROM read_parquet(
    's3://yann-fr-data/weddings/**/*.parquet',
    hive_partitioning = true,
    union_by_name = true
  )
")

weddings_972 <- tbl(con, "weddings") |>
  filter(DEPMAR == "972") |> # show_query() -- the sql qury thats being executed under the hood
  collect()

# Verification ------------------------------------------------------------
# Since the schema changed, I use union_by_name. this can cause silent data drop.
# The manifest was created prior to uploading files to S3 and stores the original csv data

# Manifest:
manifest <- read_csv("manifest.csv") |>
  select(subject,year, rows)



# S3 deaths:
duckdb_counts_death <- tbl(con, "deaths") |>
  count(year) |>
  arrange(year) |>
  collect()

left_join(manifest |> filter(subject=='deaths'),
          duckdb_counts_death, by = 'year') |> 
  mutate(diff = rows - n) |> filter(diff != 0)


# S3 weddings:
duckdb_counts_wed <- tbl(con, "weddings") |>
  count(year) |>
  arrange(year) |>
  collect()

left_join(manifest |> filter(subject=='weddings'),
          duckdb_counts_wed, by = 'year') |> 
  mutate(diff = rows - n) |> filter(diff != 0)


dbDisconnect(con, shutdown = TRUE)
