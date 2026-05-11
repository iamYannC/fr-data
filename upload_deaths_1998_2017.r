## upload_deaths_1998_2017.r
## Extract deaths DBF files from ZIP archives for 1998-2017,
## convert to parquet, and upload to S3.
## -------------------------------------------------------------------
## Bug fix: ZIP filenames contain French accents encoded in DOS/cp437
## (e.g. varmod_décès.dbf). R's unzip() extracts them but list.files()
## returns broken encoding, and grepl() crashes with "invalid multibyte
## string". Fix: use unzip(list=TRUE) to identify the ASCII-named data
## file (DEC{YYYY}.dbf or dec{YYYY}.dbf), then extract only that file.
## -------------------------------------------------------------------

library(tidyverse)
library(nanoparquet)
library(foreign)
library(aws.s3)

# ── AWS credentials ──────────────────────────────────────────────────
env_lines <- readLines(".aws_env.txt")
for (line in env_lines) {
  parts <- strsplit(line, "=")[[1]]
  if (length(parts) >= 2) {
    key <- toupper(trimws(parts[1]))
    val <- trimws(paste(parts[-1], collapse = "="))
    val <- gsub("[\"']", "", val)
    if (key == "AWS_ACCESS_KEY_ID")     Sys.setenv(AWS_ACCESS_KEY_ID = val)
    if (key == "AWS_SECRET_KEY")        Sys.setenv(AWS_SECRET_ACCESS_KEY = val)
    if (key == "AWS_SECRET_ACCESS_KEY") Sys.setenv(AWS_SECRET_ACCESS_KEY = val)
    if (key == "AWS_DEFAULT_REGION")    Sys.setenv(AWS_DEFAULT_REGION = val)
  }
}
cat("AWS region:", Sys.getenv("AWS_DEFAULT_REGION"), "\n")

BUCKET <- "yann-fr-data"

# ── Helper: write parquet to temp file, upload to S3 ─────────────────
upload_parquet <- function(df, s3_key) {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp))
  write_parquet(df, tmp)
  put_object(file = tmp, object = s3_key, bucket = BUCKET)
  list(s3_key = s3_key, rows = nrow(df), cols = ncol(df),
       size_mb = round(file.size(tmp) / 1e6, 2))
}

# ── Helper: extract main deaths DBF from zip (encoding-safe) ────────
# Uses unzip(list=TRUE) to find the data file by ASCII name pattern,
# then extracts only that file. Avoids list.files()/grepl() on
# broken-encoding filenames from accented ZIP entries.
read_deaths_dbf <- function(zip_path, year) {
  # List zip contents — Name column has the filenames
  contents <- unzip(zip_path, list = TRUE)
  all_names <- contents$Name

  # Find the main data file: matches DEC{YYYY}.dbf (case-insensitive)
  # but NOT varmod/varlist files
  pattern <- sprintf("(?i)^dec%d\\.dbf$", year)
  data_file <- all_names[grepl(pattern, all_names, perl = TRUE)]

  if (length(data_file) == 0) {
    # Fallback: any .dbf that starts with dec/DEC and is not var*
    data_file <- all_names[grepl("(?i)^dec.*\\.dbf$", all_names, perl = TRUE)]
    data_file <- data_file[!grepl("(?i)^var", data_file)]
  }

  if (length(data_file) == 0) {
    stop("No deaths DBF found in: ", zip_path,
         "\nContents: ", paste(all_names, collapse = ", "))
  }

  # Extract only the data file to a temp directory
  tmp_dir <- tempfile(pattern = paste0("deaths_", year, "_"))
  dir.create(tmp_dir, recursive = TRUE)
  on.exit(unlink(tmp_dir, recursive = TRUE))

  unzip(zip_path, files = data_file[1], exdir = tmp_dir)
  dbf_path <- file.path(tmp_dir, data_file[1])

  # Read DBF
  df <- read.dbf(dbf_path, as.is = TRUE) |> tibble()

  # Convert column names and character data from Latin-1 to UTF-8
  names(df) <- toupper(names(df))
  names(df) <- iconv(names(df), from = "latin1", to = "UTF-8")
  df <- df |> mutate(across(where(is.character),
                            \(x) iconv(x, from = "latin1", to = "UTF-8")))

  # Convert all columns to character for consistent parquet schema
  df |> mutate(across(everything(), as.character))
}

# ══════════════════════════════════════════════════════════════════════
# DEATHS 1998-2017
# ══════════════════════════════════════════════════════════════════════
cat("\n", strrep("=", 60), "\n")
cat("  DEATHS 1998-2017\n")
cat(strrep("=", 60), "\n\n")

years <- 1998L:2017L
results <- list()
schemas <- list()

for (yr in years) {
  zip_path <- sprintf("deaths/%d/etatcivil%d_dec%d_dbase.zip", yr, yr, yr)
  cat(sprintf("  [%d] %d ... ", yr - 1997L, yr))

  if (!file.exists(zip_path)) {
    cat("SKIP — file not found\n")
    next
  }

  df <- tryCatch(
    read_deaths_dbf(zip_path, yr),
    error = function(e) {
      cat(sprintf("ERROR: %s\n", e$message))
      NULL
    }
  )
  if (is.null(df)) next

  schemas[[as.character(yr)]] <- names(df)

  s3_key <- sprintf("deaths/year=%d/part-0.parquet", yr)
  info <- upload_parquet(df, s3_key)
  results <- c(results, list(tibble(
    subject = "deaths",
    year = yr,
    s3_key = info$s3_key,
    rows = info$rows,
    cols = info$cols,
    size_mb = info$size_mb
  )))
  cat(sprintf("%s rows x %d cols -> %.1f MB\n",
              format(info$rows, big.mark = ","), info$cols, info$size_mb))

  rm(df); gc(verbose = FALSE)
}

# ══════════════════════════════════════════════════════════════════════
# ROW COUNTS
# ══════════════════════════════════════════════════════════════════════
cat("\n--- Row counts ---\n")
results_df <- list_rbind(results)
row_counts <- results_df |> select(year, rows)
print(row_counts, n = 20)
write_csv(row_counts, "deaths/row_counts_1998_2017.csv")
cat("Saved to deaths/row_counts_1998_2017.csv\n")

# ══════════════════════════════════════════════════════════════════════
# SCHEMA AUDIT
# ══════════════════════════════════════════════════════════════════════
cat("\n--- Schema audit ---\n")
all_vars <- unique(unlist(schemas))
schema_matrix <- map(schemas, \(s) all_vars %in% s) |>
  set_names(names(schemas)) |>
  as_tibble() |>
  mutate(variable = all_vars, .before = 1)
print(schema_matrix)
write_csv(schema_matrix, "deaths/schema_audit_1998_2017.csv")
cat("Saved to deaths/schema_audit_1998_2017.csv\n")

# ══════════════════════════════════════════════════════════════════════
# UPDATE MANIFEST
# ══════════════════════════════════════════════════════════════════════
cat("\n--- Updating manifest ---\n")
old_manifest <- read_csv("manifest.csv", show_col_types = FALSE)
cat("Existing manifest:", nrow(old_manifest), "rows\n")

# Remove any existing deaths 1998-2017 entries (in case of re-run)
old_manifest <- old_manifest |>
  filter(!(subject == "deaths" & year %in% years))

# Add new entries
new_manifest <- bind_rows(old_manifest, results_df) |>
  arrange(subject, year)

cat("Updated manifest:", nrow(new_manifest), "rows\n")
print(new_manifest |> filter(subject == "deaths"), n = 30)

# Save locally
write_csv(new_manifest, "manifest.csv")

# Upload to S3
tmp <- tempfile(fileext = ".csv")
write_csv(new_manifest, tmp)
put_object(file = tmp, object = "manifest.csv", bucket = BUCKET)
unlink(tmp)
cat("Manifest uploaded to s3://", BUCKET, "/manifest.csv\n", sep = "")

# ══════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════
cat("\n", strrep("=", 60), "\n")
cat("  SUMMARY\n")
cat(strrep("=", 60), "\n\n")
cat("Years processed:", nrow(results_df), "\n")
cat("Total rows:", format(sum(results_df$rows), big.mark = ","), "\n")
cat("Total parquet size:", round(sum(results_df$size_mb), 1), "MB\n")
cat("Manifest total:", nrow(new_manifest), "S3 objects\n")
cat("\nDone!\n")
