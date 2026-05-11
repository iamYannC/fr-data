## upload_births_deaths_demo.r
## Process births (1998-2023), deaths (1998-2023), demographics
## and upload to S3. Elections handled separately.
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

BUCKET <- "yann-fr-data"

upload_parquet <- function(df, s3_key) {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp))
  write_parquet(df, tmp)
  put_object(file = tmp, object = s3_key, bucket = BUCKET)
  list(s3_key = s3_key, rows = nrow(df), cols = ncol(df),
       size_mb = round(file.size(tmp) / 1e6, 2))
}

manifest <- list()

# ══════════════════════════════════════════════════════════════════════
# 1. BIRTHS (1998-2023)
# ══════════════════════════════════════════════════════════════════════
cat("\n", strrep("=", 60), "\n  BIRTHS\n", strrep("=", 60), "\n\n")

births_files <- tribble(
  ~year, ~path,
  1998L, "births/1998/etatcivil1998_nais1998_dbase.zip",
  1999L, "births/1999/etatcivil1999_nais1999_dbase.zip",
  2000L, "births/2000/etatcivil2000_nais2000_dbase.zip",
  2001L, "births/2001/etatcivil2001_nais2001_dbase.zip",
  2002L, "births/2002/etatcivil2002_nais2002_dbase.zip",
  2003L, "births/2003/etatcivil2003_nais2003_dbase.zip",
  2004L, "births/2004/etatcivil2004_nais2004_dbase.zip",
  2005L, "births/2005/etatcivil2005_nais2005_dbase.zip",
  2006L, "births/2006/etatcivil2006_nais2006_dbase.zip",
  2007L, "births/2007/etatcivil2007_nais2007_dbase.zip",
  2008L, "births/2008/etatcivil2008_nais2008_dbase.zip",
  2009L, "births/2009/etatcivil2009_nais2009_dbase.zip",
  2010L, "births/2010/etatcivil2010_naiss2010_dbase.zip",
  2011L, "births/2011/etatcivil2011_nais2011_dbase.zip",
  2012L, "births/2012/etatcivil2012_nais2012_dbase.zip",
  2013L, "births/2013/etatcivil2013_nais2013_dbase.zip",
  2014L, "births/2014/etatcivil2014_nais2014_dbase.zip",
  2015L, "births/2015/etatcivil2015_nais2015_dbase.zip",
  2016L, "births/2016/etatcivil2016_nais2016_dbase.zip",
  2017L, "births/2017/etatcivil2017_nais2017_dbase.zip",
  2018L, "births/2018/etatcivil2018_nais2018_dbase.zip",
  2019L, "births/2019/etatcivil2019_nais2019_dbase.zip",
  2020L, "births/2020/FD_NAIS_2020.parquet",
  2021L, "births2021/FD_NAIS_2021.parquet",
  2022L, "births/2022/FD_NAIS_2022.parquet",
  2023L, "births/2023/FD_NAIS_2023_csv.zip"
)

read_from_zip <- function(path, pattern_dbf, pattern_csv, encoding = "latin1") {
  tmp_dir <- tempfile(pattern = "extract_")
  dir.create(tmp_dir, recursive = TRUE)
  on.exit(unlink(tmp_dir, recursive = TRUE))
  unzip(path, exdir = tmp_dir)
  files <- list.files(tmp_dir, recursive = TRUE, full.names = TRUE)
  dbf <- files[grepl(pattern_dbf, files, perl = TRUE) & !grepl("(?i)var", files)]
  csv <- files[grepl(pattern_csv, files, perl = TRUE) & !grepl("(?i)var", files)]
  if (length(dbf) > 0) {
    df <- read.dbf(dbf[1], as.is = TRUE) |> tibble()
    names(df) <- iconv(names(df), from = encoding, to = "UTF-8")
    df <- df |> mutate(across(where(is.character),
                              \(x) iconv(x, from = encoding, to = "UTF-8")))
  } else if (length(csv) > 0) {
    df <- read_delim(csv[1], delim = ";", col_types = cols(.default = "c"),
                     locale = locale(encoding = encoding), show_col_types = FALSE)
  } else {
    stop("No data file found in zip: ", path, "\nFiles: ", paste(basename(files), collapse = ", "))
  }
  df
}

births_schemas <- list()
births_row_counts <- integer()

for (i in seq_len(nrow(births_files))) {
  yr <- births_files$year[i]
  fp <- births_files$path[i]
  cat(sprintf("  [%d/26] %d ... ", i, yr))

  df <- tryCatch({
    ext <- tools::file_ext(fp)
    if (ext == "zip") {
      read_from_zip(fp, "(?i)(nais|NAIS).*\\.dbf$", "(?i)(nais|NAIS|FD_NAIS).*\\.csv$")
    } else {
      read_parquet(fp) |> tibble()
    }
  }, error = function(e) {
    cat(sprintf("ERROR: %s\n", e$message)); NULL
  })
  if (is.null(df)) next

  names(df) <- toupper(names(df))
  df <- df |> mutate(across(everything(), as.character))

  births_row_counts[as.character(yr)] <- nrow(df)
  births_schemas[[as.character(yr)]] <- names(df)

  s3_key <- sprintf("births/year=%d/part-0.parquet", yr)
  info <- upload_parquet(df, s3_key)
  manifest <- c(manifest, list(tibble(
    subject = "births", year = yr,
    s3_key = info$s3_key, rows = info$rows,
    cols = info$cols, size_mb = info$size_mb
  )))
  cat(sprintf("%s rows x %d cols -> %.1f MB\n",
              format(info$rows, big.mark = ","), info$cols, info$size_mb))

  rm(df); gc(verbose = FALSE)
}

# Save births schema audit
all_birth_vars <- unique(unlist(births_schemas))
birth_presence <- map(births_schemas, \(s) all_birth_vars %in% s) |>
  set_names(names(births_schemas)) |>
  as_tibble() |>
  mutate(variable = all_birth_vars, .before = 1)
write_csv(birth_presence, "births/schema_audit.csv")
write_csv(tibble(year = as.integer(names(births_row_counts)),
                 rows = births_row_counts), "births/row_counts.csv")
cat("  Schema audit saved to births/schema_audit.csv\n")

rm(births_schemas, births_row_counts, all_birth_vars, birth_presence)
gc(verbose = FALSE)

# ══════════════════════════════════════════════════════════════════════
# 2. DEATHS (1998-2023)
# ══════════════════════════════════════════════════════════════════════
cat("\n", strrep("=", 60), "\n  DEATHS\n", strrep("=", 60), "\n\n")

deaths_files <- tribble(
  ~year, ~path,
  1998L, "deaths/1998/etatcivil1998_dec1998_dbase.zip",
  1999L, "deaths/1999/etatcivil1999_dec1999_dbase.zip",
  2000L, "deaths/2000/etatcivil2000_dec2000_dbase.zip",
  2001L, "deaths/2001/etatcivil2001_dec2001_dbase.zip",
  2002L, "deaths/2002/etatcivil2002_dec2002_dbase.zip",
  2003L, "deaths/2003/etatcivil2003_dec2003_dbase.zip",
  2004L, "deaths/2004/etatcivil2004_dec2004_dbase.zip",
  2005L, "deaths/2005/etatcivil2005_dec2005_dbase.zip",
  2006L, "deaths/2006/etatcivil2006_dec2006_dbase.zip",
  2007L, "deaths/2007/etatcivil2007_dec2007_dbase.zip",
  2008L, "deaths/2008/etatcivil2008_dec2008_dbase.zip",
  2009L, "deaths/2009/etatcivil2009_dec2009_dbase.zip",
  2010L, "deaths/2010/etatcivil2010_dec2010_dbase.zip",
  2011L, "deaths/2011/etatcivil2011_dec2011_dbase.zip",
  2012L, "deaths/2012/etatcivil2012_dec2012_dbase.zip",
  2013L, "deaths/2013/etatcivil2013_dec2013_dbase.zip",
  2014L, "deaths/2014/etatcivil2014_dec2014_dbase.zip",
  2015L, "deaths/2015/etatcivil2015_dec2015_dbase.zip",
  2016L, "deaths/2016/etatcivil2016_dec2016_dbase.zip",
  2017L, "deaths/2017/etatcivil2017_dec2017_dbase.zip",
  2018L, "deaths/2018/etatcivil2018_dec2018_dbase.zip",
  2019L, "deaths/2019/etatcivil2019_dec2019_dbase.zip",
  2020L, "deaths/2020/etatcivil2020_dec2020_dbase.zip",
  2021L, "deaths2021/FD_DEC_2021.csv",
  2022L, "deaths/2022/etatcivil2022_dec2022_dbase.zip",
  2023L, "deaths/2023/dec2023_fdet.zip"
)

deaths_schemas <- list()
deaths_row_counts <- integer()

for (i in seq_len(nrow(deaths_files))) {
  yr <- deaths_files$year[i]
  fp <- deaths_files$path[i]
  cat(sprintf("  [%d/26] %d ... ", i, yr))

  df <- tryCatch({
    ext <- tools::file_ext(fp)
    if (ext == "zip") {
      # Deaths 2023 zip contains a parquet, not dbf/csv
      read_from_zip(fp,
                    "(?i)(dec|DEC|fdet).*\\.dbf$",
                    "(?i)(dec|DEC|FD_DEC|fdet).*\\.csv$")
    } else if (ext == "csv") {
      read_delim(fp, delim = ";", col_types = cols(.default = "c"),
                 show_col_types = FALSE)
    } else {
      stop("Unknown extension: ", ext)
    }
  }, error = function(e) {
    # Might be a parquet inside the zip (deaths 2023)
    if (ext == "zip") {
      tryCatch({
        tmp_dir <- tempfile(pattern = "deaths_pq_")
        dir.create(tmp_dir, recursive = TRUE)
        unzip(fp, exdir = tmp_dir)
        pq_files <- list.files(tmp_dir, pattern = "\\.parquet$",
                               recursive = TRUE, full.names = TRUE)
        if (length(pq_files) > 0) {
          df <- read_parquet(pq_files[1]) |> tibble()
          unlink(tmp_dir, recursive = TRUE)
          return(df)
        }
        unlink(tmp_dir, recursive = TRUE)
        cat(sprintf("ERROR: %s\n", e$message)); NULL
      }, error = function(e2) {
        cat(sprintf("ERROR: %s\n", e2$message)); NULL
      })
    } else {
      cat(sprintf("ERROR: %s\n", e$message)); NULL
    }
  })
  if (is.null(df)) next

  names(df) <- toupper(names(df))
  df <- df |> mutate(across(everything(), as.character))

  deaths_row_counts[as.character(yr)] <- nrow(df)
  deaths_schemas[[as.character(yr)]] <- names(df)

  s3_key <- sprintf("deaths/year=%d/part-0.parquet", yr)
  info <- upload_parquet(df, s3_key)
  manifest <- c(manifest, list(tibble(
    subject = "deaths", year = yr,
    s3_key = info$s3_key, rows = info$rows,
    cols = info$cols, size_mb = info$size_mb
  )))
  cat(sprintf("%s rows x %d cols -> %.1f MB\n",
              format(info$rows, big.mark = ","), info$cols, info$size_mb))

  rm(df); gc(verbose = FALSE)
}

# Save deaths schema audit
all_death_vars <- unique(unlist(deaths_schemas))
death_presence <- map(deaths_schemas, \(s) all_death_vars %in% s) |>
  set_names(names(deaths_schemas)) |>
  as_tibble() |>
  mutate(variable = all_death_vars, .before = 1)
write_csv(death_presence, "deaths/schema_audit.csv")
write_csv(tibble(year = as.integer(names(deaths_row_counts)),
                 rows = deaths_row_counts), "deaths/row_counts.csv")
cat("  Schema audit saved to deaths/schema_audit.csv\n")

rm(deaths_schemas, deaths_row_counts, all_death_vars, death_presence)
gc(verbose = FALSE)

# ══════════════════════════════════════════════════════════════════════
# 3. DEMOGRAPHICS (aggregate by commune)
# ══════════════════════════════════════════════════════════════════════
cat("\n", strrep("=", 60), "\n  DEMOGRAPHICS\n", strrep("=", 60), "\n\n")

for (file_info in list(
  list(path = "demographics/births_by_commune.csv",
       data_file = "DS_ETAT_CIVIL_NAIS_COMMUNES_data.csv",
       s3_name = "births_by_commune"),
  list(path = "demographics/deaths_by_commune.csv",
       data_file = "DS_ETAT_CIVIL_DECES_COMMUNES_data.csv",
       s3_name = "deaths_by_commune")
)) {
  if (file.exists(file_info$path)) {
    cat(sprintf("  %s ... ", file_info$s3_name))
    tmp_dir <- tempfile(pattern = "demo_")
    dir.create(tmp_dir, recursive = TRUE)
    unzip(file_info$path, exdir = tmp_dir)
    csv_path <- file.path(tmp_dir, file_info$data_file)
    df <- read_delim(csv_path, delim = ";", col_types = cols(.default = "c"),
                     show_col_types = FALSE)
    unlink(tmp_dir, recursive = TRUE)
    names(df) <- toupper(names(df))
    s3_key <- sprintf("demographics/%s.parquet", file_info$s3_name)
    info <- upload_parquet(df, s3_key)
    manifest <- c(manifest, list(tibble(
      subject = paste0("demographics_", file_info$s3_name), year = NA_integer_,
      s3_key = info$s3_key, rows = info$rows,
      cols = info$cols, size_mb = info$size_mb
    )))
    cat(sprintf("%s rows -> %.1f MB\n", format(info$rows, big.mark = ","), info$size_mb))
    rm(df); gc(verbose = FALSE)
  } else {
    cat(sprintf("  [skip] %s not found\n", file_info$path))
  }
}

# ══════════════════════════════════════════════════════════════════════
# 4. MANIFEST — merge with existing weddings
# ══════════════════════════════════════════════════════════════════════
cat("\n", strrep("=", 60), "\n  MANIFEST\n", strrep("=", 60), "\n\n")

old_manifest <- read_csv("manifest.csv", show_col_types = FALSE)
new_manifest <- list_rbind(manifest)
full_manifest <- bind_rows(
  old_manifest |> filter(subject == "weddings"),
  new_manifest
) |>
  arrange(subject, year)

cat("Full manifest:\n")
print(full_manifest, n = 100)

write_csv(full_manifest, "manifest.csv")
tmp <- tempfile(fileext = ".csv")
write_csv(full_manifest, tmp)
put_object(file = tmp, object = "manifest.csv", bucket = BUCKET)
unlink(tmp)

cat("\nManifest uploaded to s3://", BUCKET, "/manifest.csv\n", sep = "")
cat("Total S3 objects:", nrow(full_manifest), "\n")
cat("Total rows:", format(sum(full_manifest$rows, na.rm = TRUE), big.mark = ","), "\n")
cat("Total size:", round(sum(full_manifest$size_mb, na.rm = TRUE), 1), "MB\n")
cat("\nDone! Elections will be processed separately.\n")
