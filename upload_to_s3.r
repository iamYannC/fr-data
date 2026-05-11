## upload_to_s3.r
## Converts all datasets to parquet, uploads to S3 with Hive-style layout,
## and generates a manifest.
## -------------------------------------------------------------------
## S3 layout:
##   s3://fr-data/{subject}/year={YYYY}/part-0.parquet
##   s3://fr-data/manifest.csv
## -------------------------------------------------------------------

library(tidyverse)
library(nanoparquet)
library(aws.s3)

# ── Load AWS credentials ─────────────────────────────────────────────
env_lines <- readLines(".aws_env.txt")
for (line in env_lines) {
  parts <- str_split_1(line, "=")
  if (length(parts) >= 2) {
    key <- str_trim(parts[1]) |> str_to_upper()
    val <- str_trim(paste(parts[-1], collapse = "=")) |> str_remove_all('["\']')
    # aws.s3 expects these env vars:
    if (key == "AWS_ACCESS_KEY_ID")     Sys.setenv(AWS_ACCESS_KEY_ID = val)
    if (key == "AWS_SECRET_KEY")        Sys.setenv(AWS_SECRET_ACCESS_KEY = val)
    if (key == "AWS_SECRET_ACCESS_KEY") Sys.setenv(AWS_SECRET_ACCESS_KEY = val)
    if (key == "AWS_DEFAULT_REGION")    Sys.setenv(AWS_DEFAULT_REGION = val)
  }
}
cat("AWS region:", Sys.getenv("AWS_DEFAULT_REGION"), "\n")

BUCKET <- "yann-fr-data"

# ── Helper: write parquet to temp file, upload to S3 ─────────────────
upload_parquet <- function(df, s3_key, bucket = BUCKET) {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp))
  write_parquet(df, tmp)
  put_object(file = tmp, object = s3_key, bucket = bucket)
  file_size_mb <- file.size(tmp) / 1e6
  list(s3_key = s3_key, rows = nrow(df), cols = ncol(df),
       size_mb = round(file_size_mb, 2))
}

# ── Create bucket if it doesn't exist ────────────────────────────────
existing <- bucket_list_df()
if (!BUCKET %in% existing$Bucket) {
  cat("Creating bucket:", BUCKET, "\n")
  put_bucket(BUCKET, region = Sys.getenv("AWS_DEFAULT_REGION"))
} else {
  cat("Bucket", BUCKET, "already exists\n")
}

manifest <- list()

# ══════════════════════════════════════════════════════════════════════
# 1. WEDDINGS (1998-2023)
# ══════════════════════════════════════════════════════════════════════
cat("\n=== WEDDINGS ===\n")

wedding_files <- tribble(
  ~year, ~path,
  1998L, "weddings/1998/FD_MAR_1998.csv",
  1999L, "weddings/1999/FD_MAR_1999.csv",
  2000L, "weddings/2000/FD_MAR_2000.csv",
  2001L, "weddings/2001/FD_MAR_2001.csv",
  2002L, "weddings/2002/FD_MAR_2002.csv",
  2003L, "weddings/2003/FD_MAR_2003.csv",
  2004L, "weddings/2004/FD_MAR_2004.csv",
  2005L, "weddings/2005/FD_MAR_2005.csv",
  2006L, "weddings/2006/FD_MAR_2006.csv",
  2007L, "weddings/2007/FD_MAR_2007.csv",
  2008L, "weddings/2008/FD_MAR_2008.csv",
  2009L, "weddings/2009/FD_MAR_2009.csv",
  2010L, "weddings/2010/FD_MAR_2010.csv",
  2011L, "weddings/2011/FD_MAR_2011.csv",
  2012L, "weddings/2012/FD_MAR_2012.csv",
  2013L, "weddings/2013/FD_MAR_2013.csv",
  2014L, "weddings/2014/FD_MAR_2014.csv",
  2015L, "weddings/2015/FD_MAR_2015.csv",
  2016L, "weddings/2016/FD_MAR_2016.csv",
  2017L, "weddings/2017/FD_MAR_2017.csv",
  2018L, "weddings/2018/FD_MAR_2018.csv",
  2019L, "weddings/2019/FD_MAR_2019.csv",
  2020L, "weddings/2020/FD_MAR_2020.csv",
  2021L, "weddings2021/FD_MAR_2021.dbf",
  2022L, "weddings/2022/FD_MAR_2022.csv",
  2023L, "weddings/2023/FD_MAR_2023.csv"
)

# Column rename map for pre-2013
old_to_new <- c(
  ANAISH = "ANAIS1",   ANAISF = "ANAIS2",
  DEPNAISH = "DEPNAIS1", DEPNAISF = "DEPNAIS2",
  ETAMATH = "ETAMAT1",  ETAMATF = "ETAMAT2",
  INDNATH = "INDNAT1",  INDNATF = "INDNAT2"
)

target_cols <- c(
  "AMAR", "MMAR", "JSEMAINE", "DEPMAR", "DEPDOM",
  "ANAIS1", "ANAIS2", "DEPNAIS1", "DEPNAIS2",
  "SEXE1", "SEXE2", "INDNAT1", "INDNAT2",
  "ETAMAT1", "ETAMAT2", "TUCOM", "TUDOM",
  "NBENFCOM", "NBENFLEG"
)

for (i in seq_len(nrow(wedding_files))) {
  yr <- wedding_files$year[i]
  fp <- wedding_files$path[i]
  cat(sprintf("  [%d] %d ... ", i, yr))

  ext <- tools::file_ext(fp)
  if (ext == "dbf") {
    df <- foreign::read.dbf(fp, as.is = TRUE) |> tibble()
  } else {
    df <- read_delim(fp, delim = ";", col_types = cols(.default = "c"),
                     show_col_types = FALSE)
  }
  names(df) <- toupper(names(df))

  # Harmonize pre-2013 column names
  if (yr < 2013L) {
    present <- intersect(names(old_to_new), names(df))
    if (length(present) > 0) {
      names(df)[match(present, names(df))] <- old_to_new[present]
    }
    df <- df |> mutate(SEXE1 = "M", SEXE2 = "F")
  }

  # Ensure all target columns exist
  for (col in target_cols) {
    if (!col %in% names(df)) df[[col]] <- NA_character_
  }
  df <- df |>
    select(all_of(target_cols)) |>
    mutate(across(everything(), as.character))

  s3_key <- sprintf("weddings/year=%d/part-0.parquet", yr)
  info <- upload_parquet(df, s3_key)
  manifest <- c(manifest, list(tibble(
    subject = "weddings", year = yr,
    s3_key = info$s3_key, rows = info$rows,
    cols = info$cols, size_mb = info$size_mb
  )))
  cat(sprintf("%s rows, %.1f MB\n", format(info$rows, big.mark = ","), info$size_mb))
}

# ══════════════════════════════════════════════════════════════════════
# 2. DEATHS 2021
# ══════════════════════════════════════════════════════════════════════
cat("\n=== DEATHS ===\n")
cat("  [1] 2021 ... ")

deaths <- read_delim("deaths2021/FD_DEC_2021.csv", delim = ";",
                     col_types = cols(.default = "c"), show_col_types = FALSE)
names(deaths) <- toupper(names(deaths))

s3_key <- "deaths/year=2021/part-0.parquet"
info <- upload_parquet(deaths, s3_key)
manifest <- c(manifest, list(tibble(
  subject = "deaths", year = 2021L,
  s3_key = info$s3_key, rows = info$rows,
  cols = info$cols, size_mb = info$size_mb
)))
cat(sprintf("%s rows, %.1f MB\n", format(info$rows, big.mark = ","), info$size_mb))

# ══════════════════════════════════════════════════════════════════════
# 3. BIRTHS 2021
# ══════════════════════════════════════════════════════════════════════
cat("\n=== BIRTHS ===\n")
cat("  [1] 2021 ... ")

# births are already parquet — read and re-upload with Hive path
births <- read_parquet("births2021/FD_NAIS_2021.parquet")
names(births) <- toupper(names(births))

s3_key <- "births/year=2021/part-0.parquet"
info <- upload_parquet(births, s3_key)
manifest <- c(manifest, list(tibble(
  subject = "births", year = 2021L,
  s3_key = info$s3_key, rows = info$rows,
  cols = info$cols, size_mb = info$size_mb
)))
cat(sprintf("%s rows, %.1f MB\n", format(info$rows, big.mark = ","), info$size_mb))

# ══════════════════════════════════════════════════════════════════════
# 4. MANIFEST
# ══════════════════════════════════════════════════════════════════════
cat("\n=== MANIFEST ===\n")

manifest_df <- list_rbind(manifest) |> arrange(subject, year)
print(manifest_df, n = 50)

# Save locally
write_csv(manifest_df, "manifest.csv")

# Upload to S3
tmp <- tempfile(fileext = ".csv")
write_csv(manifest_df, tmp)
put_object(file = tmp, object = "manifest.csv", bucket = BUCKET)
unlink(tmp)

cat("\nManifest uploaded to s3://", BUCKET, "/manifest.csv\n", sep = "")
cat("\nTotal S3 size:", sum(manifest_df$size_mb), "MB\n")
cat("Total rows:", format(sum(manifest_df$rows), big.mark = ","), "\n")
cat("\nDone!\n")
