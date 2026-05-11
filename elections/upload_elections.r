suppressPackageStartupMessages({
  library(aws.s3)
  library(arrow)
  library(dplyr)
})

# Load AWS creds
env_path <- "C:/Users/97253/Documents/fr-data/.aws_env.txt"
env_lines <- readLines(env_path)
for (line in env_lines) {
  if (grepl("=", line, fixed = TRUE)) {
    parts <- strsplit(line, "=", fixed = TRUE)[[1]]
    key <- trimws(parts[1])
    val <- trimws(paste(parts[-1], collapse = "="))
    val <- gsub("[\"']", "", val)
    if (key == "aws_access_key_id") Sys.setenv(AWS_ACCESS_KEY_ID = val)
    if (key == "aws_secret_key")    Sys.setenv(AWS_SECRET_ACCESS_KEY = val)
    if (key == "aws_default_region") Sys.setenv(AWS_DEFAULT_REGION = val)
  }
}

bucket <- "yann-fr-data"
base   <- "C:/Users/97253/Documents/fr-data/elections"

# Manifest rows
new_rows <- list()

for (y in c(2002, 2007, 2012, 2017, 2022)) {
  for (subj in c("general", "candidates")) {
    local_path <- file.path(base, paste0("year=", y), paste0(subj, ".parquet"))
    s3_key     <- paste0("elections/year=", y, "/", subj, ".parquet")
    df <- read_parquet(local_path)
    cat("uploading", s3_key, "rows=", nrow(df), "\n")
    put_object(file = local_path, object = s3_key, bucket = bucket)
    new_rows[[length(new_rows) + 1]] <- data.frame(
      subject = paste0("elections_", subj),
      year    = y,
      s3_key  = s3_key,
      rows    = nrow(df),
      cols    = ncol(df),
      size_mb = round(file.size(local_path) / 1024 / 1024, 2)
    )
  }
}

# Upload nuances dictionary
nuances_local <- file.path(base, "nuances.csv")
nuances_key   <- "elections/nuances.csv"
put_object(file = nuances_local, object = nuances_key, bucket = bucket)
cat("uploaded", nuances_key, "\n")

new_rows[[length(new_rows) + 1]] <- data.frame(
  subject = "elections_nuances",
  year    = NA_integer_,
  s3_key  = nuances_key,
  rows    = nrow(read.csv(nuances_local, sep = ";")),
  cols    = ncol(read.csv(nuances_local, sep = ";")),
  size_mb = round(file.size(nuances_local) / 1024 / 1024, 4)
)

# Update manifest
manifest_path <- "C:/Users/97253/Documents/fr-data/manifest.csv"
m <- read.csv(manifest_path)
new_df <- do.call(rbind, new_rows)
m2 <- rbind(m, new_df)
write.csv(m2, manifest_path, row.names = FALSE)
cat("manifest now has", nrow(m2), "rows\n")
