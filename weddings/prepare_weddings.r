## prepare_weddings.r
## Reads all wedding data (1998-2023), converts dbf->csv with consistent
## naming, and audits schema differences across years.
## -------------------------------------------------------------------

library(tidyverse)
library(foreign)

base_dir <- "weddings"

# ── 1. Build file inventory ──────────────────────────────────────────
# Map each year to its main data file (inconsistent naming from INSEE)
data_files <- tribble(
  ~year, ~path,
  1998L, "1998/MAR1998.dbf",
  1999L, "1999/MAR1999.dbf",
  2000L, "2000/MAR2000.dbf",
  2001L, "2001/MAR2001.dbf",
  2002L, "2002/MAR2002.dbf",
  2003L, "2003/MAR2003.dbf",
  2004L, "2004/MAR2004.dbf",
  2005L, "2005/MAR2005.dbf",
  2006L, "2006/MAR2006.dbf",
  2007L, "2007/MAR2007.dbf",
  2008L, "2008/MAR2008.dbf",
  2009L, "2009/MAR2009.dbf",
  2010L, "2010/MAR2010.dbf",
  2011L, "2011/mar2011_dbase.dbf",
  2012L, "2012/mar2012.dbf",
  2013L, "2013/mar2013.dbf",
  2014L, "2014/mar2014.dbf",
  2015L, "2015/mar2015.dbf",
  2016L, "2016/mar2016.dbf",
  2017L, "2017/etatcivil2017_mar2017.dbf",
  2018L, "2018/FD_MAR_2018.dbf",
  2019L, "2019/FD_MAR_2019.dbf",
  2020L, "2020/FD_MAR_2020.dbf",
  2021L, "../weddings2021/FD_MAR_2021.dbf",
  2022L, "2022/FD_MAR_2022.dbf",
  2023L, "2023/FD_MAR_2023.csv"
) |>
  mutate(full_path = file.path(base_dir, path))

cat("Found", nrow(data_files), "years of wedding data\n\n")

# ── 2. Read each file, standardize columns, save as CSV ─────────────
read_wedding <- function(path, year) {
  ext <- tools::file_ext(path)
  if (ext == "dbf") {
    df <- read.dbf(path, as.is = TRUE) |> tibble()
  } else {
    df <- read_delim(path, delim = ";", col_types = cols(.default = "c"),
                     show_col_types = FALSE)
  }
  # Uppercase all column names for consistency
  names(df) <- toupper(names(df))
  df
}

schema_list <- list()
row_counts  <- integer()

for (i in seq_len(nrow(data_files))) {
  yr   <- data_files$year[i]
  fp   <- data_files$full_path[i]
  cat(sprintf("[%d/%d] Reading %d ... ", i, nrow(data_files), yr))

  df <- read_wedding(fp, yr)
  row_counts[as.character(yr)] <- nrow(df)
  schema_list[[as.character(yr)]] <- tibble(
    variable = names(df),
    class    = map_chr(df, \(x) class(x)[1])
  )
  cat(sprintf("%s rows x %d cols\n", format(nrow(df), big.mark = ","), ncol(df)))

  # Save standardised CSV (skip 2023 - already csv, skip 2021 - lives in weddings2021/)
  if (yr == 2023L || yr == 2021L) next

  out_dir  <- file.path(base_dir, as.character(yr))
  out_path <- file.path(out_dir, sprintf("FD_MAR_%d.csv", yr))
  write_delim(df, out_path, delim = ";")
  cat("   -> saved", out_path, "\n")
}

# ── 3. Schema audit ─────────────────────────────────────────────────
cat("\n\n========== SCHEMA AUDIT ==========\n\n")

# All unique column names across all years
all_vars <- schema_list |>
  imap(\(s, yr) s |> mutate(year = as.integer(yr))) |>
  list_rbind()

# Presence matrix: which variables exist in which years
presence <- all_vars |>
  distinct(year, variable) |>
  mutate(present = TRUE) |>
  pivot_wider(names_from = year, values_from = present, values_fill = FALSE) |>
  arrange(variable)

# Count how many years each variable appears in
presence <- presence |>
  mutate(
    n_years = rowSums(across(where(is.logical))),
    .after = variable
  )

cat("── Variables present in ALL", length(schema_list), "years ──\n")
universal <- presence |> filter(n_years == length(schema_list))
cat(paste(universal$variable, collapse = ", "), "\n\n")

cat("── Variables NOT in all years ──\n")
partial <- presence |> filter(n_years < length(schema_list))
if (nrow(partial) == 0) {
  cat("(none — all variables are universal)\n")
} else {
  for (j in seq_len(nrow(partial))) {
    v <- partial$variable[j]
    yrs <- names(partial)[-(1:2)][as.logical(partial[j, -(1:2)])]
    cat(sprintf("  %-12s  (%d/%d years): %s\n",
                v, partial$n_years[j], length(schema_list),
                paste(yrs, collapse = ", ")))
  }
}

cat("\n── Row counts per year ──\n")
rc <- tibble(year = as.integer(names(row_counts)), rows = row_counts) |>
  arrange(year)
for (k in seq_len(nrow(rc))) {
  cat(sprintf("  %d: %s\n", rc$year[k], format(rc$rows[k], big.mark = ",")))
}

# Save audit to CSV
write_csv(presence, file.path(base_dir, "schema_audit.csv"))
write_csv(rc, file.path(base_dir, "row_counts.csv"))
cat("\nSaved schema_audit.csv and row_counts.csv\n")
