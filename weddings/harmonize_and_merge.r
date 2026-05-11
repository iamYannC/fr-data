## harmonize_and_merge.r
## Harmonizes column names across all 26 years of wedding data (1998-2023)
## and produces a single merge-ready file.
##
## KEY SCHEMA CHANGES:
##   - Pre-2013: gendered suffixes (ANAISH/ANAISF, DEPNAISH/DEPNAISF, etc.)
##     where H = homme (husband) and F = femme (wife)
##   - 2013+: numbered suffixes (ANAIS1/ANAIS2, DEPNAIS1/DEPNAIS2, etc.)
##     where 1 = spouse 1, 2 = spouse 2
##   - 2013+ added SEXE1/SEXE2 (same-sex marriage legalised 2013)
##   - NBENFLEG (1998-2005) -> replaced by NBENFCOM (2007+)
##   - ETAMAT1/2 dropped after 2021
##   - TUCOM present 1998-2022, dropped in 2023
##   - 2023 adds ENFCOM, INDLN1, INDLN2, REGDOM, REGMAR
##   - 2006 is missing NBENFLEG (already dropped) but NBENFCOM not yet added
##
## HARMONIZATION STRATEGY:
##   Rename H/F -> 1/2 for pre-2013 data. For those years SEXE1="M", SEXE2="F"
##   is implicit (same-sex marriage was not legal). We add those columns.
## -------------------------------------------------------------------

library(tidyverse)

base_dir <- "weddings"

# ── Column rename map for pre-2013 files ─────────────────────────────
old_to_new <- c(
  ANAISH   = "ANAIS1",
  ANAISF   = "ANAIS2",
  DEPNAISH = "DEPNAIS1",
  DEPNAISF = "DEPNAIS2",
  ETAMATH  = "ETAMAT1",
  ETAMATF  = "ETAMAT2",
  INDNATH  = "INDNAT1",
  INDNATF  = "INDNAT2"
)

# ── Build file paths ─────────────────────────────────────────────────
years <- c(1998:2020, 2022:2023)
paths <- map_chr(years, \(yr) {
  if (yr == 2023) {
    file.path(base_dir, as.character(yr), sprintf("FD_MAR_%d.csv", yr))
  } else {
    file.path(base_dir, as.character(yr), sprintf("FD_MAR_%d.csv", yr))
  }
})
# 2021 lives in weddings2021/
years <- c(years[years <= 2020], 2021L, years[years >= 2022])
paths <- c(
  paths[seq_len(which(years == 2020))],
  file.path(base_dir, "../weddings2021/FD_MAR_2021.dbf"),
  paths[(which(years == 2020) + 1):length(paths)]
)
# Fix: rebuild cleanly
years <- 1998:2023
paths <- map_chr(years, \(yr) {
  if (yr == 2021) return("weddings2021/FD_MAR_2021.dbf")
  if (yr == 2023) return(file.path(base_dir, "2023/FD_MAR_2023.csv"))
  file.path(base_dir, as.character(yr), sprintf("FD_MAR_%d.csv", yr))
})

# ── Target columns (the union we want in the final file) ────────────
# Core columns present in all or most years, after harmonization
target_cols <- c(
  "AMAR",      # year of marriage
  "MMAR",      # month of marriage
  "JSEMAINE",  # day of week
  "DEPMAR",    # department of marriage
  "DEPDOM",    # department of domicile
  "ANAIS1",    # birth year, spouse 1
  "ANAIS2",    # birth year, spouse 2
  "DEPNAIS1",  # birth dept, spouse 1
  "DEPNAIS2",  # birth dept, spouse 2
  "SEXE1",     # sex of spouse 1
  "SEXE2",     # sex of spouse 2
  "INDNAT1",   # nationality, spouse 1
  "INDNAT2",   # nationality, spouse 2
  "ETAMAT1",   # prior marital status, spouse 1
  "ETAMAT2",   # prior marital status, spouse 2
  "TUCOM",     # municipality size
  "TUDOM",     # urban unit size
  "NBENFCOM",  # children in common before marriage
  "NBENFLEG"   # legitimised children (1998-2005 only)
)

# ── Read & harmonize ─────────────────────────────────────────────────
read_one <- function(path, year) {
  ext <- tools::file_ext(path)
  if (ext == "dbf") {
    df <- foreign::read.dbf(path, as.is = TRUE) |> tibble()
  } else {
    df <- read_delim(path, delim = ";", col_types = cols(.default = "c"),
                     show_col_types = FALSE)
  }
  names(df) <- toupper(names(df))

  # Rename H/F -> 1/2 for pre-2013 data
  if (year < 2013) {
    present <- intersect(names(old_to_new), names(df))
    if (length(present) > 0) {
      names(df)[match(present, names(df))] <- old_to_new[present]
    }
    # Pre-2013: all marriages were heterosexual (same-sex legal from 2013)
    df <- df |> mutate(SEXE1 = "M", SEXE2 = "F")
  }

  # Ensure all target columns exist (fill missing with NA)
  for (col in target_cols) {
    if (!col %in% names(df)) df[[col]] <- NA_character_
  }

  # Force all columns to character for safe binding
  df <- df |> mutate(across(everything(), as.character))

  # Select only target columns
  df |> select(all_of(target_cols))
}

cat("Reading and harmonizing 26 years of wedding data...\n")
all_data <- map2(paths, years, \(p, yr) {
  cat(sprintf("  %d ... ", yr))
  df <- read_one(p, yr)
  cat(sprintf("%s rows\n", format(nrow(df), big.mark = ",")))
  df
}) |>
  list_rbind()

cat(sprintf("\nCombined: %s rows x %d cols\n",
            format(nrow(all_data), big.mark = ","), ncol(all_data)))

# ── Save ─────────────────────────────────────────────────────────────
out_path <- file.path(base_dir, "weddings_1998_2023.csv")
write_delim(all_data, out_path, delim = ";", na = "")
cat("Saved:", out_path, "\n")

cat(sprintf("File size: %.1f MB\n", file.size(out_path) / 1e6))

# Quick sanity check
cat("\n── Rows per year ──\n")
all_data |>
  summarise(n = n(), .by = AMAR) |>
  arrange(AMAR) |>
  print(n = 30)
