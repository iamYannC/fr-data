## download_births_deaths.r
## Downloads all births and deaths fichier détail ZIPs from INSEE (1998-2023)
## then extracts them into year subfolders.
## -------------------------------------------------------------------

library(tidyverse)

base_url <- "https://www.insee.fr/fr/statistiques/fichier"

# ── BIRTHS: page IDs and filenames ───────────────────────────────────
births_inventory <- tribble(
  ~year, ~page_id, ~filename,
  1998L, "2408051", "etatcivil1998_nais1998_dbase.zip",
  1999L, "2408042", "etatcivil1999_nais1999_dbase.zip",
  2000L, "2408033", "etatcivil2000_nais2000_dbase.zip",
  2001L, "2408022", "etatcivil2001_nais2001_dbase.zip",
  2002L, "2408013", "etatcivil2002_nais2002_dbase.zip",
  2003L, "2408004", "etatcivil2003_nais2003_dbase.zip",
  2004L, "2407995", "etatcivil2004_nais2004_dbase.zip",
  2005L, "2407973", "etatcivil2005_nais2005_dbase.zip",
  2006L, "2407964", "etatcivil2006_nais2006_dbase.zip",
  2007L, "2407955", "etatcivil2007_nais2007_dbase.zip",
  2008L, "2407946", "etatcivil2008_nais2008_dbase.zip",
  2009L, "2407937", "etatcivil2009_nais2009_dbase.zip",
  2010L, "2407928", "etatcivil2010_nais2010_dbase.zip",
  2011L, "2407919", "etatcivil2011_nais2011_dbase.zip",
  2012L, "2407910", "etatcivil2012_nais2012_dbase.zip",
  2013L, "2117101", "etatcivil2013_nais2013_dbase.zip",
  2014L, "2114964", "etatcivil2014_nais2014_dbase.zip",
  2015L, "2406436", "etatcivil2015_nais2015_dbase.zip",
  2016L, "3051485", "etatcivil2016_nais2016_dbase.zip",
  2017L, "3596190", "etatcivil2017_nais2017_dbase.zip",
  2018L, "4215180", "etatcivil2018_nais2018_dbase.zip",
  2019L, "4768335", "etatcivil2019_nais2019_dbase.zip",
  2020L, "5419785", "etatcivil2020_nais2020_dbase.zip",
  # 2021: already have locally as parquet — download varmod only
  2021L, "6652024", "FD_NAIS_2021.parquet",
  # 2022-2023: newer format (parquet + csv)
  2022L, "7708070", "FD_NAIS_2022.parquet",
  2023L, "8285987", "FD_NAIS_2023_csv.zip"
)

# ── DEATHS: page IDs and filenames ───────────────────────────────────
deaths_inventory <- tribble(
  ~year, ~page_id, ~filename,
  1998L, "2408054", "etatcivil1998_dec1998_dbase.zip",
  1999L, "2408045", "etatcivil1999_dec1999_dbase.zip",
  2000L, "2408036", "etatcivil2000_dec2000_dbase.zip",
  2001L, "2408025", "etatcivil2001_dec2001_dbase.zip",
  2002L, "2408016", "etatcivil2002_dec2002_dbase.zip",
  2003L, "2408007", "etatcivil2003_dec2003_dbase.zip",
  2004L, "2407998", "etatcivil2004_dec2004_dbase.zip",
  2005L, "2407976", "etatcivil2005_dec2005_dbase.zip",
  2006L, "2407967", "etatcivil2006_dec2006_dbase.zip",
  2007L, "2407958", "etatcivil2007_dec2007_dbase.zip",
  2008L, "2407949", "etatcivil2008_dec2008_dbase.zip",
  2009L, "2407940", "etatcivil2009_dec2009_dbase.zip",
  2010L, "2407931", "etatcivil2010_dec2010_dbase.zip",
  2011L, "2407922", "etatcivil2011_dec2011_dbase.zip",
  2012L, "2407913", "etatcivil2012_dec2012_dbase.zip",
  2013L, "2117115", "etatcivil2013_dec2013_dbase.zip",
  2014L, "2114975", "etatcivil2014_dec2014_dbase.zip",
  2015L, "2406453", "etatcivil2015_dec2015_dbase.zip",
  2016L, "3053349", "etatcivil2016_dec2016_dbase.zip",
  2017L, "3606190", "etatcivil2017_dec2017_dbase.zip",
  2018L, "4216603", "etatcivil2018_dec2018_dbase.zip",
  2019L, "4801913", "etatcivil2019_dec2019_dbase.zip",
  2020L, "5431034", "etatcivil2020_dec2020_dbase.zip",
  # 2021: already have locally as CSV
  2021L, "7616856", "etatcivil2021_dec2021_dbase.zip",
  2022L, "7707436", "etatcivil2022_dec2022_dbase.zip",
  2023L, "8279314", "dec2023_fdet.zip"
)

# ── Download helper ──────────────────────────────────────────────────
download_file <- function(page_id, filename, dest_dir) {
  dir.create(dest_dir, showWarnings = FALSE, recursive = TRUE)
  dest <- file.path(dest_dir, filename)
  if (file.exists(dest)) {
    cat("  [skip] already exists:", dest, "\n")
    return(TRUE)
  }
  url <- paste0(base_url, "/", page_id, "/", filename)
  # curl -skL for SSL issues on Windows
  cmd <- sprintf('curl -skL -o "%s" "%s"', dest, url)
  cat("  [download]", url, "\n")
  rc <- system(cmd)
  if (rc != 0 || !file.exists(dest) || file.size(dest) < 1000) {
    cat("  [FAILED] rc=", rc, "\n")
    if (file.exists(dest)) unlink(dest)
    return(FALSE)
  }
  cat("  [OK]", round(file.size(dest) / 1e6, 1), "MB\n")
  return(TRUE)
}
SKIP_PATTERN <- "\\.(dbf|csv|parquet)$"

download_year <- function(inventory, type, dest_root) {
  total <- nrow(inventory)
  
  map_lgl(seq_len(total), \(i) {
    row <- inventory[i, ]
    yr  <- row$year
    cat(sprintf("[%d/%d] %s %d\n", i, total, type, yr))
    
    if (length(list.files(file.path(dest_root, yr), pattern = SKIP_PATTERN)) > 0) {
      cat("  [skip] Files already unzipped\n")
      return(TRUE)
    }
    
    download_file(row$page_id, row$filename, file.path(dest_root, as.character(yr)))
  })
}

cat("=== DOWNLOADING BIRTHS ===\n\n")
births_results <- download_year(births_inventory, "Births", "births")
cat(paste0("\nBirths downloads: ", sum(births_results), "/", nrow(births_inventory), " succeeded\n"))

cat("=== DOWNLOADING DEATHS ===\n\n")
deaths_results <- download_year(deaths_inventory, "Deaths", "deaths")
cat(paste0("\nDeaths downloads: ", sum(deaths_results), "/", nrow(deaths_inventory), " succeeded\n"))

cat("\n=== DONE ===\n")