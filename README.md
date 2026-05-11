# fr-data
## NEVER ADD CLAUDE or any other agent as a co-author to Git commits!

Claude code session id: 2f59079c-07a2-4479-a336-b69c2bb87afd

Project for exploring and analyzing French public open data from [data.gouv.fr](https://www.data.gouv.fr/).

## MCP Server

This project has the **official data.gouv.fr MCP server** configured (`.mcp.json`). It connects to the public hosted instance at `https://mcp.data.gouv.fr/mcp` -- no API key needed for read operations.

### Available MCP Tools

| Tool | Purpose |
|------|---------|
| `search_datasets` | Search datasets by keywords |
| `get_dataset_info` | Get detailed dataset metadata |
| `list_dataset_resources` | List files/resources in a dataset |
| `get_resource_info` | Get resource details (format, size, URL) |
| `query_resource_data` | Query resource contents via the Tabular API |
| `search_dataservices` | Find registered APIs on the platform |
| `get_dataservice_info` | Get API metadata and OpenAPI spec URL |
| `get_dataservice_openapi_spec` | Fetch summarized OpenAPI spec for a service |
| `get_metrics` | Monthly visits/download stats for datasets |

### Direct API Access

The data.gouv.fr REST API is also available directly:

- **Base URL:** `https://www.data.gouv.fr/api/1/`
- **Swagger spec:** `https://www.data.gouv.fr/api/1/swagger.json`
- **Auth:** Read operations need no key. Write operations require `X-API-KEY` header.
- **Pagination:** Responses use `page` and `page_size` params (default 20). Response includes `next_page`/`previous_page` URLs.
- **Key endpoints:** `/datasets/`, `/organizations/`, `/reuses/`, `/dataservices/`, `/spatial/`

**Note:** Dataset IDs in the URL on data.gouv.fr look like `06f17e74-61d9-4ba8-85a6-736547a8bf58` — grab them from the browser URL, not from INSEE page numbers.

```bash
# Get dataset metadata (returns resources[] with download URLs)
curl.exe -X GET "https://www.data.gouv.fr/api/1/datasets/06f17e74-61d9-4ba8-85a6-736547a8bf58/" -H "Accept: application/json"
```

### Tabular API

For datasets indexed by the tabular engine, use `tabular-api.data.gouv.fr` (different subdomain):

- **Base URL:** `https://tabular-api.data.gouv.fr/`
- **Resource data endpoint:** `/api/resources/{resource_id}/data/`
- **Formats:** `/data/` (JSON), `/data/csv/`, `/data/json/`
- **Swagger UI:** `https://tabular-api.data.gouv.fr/api/docs`

```bash
# Paginated JSON
curl.exe -X GET "https://tabular-api.data.gouv.fr/api/resources/d0574a19-9005-4fff-92db-050b5fb2c72c/data/?page=1&page_size=20" -H "Accept: application/json"

# Full CSV download
curl.exe -X GET "https://tabular-api.data.gouv.fr/api/resources/d0574a19-9005-4fff-92db-050b5fb2c72c/data/csv/" -o output.csv
```

Use the MCP tools as the primary interface. Fall back to direct API calls (via `httr2` or `curl`) when you need endpoints the MCP doesn't cover.

#### INSEE API KEY
Although not necessary, I created an API key in https://portail-api.insee.fr/. the key is stored in the file `.env`.

### MCP Workflow: Finding & Downloading INSEE Data

The MCP is great for **discovery** but most INSEE resources on data.gouv.fr link to INSEE web pages rather than direct file downloads. The typical workflow is:

1. **`search_datasets`** — find datasets by keyword (use short, specific French terms; the API uses AND logic)
2. **`list_dataset_resources`** — get resource URLs. Most INSEE resources point to `insee.fr/fr/statistiques/...` pages, not direct downloads
3. **For INSEE page URLs** — use `WebFetch` on the INSEE page to extract the actual CSV/dBase download links. INSEE download URLs follow the pattern:
   - `https://www.insee.fr/fr/statistiques/fichier/{page_id}/{filename}.zip`
4. **Download with `curl -skL`** — the `-k` flag is needed on Windows to bypass SSL certificate issues with `api.insee.fr` and `insee.fr`

Some newer datasets (e.g., "Nombre de décès annuels par commune") have direct API download URLs like `https://api.insee.fr/melodi/file/...` — these also work with `curl -skL`.

**Important:** Dataset IDs on data.gouv.fr are MongoDB ObjectIds (e.g., `53699d0ea3a729239d205b2e`), not the numeric IDs used on insee.fr (e.g., `7616856`). You cannot use INSEE page numbers as data.gouv.fr dataset IDs.

## Data

### État civil 2021 (Naissances, décès, mariages)

Source: INSEE, collection page `https://www.insee.fr/fr/statistiques/6652160`

| Folder | Contents | INSEE page | Records |
|--------|----------|------------|---------|
| `births2021/` | Births (naissances) | — | — |
| `deaths2021/` | Deaths (décès) — `FD_DEC_2021.csv` + `varmod_DEC_2021.csv` | `7616856` | 661,585 |
| `weddings2021/` | Weddings (mariages) 2021 only | `7453878` | 218,819 |

**Deaths columns:** `ADEC` (year), `MDEC` (month), `JDEC` (day), `DEPDEC` (dept of death), `SEXE`, `ANAIS` (birth year), `MNAIS` (birth month), `JNAIS` (birth day), `DEPNAIS` (birth dept), `PNAIS` (birth country). Semicolon-delimited.

### Weddings 1998–2023 (Mariages, état civil)

Source: INSEE état civil, 26 years of individual-level marriage records.

**Local files** (originals + audit artifacts):

| Path | Contents |
|------|----------|
| `weddings/{YYYY}/` | Year subfolders with original dbf files + varmod/varlist dictionaries |
| `weddings/schema_audit.csv` | Column presence matrix across all 26 years |
| `weddings/row_counts.csv` | Row counts per year |
| `weddings/prepare_weddings.r` | Script: extract dbf→csv + schema audit |
| `weddings/harmonize_and_merge.r` | Script: rename H/F→1/2, add SEXE, merge all years |
| `upload_to_s3.r` | Script: convert to parquet + upload to S3 |
| `manifest.csv` | S3 object registry (subject, year, s3_key, rows, cols, size) |

**Harmonized columns (19):** `AMAR` (year), `MMAR` (month), `JSEMAINE` (day of week), `DEPMAR` (dept of marriage), `DEPDOM` (dept of domicile), `ANAIS1`/`ANAIS2` (birth years), `DEPNAIS1`/`DEPNAIS2` (birth depts), `SEXE1`/`SEXE2`, `INDNAT1`/`INDNAT2` (nationality), `ETAMAT1`/`ETAMAT2` (prior marital status), `TUCOM` (municipality size), `TUDOM` (urban unit size), `NBENFCOM` (children in common), `NBENFLEG` (legitimised children, 1998-2005 only).

**Key schema breaks:** Pre-2013 used gendered suffixes (H=homme, F=femme); 2013+ uses numbered spouse (1/2) after same-sex marriage legalization. `NBENFLEG` → `NBENFCOM` around 2006-2007. `ETAMAT1/2` dropped after 2021. 2023 adds `ENFCOM`, `INDLN1/2`, `REGDOM`, `REGMAR` (not in harmonized set).

### Births 1998–2023 (Naissances, état civil)

Source: INSEE état civil. 26 years × ~750k–800k records/year. Local: `births/{YYYY}/` (DBF + varmod). Schema follows same H/F→1/2 break in 2013. Columns include `AGEMERE`, `AMAR` (parents marriage year), `ANAIS`/`MNAIS`/`JNAIS`, `DEPNAIS`, `DEPDOM`, `SEXE`, `INDNAT`, `INDLN`, `TUCOM`, `TUDOM`, etc.

### Deaths 1998–2023 (Décès, état civil)

Source: INSEE état civil. 26 years × ~530k–660k records/year. Local: `deaths/{YYYY}/`. Same column set as deaths2021: `ADEC`/`MDEC`/`JDEC` (death date), `DEPDEC`, `SEXE`, `ANAIS`/`MNAIS`/`JNAIS` (birth date), `DEPNAIS`, `LIEUDEC2` or `PNAIS` (location). DBF zips have French-accented filenames in cp437 — use `unzip(list=TRUE)` then extract only the ASCII-named main `DEC{YYYY}.dbf`.

### Elections (Données des élections agrégées)

Source: data.gouv.fr dataset `6481e741d4cf002ec0efec9d`. Pre-aggregated nationwide elections at bureau-de-vote granularity. **Filtered to presidential only** (2002, 2007, 2012, 2017, 2022; both tours). Two tables: `general.parquet` (turnout, blanks, nulls) and `candidates.parquet` (per-candidate votes, nuance code). Plus `nuances.csv` dictionary mapping nuance codes (e.g. `LFI`, `RN`, `EXG`) to political family labels. The full source CSV (`general_results_good.csv`, 3.16M rows, all 56 elections) is kept locally; only presidential subsets are uploaded.

### Demographics

Two complementary commune-level annual aggregates from INSEE Mélodi API:
- `births_by_commune.parquet`: yearly birth counts per commune
- `deaths_by_commune.parquet`: yearly death counts per commune

Source CSVs are returned as ZIPs by Mélodi despite the `_CSV_FR` suffix — must `unzip()` first.

## AWS S3 Data Lake

### Bucket: `s3://yann-fr-data` (us-east-2)

**Credentials:** `.aws_env.txt` in project root (keys: `aws_access_key_id`, `aws_secret_key`, `aws_default_region`). Loaded via `upload_to_s3.r` helper. R package: `aws.s3`.

### Layout: Hive-style partitioned parquet

```
s3://yann-fr-data/
  {subject}/year={YYYY}/part-0.parquet
  manifest.csv
```

Current contents (91 objects, ~342 MB total):

| Subject | Years | Rows | Parquet size |
|---------|-------|------|-------------|
| `weddings/` | 1998–2023 (26 files) | 6,601,032 | ~34.8 MB |
| `deaths/` | 1998–2023 (26 files) | 14,945,730 | ~52.5 MB |
| `births/` | 1998–2023 (26 files) | 20,594,259 | ~194.6 MB |
| `elections/` general (presidential) | 2002, 2007, 2012, 2017, 2022 | 673,228 | ~18.9 MB |
| `elections/` candidates (presidential) | 2002, 2007, 2012, 2017, 2022 | 4,764,054 | ~34.7 MB |
| `elections/nuances.csv` | — | 51 | <0.01 MB |
| `demographics/` births_by_commune | annual 1975+ | 710,821 | ~3.1 MB |
| `demographics/` deaths_by_commune | annual 1975+ | 710,821 | ~3.2 MB |

### Design decisions

- **Bucket name:** `yann-fr-data` (not `fr-data` — that name was globally taken). General-purpose bucket for all INSEE datasets.
- **Parquet over CSV:** ~8x compression (44 MB vs 335 MB for weddings alone), columnar access, typed columns.
- **Hive partitioning by year:** Enables partition pruning in DuckDB/Athena — queries filtering on year only read the relevant files.
- **Subject as top-level prefix (not partition key):** Subjects have different schemas so they are separate logical tables, not row-bindable. The prefix acts as a table selector, not a filter dimension.
- **No primary key:** These are event records with no natural unique ID from INSEE. For deduplication, the full row is the key. A synthetic rowid adds no analytical value.
- **No indexes:** Parquet row-group min/max statistics + year partitioning = the S3 equivalent of a clustered index on year. DuckDB and Athena exploit this automatically.
- **`manifest.csv`:** Lightweight registry at bucket root. Query it to discover what's available by subject or year without listing S3 objects.

### Querying from R

```r
# With DuckDB (local, no AWS charges for compute):
library(DBI); library(duckdb)
con <- dbConnect(duckdb())
dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
dbExecute(con, "SET s3_region='us-east-2';")
dbExecute(con, "SET s3_access_key_id='...';")
dbExecute(con, "SET s3_secret_access_key='...';")

# All weddings — year column auto-extracted from Hive path
dbGetQuery(con, "
  SELECT * FROM read_parquet('s3://yann-fr-data/weddings/**/*.parquet', hive_partitioning=true)
  WHERE year BETWEEN 2018 AND 2023
")

# With aws.s3 (download then read locally):
aws.s3::save_object("weddings/year=2021/part-0.parquet", bucket = "yann-fr-data", file = "tmp.parquet")
nanoparquet::read_parquet("tmp.parquet")
```
