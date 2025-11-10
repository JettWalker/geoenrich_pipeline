################################################################################
# Geoenrich (Python) from R via reticulate — end-to-end example
# - Cleans your CSV (coords/dates)
# - Creates geoenrich dataset
# - Authenticates to Copernicus Marine (one-time)
# - Enriches a variable (e.g., chlorophyll)
# - Produces summary stats
#
# REQUIREMENTS:
#   • Have R ≥ 4.2 and RStudio installed
#   • Have Python ≥ 3.9 available
#   • Python package (in reticulate’s Python): geoenrich (pip). follow https://geoenrich.readthedocs.io/en/latest/r-install.html and look at "1. Prerequisites"
#   • Copernicus Marine account (free): https://data.marine.copernicus.eu/register
################################################################################

## ======================== EDIT ME (project-specific) ======================== ##
# 1) Paths
PROJECT_DIR <- "/Users/yourname/your_project_folder"  # <-- set to your folder
INPUT_CSV   <- "your_points.csv"                      # <-- name of your source CSV

# 2) Column names in your CSV (match exactly, incl. spaces/case)
COL_ID   <- "Sample_ID"
COL_DATE <- "Date"
COL_LAT  <- "Lat"
COL_LON  <- "Long"

# 3) Date format in your CSV (choose one)
# Examples: 19/4/2012 -> "%d/%m/%Y"   |   19_4_2012 -> "%d_%m_%Y"   |   2012-04-19 -> "%Y-%m-%d"
DATE_FMT <- "%d/%m/%Y"

# 4) Dataset ID (short name with NO ".csv"; used to create files under ./biodiv/)
DATASET_ID <- "my_dataset"   # e.g., "shark_bay_clean"

# 5) Variable to enrich (and source)
VAR_ID    <- "chlorophyll"   # e.g., "chlorophyll", "sst", etc.
VAR_SOURCE <- "copernicus"   # used informationally here

# 6) Buffers
GEO_BUFFER_KM  <- 2L         # spatial buffer (km)
TIME_BUFFER    <- c(-30L, 0L) # temporal buffer (days relative to event date)

# 7) Copernicus credentials (use the REAL password; do not URL-encode "@")
COP_USER <- ""               # e.g., "<your username>"
COP_PASS <- ""               # e.g., "<your password>"
## =========================================================================== ##


# 0) Packages ------------------------------------------------------------------
if (!requireNamespace("reticulate", quietly = TRUE)) install.packages("reticulate")
if (!requireNamespace("readr", quietly = TRUE))      install.packages("readr")
if (!requireNamespace("dplyr", quietly = TRUE))      install.packages("dplyr")
if (!requireNamespace("stringr", quietly = TRUE))    install.packages("stringr")

library(reticulate)
library(readr)
library(dplyr)
library(stringr)

# 1) Python deps (install if missing) -----------------------------------------
if (!"geoenrich" %in% reticulate::py_list_packages()$package) {
  message("Installing geoenrich into reticulate's Python …")
  reticulate::py_install("geoenrich", pip = TRUE)
}

# 2) Working dir & inputs ------------------------------------------------------
setwd(PROJECT_DIR)
stopifnot(file.exists(INPUT_CSV))

CLEAN_CSV <- sprintf("%s_clean.csv", tools::file_path_sans_ext(basename(INPUT_CSV)))

# 3) (OPTIONAL) Clean the CSV (coords & dates) -------------------------------------------
# - Converts "-" and other junk to NA and drops bad rows
# - Keeps Date as character; geoenrich parses using DATE_FMT
df <- readr::read_csv(INPUT_CSV, guess_max = 100000, na = c("", " ", "-", "NA", "N/A"))

# Check columns exist (fail early with a helpful message)
needed <- c(COL_ID, COL_DATE, COL_LAT, COL_LON)
missing <- setdiff(needed, names(df))
if (length(missing)) stop("Missing columns in CSV: ", paste(missing, collapse = ", "))

# Coerce coords to numeric and drop invalid rows
df <- df %>%
  mutate(
    !!COL_LAT := readr::parse_number(.data[[COL_LAT]] |> as.character()),
    !!COL_LON := readr::parse_number(.data[[COL_LON]] |> as.character()),
    !!COL_DATE:= .data[[COL_DATE]] |> as.character() |> str_trim()
  ) %>%
  filter(!is.na(.data[[COL_LAT]]), !is.na(.data[[COL_LON]]),
         .data[[COL_LAT]] >= -90, .data[[COL_LAT]] <= 90,
         .data[[COL_LON]] >= -180, .data[[COL_LON]] <= 180)

readr::write_csv(df, CLEAN_CSV)
message("Wrote cleaned file: ", CLEAN_CSV, " (rows: ", nrow(df), ")")

# 4) Python modules ------------------------------------------------------------
os <- import("os")
dl <- import("geoenrich.dataloader",  convert = FALSE)
en <- import("geoenrich.enrichment",  convert = FALSE)
ex <- import("geoenrich.exports",     convert = FALSE)

# 5) Ensure expected folders exist --------------------------------------------
dir.create("biodiv", recursive = TRUE, showWarnings = FALSE)
dir.create(file.path("biodiv", "datasets"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path("biodiv", "results"),  recursive = TRUE, showWarnings = FALSE)
dir.create("sat", recursive = TRUE, showWarnings = FALSE)   # where .nc files may be written

# 6) Load occurrences into geoenrich ------------------------------------------
geodf <- dl$import_occurrences_csv(
  path        = CLEAN_CSV,
  id_col      = COL_ID,
  date_col    = COL_DATE,
  lat_col     = COL_LAT,
  lon_col     = COL_LON,
  date_format = DATE_FMT
)
cat("Loaded rows:", reticulate::py_len(geodf), "\n")

# 7) Create the enrichment file (use clean DATASET_ID, no ".csv") -------------
if (reticulate::py_has_attr(en, "create_enrichment_file")) {
  en$create_enrichment_file(geodf, DATASET_ID)
} else if (reticulate::py_has_attr(ex, "create_enrichment_file")) {
  ex$create_enrichment_file(geodf, DATASET_ID)
} else {
  stop("create_enrichment_file() not found in geoenrich.")
}
message("Enrichment file prepared for dataset_id: ", DATASET_ID)

# 8) Copernicus login (non-interactive; creates cached credentials) -----------
if (nzchar(COP_USER) && nzchar(COP_PASS)) {
  reticulate::py_run_string(sprintf("
import os, copernicusmarine
os.environ['COPERNICUSMARINE_SERVICE_USERNAME'] = %s
os.environ['COPERNICUSMARINE_SERVICE_PASSWORD'] = %s
copernicusmarine.login(username=os.environ['COPERNICUSMARINE_SERVICE_USERNAME'],
                       password=os.environ['COPERNICUSMARINE_SERVICE_PASSWORD'])
print('Copernicus login OK')
", shQuote(COP_USER), shQuote(COP_PASS)))
} else {
  message("TIP: set COP_USER/COP_PASS at the top to skip interactive prompts.")
}

# (Optional) suppress noisy pool warnings or tune concurrency:
# reticulate::py_run_string("import warnings; warnings.filterwarnings('ignore', message='Connection pool is full')")
# reticulate::py_run_string("import requests.adapters; requests.adapters.DEFAULT_POOLSIZE = 20; print('Pool size set to 20')")

# 9) Enrich --------------------------------------------------------------------
time_tuple <- reticulate::tuple(as.integer(TIME_BUFFER[1]), as.integer(TIME_BUFFER[2]))
message(sprintf("Enriching '%s' from %s …", VAR_ID, VAR_SOURCE))
en$enrich(DATASET_ID, VAR_ID, as.integer(GEO_BUFFER_KM), time_tuple)


# 10) Summaries / exports ------------------------------------------------------
if (reticulate::py_has_attr(ex, "produce_stats")) {
  ex$produce_stats(DATASET_ID, VAR_ID, out_path = "./")
} else if (reticulate::py_has_attr(en, "produce_stats")) {
  en$produce_stats(DATASET_ID, VAR_ID, out_path = "./")
} else {
  message("Note: produce_stats() not found in this geoenrich version; skipping.")
}


# 11) Show outputs -------------------------------------------------------------
cat("\nFiles under ./biodiv (and possibly ./sat):\n")
print(list.files("biodiv", recursive = TRUE))