library(readr)
library(dplyr)
library(purrr)
library(stringi)
library(outils) # à installer manuellement dans la github publish action
library(RPostgres)
library(gt)

dossiers <- c(
  dossier_raw  <- 'inst/rawdata',
  dossier_raw_deces  <- file.path(dossier_raw, 'deces'),
  dossier_temp <- 'inst/temp',
  dossier_data <- 'data'
)

# Création des dossiers
invisible(
  sapply(dossiers, \(x) { if (!dir.exists(x)) dir.create(x, recursive = TRUE) })
)

if (!"db_deces.RDS" %in% list.files(path = dossier_data)) {
  source("R/01_import_data_dcd.R")
}
  
con <- my_con()

request <- "SELECT naissance_commune, naissance_date, deces_date,
          AGE(deces_date, naissance_date) as AGE 
          FROM dcd.deces
          WHERE deces_date IS NOT NULL AND
                naissance_date IS NOT NULL AND
                naissance_date >= '1967-04-19'::date
          ORDER BY AGE DESC, naissance_date, naissance_commune
          LIMIT 50;"

result <- RPostgres::dbGetQuery(con, statement = request)

if (inherits(result, "data.frame") & (nrow(result) == 50)) {
  gt(result)
} else {
  source("R/02_sql_section.R")
}

