# Import des données de décès
# 'https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/'
# Remarque :
# Les données sont téléchargées manuellement vers le dossier donnees_deces

library(readr)
library(dplyr)
library(purrr)
library(stringi)

dossiers <- c(
  donnees_raw   <- 'inst/rawdata',
  donnees_deces <- file.path(donnees_raw, 'deces'),
  donnees       <- 'data'
)

# Création des dossiers
invisible(
  sapply(dossiers, \(x) { if (!dir.exists(x)) dir.create(x, recursive = TRUE) })
)


# Décodage des données
taille_donnees <- c(
  nom_prenoms           = 80,
  sexe                  =  1,
  naissance_date_string =  8,
  naissance_code_insee  =  5,
  naissance_commune     = 30,
  naissance_pays        = 30,
  deces_date_string     =  8,
  deces_code_insee      =  5,
  deces_numero_acte     =  9
)

paths_fichiers_deces <- list.files(donnees_deces,
                                     pattern = "[0-9]{4}[.]txt$",
                                     # pattern = "197[0-9][.]txt$",
                                     full.names = TRUE)

db_raw_deces <- paths_fichiers_deces |> 
  map(read_fwf, 
      col_positions = fwf_widths(taille_donnees, 
                                 col_names = names(taille_donnees)),
      col_types = cols(
        .default = col_character())
  ) 
  
#
#-------------------------------------------------------------------------------
# Format nom_prenoms : 
# "<nom>*<prenom1> <prenom2> <prenom3>...<last_prenom>/" 
# 
db_get_nom <- function(string) {
  
  pattern_nom = "(.*?)[*]"
  nom <- stri_match_all_regex(
    str = string,
    pattern = pattern_nom
    )[[1]][,2]
  
  return(nom)
  
}

db_get_prenoms <- function(string) {
  
  pattern_nom = "[*](.*)[/]$"
  prenoms <- stri_match_all_regex(
    str = string,
    pattern = pattern_nom
  )[[1]][,2]
  
  return(prenoms)
  
}

db_get_date <- function(string, format = "%Y%m%d") {
  
    # return a date or NA if date is not VALID
    date_event = as.Date(string, format = format, optional = TRUE)
  
  return(date_event)
  
}
#
#-------------------------------------------------------------------------------
#

db_deces <- db_raw_deces |> 
  bind_rows() |> 
  unique() |> 
  mutate( # conversion en UTF-8 à cause de "CS\xa8ND\xa8R*LASZLO/"
    nom_prenoms = iconv(nom_prenoms, from = "ISO-8859-1", to = "UTF-8")
  )

rm(db_raw_deces)

db_deces <- db_deces |>   
  rowwise() |> 
  mutate(
    nom = db_get_nom(nom_prenoms),
    prenoms = db_get_prenoms(nom_prenoms),
    naissance_date = db_get_date(naissance_date_string),
    deces_date = db_get_date(deces_date_string),
  ) 


