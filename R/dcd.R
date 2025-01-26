# Import des données de décès
# 'https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/'
# Remarque :
# Les données sont téléchargées manuellement vers le dossier donnees_deces

library(readr)
library(dplyr)
library(purrr)
library(stringi)

dossiers <- c(
  donnees_raw   = 'inst/rawdata',
  donnees_deces = file.path(donnees_raw, 'deces'),
  donnees       = 'data'
)

# Création des dossiers
sapply(dossiers, \(x) { if (!dir.exists(x)) dir.create(x, recursive = TRUE) })

# Décodage des données
taille_donnees <- c(
  nom                  = 80,
  sexe                 =  1,
  naissance_date       =  8,
  naissance_code_insee =  5,
  naissance_commune    = 30,
  naissance_pays       = 30,
  deces_date           =  8,
  deces_code_insee     =  5,
  deces_numero_acte    =  9
)

chemins_fichiers_deces <- list.files(donnees_deces,
                                     pattern = "[0-9]{4}[.]txt$",
                                     full.names = TRUE)

dbs_raw_deces <- chemins_fichiers_deces |> 
  map(read_fwf, 
      col_positions = fwf_widths(taille_donnees, 
                                 col_names = names(taille_donnees)),
      col_types = cols(
        .default = col_character())
  )

db <- bind_rows(dbs_raw_deces) |> 
  unique()

db_get_nom_prenoms <- function(string) {
  
  string <- gsub(
    pattern = "/", # symbole parasite
    replacement = "",
    x = string
  )
  nom_prenoms <- stri_split_fixed(
    string, 
    pattern = "*"
    )
  nom <- nom_prenoms[[1]][1]
  prenoms <- stri_split_fixed(
    nom_prenoms[[1]][2],
    pattern = " "
  ) |> 
    unlist()
  
  return(list(nom = nom, prenoms = prenoms))
  
} 

db_get_nom <- function(string) {
  
  return(db_get_nom_prenoms(string)$nom)

}

db_get_prenoms <- function(string) {
  
  return(db_get_nom_prenoms(string)[[2]])
  
}

db_get_nom(db$nom[1:10])

