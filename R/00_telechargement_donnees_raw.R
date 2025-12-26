# Import des données de décès
# 'https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/'

library(readr)
library(dplyr)

# les données sont téléchargées dans le dossier data/raw

dossier_data_raw <- "data/raw"
dossier_data <- "data"

# Créer les données

if(!dir.exists(dossier_data_raw)) dir.create(dossier_data_raw)
if(!dir.exists(dossier_data)) dir.create(dossier_data)

# Liste des URLs des fichiers de patients décédés

urls_fichiers_raw <- c(

  "2020" = "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20210112-143457/deces-2020.txt",
  "2019" = "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20200113-173945/deces-2019.txt",
  "2018" = "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191205-191652/deces-2018.txt"

)

telecharge_fichier <- function(url_fichier_raw, dossier_cible = dossier_data_raw) {
  
  nom_fichier <- basename(url_fichier_raw)
  chemin_fichier <- file.path(dossier_cible, nom_fichier)

  if(!file.exists(chemin_fichier)) {
    message("Téléchargement via l'url ", url_fichier_raw)
    curl::curl_download(
      url = url_fichier_raw, 
      destfile = chemin_fichier, 
      quiet = FALSE
    )
    message("Téléchargement terminé. Taille : ", file.size(chemin_fichier), " octets")
  } else {message('Fichier déjà présent')}
  
  chemin_fichier
}

chemin_fichiers_telecharges <- lapply(urls_fichiers_raw, telecharge_fichier)
