# Import des données de décès
# 'https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/'
# Remarque :
# Les données sont téléchargées manuellement vers le dossier donnees_deces

# remotes::install_github("a-lambda/outils", force = TRUE) (inutile, juste pour rappel)

library(readr)
library(dplyr)
library(purrr)
library(stringi)
library(remotes)
library(outils)
library(RPostgres)
library(gt)

dossier_data <- "data"
dossier_raw  <- "data/raw"
dossier_temp <- "data/temp"

#-------------------------------------- Fonction de détection des caractères nuls \00

detect_nulls_inside_file <- function(path_file) {
  
  raw_text <- read_file_raw(path_file)
  return(length(which(raw_text == as.raw(0))) > 0)

}

#-------------------------------------- Fonction de suppression des caractères nuls \00

del_nulls_in_file <- function(path_file) {
  
  raw_text <- read_file_raw(path_file)
  raw_text_without_nulls <- raw_text[which(raw_text != as.raw(0))]
  readr::write_file(x = raw_text_without_nulls, file = path_file )
  
}

#-------------------------------------- Fonction de remplacement des caractères nuls \00 par un caractère autre
# fichier deces-1995.txt
# COMETTO*SILVESTRO/                                                              11909092399127SAN ANTONIO DI SUSA\00\00\00\00\00\00\00\00\00\00\00ITALIE                        19950102384171        N             00000000

replace_nulls_in_text <- function(path_file, code) {
  
  raw_text <- read_file_raw(path_file)
  raw_text[which(raw_text == as.raw(0))] <- as.raw(code)
  readr::write_file(x = raw_text, file = path_file )

}

#-------------------------------------- Transfert des données brutes vers temp

list.files(dossier_raw,
           pattern = "[0-9]{4}[.]txt$",
           full.names = TRUE) |> 
  file.copy(to = dossier_temp)

#-------------------------------------- Remplacement des valeurs nulles \x00 par un espace 

list.files(dossier_temp, full.names = TRUE) |> 
  map(replace_nulls_in_text, code = 32) 

#-------------------------------------- Test tout est OK concernant les valeurs nulles

list.files(dossier_temp, full.names = TRUE) |> 
  map(detect_nulls_inside_file) |>
  unlist() |> 
  sum()

#-------------------------------------- Décodage des données
# ligne exemple :
# PAILLON*STEPHANE JEAN CLAUDE/                                                   11969101601034BELLEY                                                      19700331380857                              
#
codage_donnees <- c(
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

#-------------------------------------- Décodage et regroupement en un seul fichier (28756094 lignes à fin 2025)

db_raw_deces <- list.files(dossier_temp,
                           pattern = "[0-9]{4}[.]txt$",
                           full.names = TRUE) |> 
  map(
    read_fwf, 
    col_positions = fwf_widths(codage_donnees,  col_names = names(codage_donnees)),
    col_types = cols(.default = col_character())
  ) |> 
  bind_rows() |> 
  unique()

#-------------------------------------- Suppression des colonnes naissance_commune, naissance_pays, deces_numero_acte

db_raw_deces <- db_raw_deces |> 
  dplyr::select("nom_prenoms", "sexe", "naissance_date_string", "naissance_code_insee", "deces_date_string", "deces_code_insee")

#-------------------------------------- On ne garde que les lignes où tous les champs sont renseignés (28745172 lignes fin 2025)

db_raw_deces <- db_raw_deces[complete.cases(db_raw_deces),]

#------------------------------------- DETECTION DES SAISIES ILLEGALES A FIN 2025

has_string_illegal_chars <- function(string, authorized_chars) {

  pattern = paste0("[^", authorized_chars, "]")
  return(stri_count_regex(string, pattern = pattern) > 0)

}

has_name_illegal_chars <- function(string) {

  return(has_string_illegal_chars(string, authorized_chars = "-A-Z /*'"))

}

has_sexe_illegal_chars <- function(string) {

  return(has_string_illegal_chars(string, authorized_chars = "12"))

}

has_numeric_field_illegal_chars <- function(string) {

  return(has_string_illegal_chars(string, authorized_chars = "0-9"))

}

has_code_insee_illegal_chars <- function(string) {

  return(has_string_illegal_chars(string, authorized_chars = "0-9AB"))

}

# 1 - champ nom_prenoms (1 enregistrement KO : "CS\xa8ND\xa8R*LASZLO/")
db_raw_deces |> 
  dplyr::filter(has_name_illegal_chars(nom_prenoms)) |> 
  write.csv2(file = "data/rejets_nom_prenoms.csv", row.names = FALSE)
#
# 2 - champ sexe (OK)
db_raw_deces |> 
  dplyr::filter(has_sexe_illegal_chars(sexe))
#
# 3 - champ naissance_date_string (OK)
db_raw_deces |> 
  dplyr::filter(has_numeric_field_illegal_chars(naissance_date_string))
#
# 4 - champ naissance_code_insee (OK)
db_raw_deces |> 
  dplyr::filter(has_code_insee_illegal_chars(naissance_code_insee))
#
# 5 - champ naissance_commune (KO)
# Ce champ comporte beaucoup trop d'incohérences et ne doit pas être pris en compte !!!
#
# 6 - champ naissance_pays (KO)
# Ce champ comporte beaucoup trop d'incohérences et ne doit pas être pris en compte !!!
#
# 7 - champ deces_date_string (OK)
db_raw_deces |> 
  dplyr::filter(has_numeric_field_illegal_chars(deces_date_string))
#
# 8 - champ deces_code_insee (1 enregistrement KO : "-   -")
db_raw_deces |> 
  dplyr::filter(has_code_insee_illegal_chars(deces_code_insee))|> 
  write.csv2(file = "data/rejets_deces_code_insee.csv", row.names = FALSE)
#
# 9 - champ deces_numero_acte (OK)
# Ce champ comporte beaucoup trop d'incohérences et ne doit pas être pris en compte !!!
#

#-------------------------------------- Filtrage effectif sur la conformité des caractères dans les colonnes (reste 28745170 lignes)

db_raw_deces <- db_raw_deces |> 
  dplyr::filter(!has_name_illegal_chars(nom_prenoms)) |> 
  dplyr::filter(!has_sexe_illegal_chars(sexe)) |> 
  dplyr::filter(!has_numeric_field_illegal_chars(naissance_date_string)) |> 
  dplyr::filter(!has_code_insee_illegal_chars(naissance_code_insee)) |> 
  dplyr::filter(!has_numeric_field_illegal_chars(deces_date_string)) |> 
  dplyr::filter(!has_code_insee_illegal_chars(deces_code_insee))

#-------------------------------------- Recherche des lignes où le nom ne peut être identifié (absence de l'étoile)

db_raw_deces  |>
  filter(stri_count_regex(nom_prenoms, pattern = "[*]") != 1) |> 
  write.csv2(file = "data/rejets_nom_introuvable.csv", row.names = FALSE)

# A tibble: 1 × 9
# nom_prenoms                                  sexe  naissance_date_string naissance_code_insee naissance_commune naissance_pays deces_date_string deces_code_insee deces_numero_acte
# <chr>                                        <chr> <chr>                 <chr>                <chr>             <chr>          <chr>             <chr>            <chr>            
# MARIE ANTOINE JOSEPH DIT MARIE ANTOINE JOSEPH ALIAS SOOSAIRAJ DIT AUSSI MARIE J/ 1     19270330              99223                PERUMPANNAIYUR    INDE           20120521          99223            624              
#
#-------------------------------------- Suppression des lignes où le nom ne peut pas être identifié (reste 28745169 lignes)

db_raw_deces <- db_raw_deces  |>
  filter(stri_count_regex(nom_prenoms, pattern = "[*]") == 1) 

#-------------------------------------- Suppression des lignes où les prénoms ne peuvent pas être identifiés (reste 28745102 lignes)

db_raw_deces <- db_raw_deces  |>
  filter(stri_count_regex(nom_prenoms, pattern = "[/]") == 1)

#-------------------------------------- Sauvegarde db_raw_deces

saveRDS(object = db_raw_deces,
        file   = file.path(dossier_data, "db_raw_deces.RDS"))

#-------------------------------------- USER FUNCTIONS
# Format nom_prenoms : 
# "<nom>*<prenom1> <prenom2> <prenom3>...<last_prenom>/" 
# 
db_get_nom_prenoms <- function(string) {
  
  nom_prenoms = substr(string, 1, 80)
  pattern_nom <- "([^*]*)[*]"
  nom <- stri_match_all_regex(
    str = nom_prenoms,
    pattern = pattern_nom
  )[[1]][,2]
  pattern_prenoms <- "\\*([^*/]*)\\/"
  prenoms <- stri_match_all_regex(
    str = nom_prenoms,
    pattern = pattern_prenoms
  )[[1]][,2]
  return(list(nom, prenoms))
  
}

db_get_nom <- function(string) {
  
  nom_prenoms = substr(string, 1, 80)
  pattern_nom <- "([^*]*)[*]"
  nom <- stri_match_all_regex(
    str = nom_prenoms,
    pattern = pattern_nom
  )[[1]][,2]
  
}

db_get_prenoms <- function(string) {
  
  nom_prenoms = substr(string, 1, 80)
  pattern_prenoms <- "\\*([^*/]*)\\/"
  prenoms <- stri_match_all_regex(
    str = nom_prenoms,
    pattern = pattern_prenoms
  )[[1]][,2]
  
}

db_get_date <- function(string, format = "%Y%m%d") {
  
  # return a date or NA if date is not VALID
  date_event <- as.Date(string, format = format, optional = TRUE)
  
}

#-------------------------------------- Extraction nom et prénoms et dates

db_deces <- db_raw_deces |>   
  rowwise() |> # mutate appliqué par ligne
  mutate(
    nom               = db_get_nom(nom_prenoms),
    prenoms           = db_get_prenoms(nom_prenoms),
    naissance_date    = db_get_date(naissance_date_string),
    deces_date        = db_get_date(deces_date_string)
  ) |> 
  relocate(nom, prenoms, sexe, 
           naissance_date, naissance_code_insee,
           deces_date, deces_code_insee)

#-------------------------------------

saveRDS(object = db_deces,
        file   = file.path(dossier_data, "db_deces.RDS"))



#------------------------------------- Conversion en UTF-8  
# Raisons :

db_deces_unique_utf8 <- db_raw_deces |> 
  mutate( 
    nom_prenoms       = iconv(nom_prenoms,
                              from = "ISO-8859-1",
                              to = "UTF-8"),
    naissance_commune = iconv(naissance_commune,
                              from = "ISO-8859-1",
                              to = "UTF-8"),
    naissance_pays    = iconv(naissance_pays,
                              from = "ISO-8859-1",
                              to = "UTF-8"),
    deces_numero_acte = iconv(deces_numero_acte,
                              from = "ISO-8859-1",
                              to = "UTF-8")
  )




rm(db_raw_deces) # gain de mémoire



rm(db_deces_unique_utf8) # gain de mémoire



db_deces_4_sql <- db_deces |> 
  select(-c(nom_prenoms, naissance_date_string, deces_date_string))

saveRDS(object = db_deces_4_sql,
        file   = file.path(dossier_data, "db_deces_4_sql.RDS"))

unlink(dossier_temp, recursive = TRUE)
