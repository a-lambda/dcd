# Import des données de décès
# 'https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/'
# Remarque :
# Les données sont téléchargées manuellement vers le dossier donnees_deces

#-------------------------------------- Décodage des données
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

#-------------------------------------- USER FUNCTIONS
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

db_get_lieu <- function(string) {
  
  pattern_lieu = "([A-Z0-9 \\-\\']*)"
  lieu <- stri_match_all_regex(
    str = string,
    pattern = pattern_lieu
  )[[1]][,2] |> 
  paste0(collapse = "")
  
  return(lieu)
  
}

del_nulls_in_text <- function(path_file) {
  
  raw_text <- read_file_raw(path_file)
  raw_text_without_nulls <- raw_text[which(raw_text != as.raw(0))]
  readr::write_file(x = raw_text_without_nulls, file = path_file )
  
  return()

}

# à cause de "SAN ANTONIO DI SUSANULNULNULNULNULNULNULNULNULNULNULITALIE"
replace_nulls_int_text <- function(path_file, code) {
  
  raw_text <- read_file_raw(path_file)
  raw_text[which(raw_text == as.raw(0))] <- as.raw(code)
  readr::write_file(x = raw_text, file = path_file )

  return()

}

#-------------------------------------- Transfert données brutes vers temp

list.files(dossier_raw_deces,
           pattern = "[0-9]{4}[.]txt$",
           #pattern = "1995[.]txt$",
           full.names = TRUE) |> 
  file.copy(to = dossier_temp)

#-------------------------------------- Suppression des valeurs nulles \x00

list.files(dossier_temp, full.names = TRUE) |> 
  map(replace_nulls_int_text, code = 32) 

#-------------------------------------- Décodage

db_raw_deces <- list.files(dossier_temp,
                           pattern = "[.]txt$",
                           full.names = TRUE) |> 
  map(read_fwf, 
      col_positions = fwf_widths(taille_donnees, 
                                 col_names = names(taille_donnees)),
      col_types = cols(
        .default = col_character())
  ) 

#------------------------------------- Conversion en UTF-8  
# Raisons :
# - nom_prenoms       contient "CS\xa8ND\xa8R*LASZLO/"
# - naissance_commune contient "0x88ZEFINA"
# - naissance_pays    contient "\xa8PORTUGAL"
# - deces_numero_acte contient "0xf814/322"
# 
db_deces_unique_utf8 <- db_raw_deces |> 
  bind_rows() |> 
  unique() |> 
  # on veut pouvoir extraire au moins 1 nom
  filter(stri_count_regex(nom_prenoms, pattern = "[*]") == 1) |> 
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

db_deces <- db_deces_unique_utf8 |>   
  rowwise() |> # mutate appliqué par ligne
  mutate(
    nom               = db_get_nom(nom_prenoms),
    prenoms           = db_get_prenoms(nom_prenoms),
    naissance_date    = db_get_date(naissance_date_string),
    # naissance_commune = db_get_lieu(naissance_commune),
    # naissance_pays    = db_get_lieu(naissance_pays),
    deces_date        = db_get_date(deces_date_string)
  ) |> 
  relocate(
    nom, prenoms, sexe, 
    naissance_date, naissance_code_insee, naissance_commune, naissance_pays,
    deces_date, deces_code_insee, deces_numero_acte)

rm(db_deces_unique_utf8) # gain de mémoire

saveRDS(object = db_deces,
        file   = file.path(dossier_data, "db_deces.RDS"))

db_deces_4_sql <- db_deces |> 
  select(-c(nom_prenoms, naissance_date_string, deces_date_string))

saveRDS(object = db_deces_4_sql,
        file   = file.path(dossier_data, "db_deces_4_sql.RDS"))

unlink(dossier_temp, recursive = TRUE)
