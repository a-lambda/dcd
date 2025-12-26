# Intégration des données raw dans une base de données

## Récupération de l'ensemble des fichiers raw

chemins_fichiers_raw <- list.files(path = "data/raw", pattern = "*.txt", full.names = TRUE)

## les fichiers raw comportent des données à taille fixe

positions <- c(
  nom = 80,
  sexe = 1,
  naissance_date = 8,
  naissance_code_lieu = 5,
  naissance_commune = 30,
  naissance_pays = 30,
  deces_date = 8,
  deces_code_lieu = 5,
  deces_numero_acte = 9
)

liste_tibble_raw <- lapply(chemins_fichiers_raw, read_fwf,
  col_positions = fwf_widths(positions, col_names = names(positions)),
  col_types = cols(
    .default = col_character())
)

db_raw <- bind_rows(liste_tibble_raw)
duplicates <- db_raw[which(duplicated(db_raw)),]
db_unique <- db_raw |> unique()





# Attention pour les dates : certaines sont approximatives. Lorsque c'est le cas
# la partie incertaine (mois ou jour) est à 00. -> remplacer les 00 par 01.
# Pour les années inconnues -> ne rien mettre ?

nettoyer_partie_date <- function(x, debut, fin) {
  
  rez <- x %>%
    substr(debut, fin) %>% 
    as.integer()
  rez[rez==0] <- NA
  rez

}

complete_manquant <- function(x) {
   
  x[is.na(x)] <- as.integer(mean(x, na.rm = TRUE))
  x

}

db_clean <- db %>%
  mutate(
    naissance_annee = nettoyer_partie_date(naissance_date, 1, 4),
    # si absent, prendre l'age moyen
    naissance_annee_complete = complete_manquant(naissance_annee), 
    naissance_mois = nettoyer_partie_date(naissance_date, 5, 6),
    naissance_mois_complete = complete_manquant(naissance_mois), 
    naissance_jour = nettoyer_partie_date(naissance_date, 7, 8),
    naissance_jour_complete = complete_manquant(naissance_jour), 
    naissance_date_brute = naissance_date,
    naissance_date = as.Date(naissance_date, '%Y%m%d'),
    naissance_date_complete = as.Date(paste0(naissance_annee_complete, '-', naissance_mois_complete, '-', naissance_jour_complete)),
    deces_annee = nettoyer_partie_date(deces_date, 1, 4),
    # si absent, prendre l'age moyen
    deces_annee_complete = complete_manquant(deces_annee), 
    deces_mois = nettoyer_partie_date(deces_date, 5, 6),
    deces_mois_complete = complete_manquant(deces_mois), 
    deces_jour = nettoyer_partie_date(deces_date, 7, 8),
    deces_jour_complete = complete_manquant(deces_jour), 
    deces_date = as.Date(deces_date, '%Y%m%d'),
    deces_date_complete = as.Date(paste0(deces_annee_complete, '-', deces_mois_complete, '-', deces_jour_complete))
  ) 

sum(is.na(db_clean$naissance_annee))
sum(is.na(db_clean$naissance_mois))
sum(is.na(db_clean$naissance_jour))
any(is.na(db_clean$naissance_date_complete))
any(is.na(db_clean$deces_date_complete))

# Identifier le département FR en fonction du code lieu
# Télécharger les données


url_nomenclatures <- 'https://www.insee.fr/fr/statistiques/fichier/4316069/cog_ensemble_2020_csv.zip'

if (!file.exists(file.path(dossier_donnees_externes, basename(url_nomenclatures)))) {
   
zip_nomenclatures_insee <- dl_fichier('https://www.insee.fr/fr/statistiques/fichier/4316069/cog_ensemble_2020_csv.zip')

list_fichiers <- unzip(zip_nomenclatures_insee, exdir = 'inst/extdata')

}

communes <- read_csv('inst/extdata/communes2020.csv')
departements <- read_csv('inst/extdata/departement2020.csv')
regions <- read_csv('inst/extdata/region2020.csv')
pays <- read_csv('inst/extdata/pays2020.csv')

any(duplicated(communes$com))
communes %>% filter(duplicated(com)) # Duplicats : COMD -> ne prendre que les COM ? 

# Préparer une base de commune sans dupliqué (en prenant la première occurence)
communes_deduplique <- communes %>% filter(!duplicated(com))
any(duplicated(communes$com[communes$typecom == 'COM']))

dbp <- db_clean %>%
  left_join(
    communes_deduplique %>%  
      transmute(
        deces_code_lieu = com,
        deces_region = as.character(reg),
        deces_dep = dep,
        deces_commune_libelle = libelle
        )
    ) %>%
  left_join(
    departements %>% 
      select(
        deces_dep = dep, 
        deces_dep_libelle = libelle
        )
    ) %>%
  left_join(regions %>% select(deces_region = reg, deces_region_libelle = libelle)) %>%
  left_join(
    pays %>% 
      filter(actual == 1) %>% 
      select(
        deces_code_lieu = cog, deces_pays = libcog))

sum(is.na(dbp$deces_code_lieu))


sum(is.na(dbp$deces_dep))
dbp %>% filter(is.na(deces_dep)) %>% select(naissance_commune, deces_code_lieu, deces_pays) %>% group_by(deces_code_lieu, deces_pays) %>% summarise(n = n()) %>% arrange(desc(n))

dbp %>% filter(deces_code_lieu == '98736')

# Il manque encore les COM

# Ceci devrait suffire pour notre pyramide des ages en france (hors COM)
saveRDS(dbp, file = 'data/deces_fr.rds')
