# append data from db_deces_4_sql.RDS

db_deces_4_sql <- readRDS(file = file.path(dossier_data, "db_deces_4_sql.RDS"))

RPostgres::dbAppendTable(
  con = con, 
  name = Id(schema = "dcd", table = "deces"), 
  value = db_deces_4_sql
)
