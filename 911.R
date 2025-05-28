rm(list = ls())

#----Initialising----
library(RMySQL)
library(lubridate)
library(stringr)
library(dplyr)
library(data.table)
library(googlesheets4)
library(readxl)
library(fst)
library(arrow)
library(ggplot2)
library(RJDBC)
library(readr)
library(splitstackshape)
library(googleCloudStorageR)
library(googledrive)
library(googlesheets4)
#library(gmailr)
library(httr)

options(scipen = 99999)

gcs_save_rds <- function(input, output) {
  saveRDS(input, output)
}

gcs_save_csv <- function(input, output){
  write.csv(input, output, row.names = FALSE)
}

googlesheets4::gs4_auth(
  path = "\\\\172.29.66.90\\Brain\\Config Engine\\config_run_auto\\myntra-planning-81e3bc0e8693.json",
  scopes = "https://www.googleapis.com/auth/spreadsheets"
)
1


project_id <- "fks-ip-azkaban"
auth_file <- "D:\\vishruti.vora\\fks-ip-azkaban-sak (2).json"
gcs_bucket <- "fk-ipc-data-sandbox"
gcs_bucket2 <- "fk-ipc-adhoc-data-sandbox"
gcs_auth(json_file = auth_file)
gcs_global_bucket("fk-ipc-data-sandbox")
Sys.setenv("GCS_DEFAULT_BUCKET" = gcs_bucket, "GCS_AUTH_FILE" = auth_file)
options(googleCloudStorageR.upload_limit = 5000000L)
gcs_get_object

gamma = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1nHGtdr6-XOjs2iMEuINeut2WlVajyduh7nrIXLV1unI/edit?gid=1061190138#gid=1061190138',sheet ='gamma'))
alpha = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1nHGtdr6-XOjs2iMEuINeut2WlVajyduh7nrIXLV1unI/edit?gid=1061190138#gid=1061190138',sheet ='alpha'))

nine_one = rbind(alpha,gamma)

ds_master= data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2))
setnames(ds_master, old ='DS',new='business_zone')

sh_master = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1mIxIfmVOJ1whZO4bDKD51Wlcqud2l2rzuOgbFt4ges4/edit?gid=966141711#gid=966141711',sheet='sourcing_hub'))
sh_master[,`:=`(facility_id = NULL)]
setnames(sh_master, old =c('code','city'),new=c('sh_code','City'))


nine_one= nine_one[`To Location Site Id` %in% unique(ds_master$business_zone)]
nine_one= nine_one[`From Location Site Id` %in% unique(sh_master$sh_code)]

prod_cat = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1mIxIfmVOJ1whZO4bDKD51Wlcqud2l2rzuOgbFt4ges4/edit?gid=782554625#gid=782554625',sheet ='product_categorization'))
setnames(prod_cat , old ='product_id',new='FSN')

nine_one = prod_cat[nine_one, on=c('FSN')]
write_sheet(nine_one,'https://docs.google.com/spreadsheets/d/1nHGtdr6-XOjs2iMEuINeut2WlVajyduh7nrIXLV1unI/edit?gid=1061190138#gid=1061190138',sheet='final')

gc()
