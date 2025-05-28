rm(list = ls())
setwd("D:/shruti.shahi/Temp")
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

# googlesheets4::gs4_auth(
#   path = "\\\\172.29.66.90\\Brain\\Config Engine\\config_run_auto\\myntra-planning-81e3bc0e8693.json",
#   scopes = "https://www.googleapis.com/auth/spreadsheets"
# )
1


project_id <- "fks-ip-azkaban"
auth_file <- "C:\\Users\\shruti.shahi\\Desktop\\MLE\\fks-ip-azkaban-sak (2) (1).json"
gcs_bucket <- "fk-ipc-data-sandbox"
gcs_bucket2 <- "fk-ipc-adhoc-data-sandbox"
gcs_auth(json_file = auth_file)
gcs_global_bucket("fk-ipc-data-sandbox")
Sys.setenv("GCS_DEFAULT_BUCKET" = gcs_bucket, "GCS_AUTH_FILE" = auth_file)
options(googleCloudStorageR.upload_limit = 5000000L)
gcs_get_object