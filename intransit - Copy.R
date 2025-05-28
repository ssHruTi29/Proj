options(java.parameters = "-Xmx8000m")
library(RJDBC)
library(plyr)
library(dplyr)
library(reshape)
library(lubridate)
library(data.table)


################ GCP Write Functions  #################

gcs_save_rds <- function(input, output) {
  saveRDS(input, output)
}

gcs_save_csv <- function(input, output){
  write.csv(input, output, row.names = FALSE)
}

project_id <- "fks-ip-azkaban"
auth_file <- "/usr/share/fk-retail-ipc-azkaban-exec/resources/fks-ip-azkaban-sak.json"
gcs_bucket <- "fk-ipc-data-sandbox"
gcs_bucket2 <- "fk-ipc-adhoc-data-sandbox"
Sys.setenv("GCS_DEFAULT_BUCKET" = gcs_bucket, "GCS_AUTH_FILE" = auth_file)
library(googleCloudStorageR)
options(googleCloudStorageR.upload_limit = 2000000000L)


############# Summary Functions ########################
project_id <- "fks-ip-azkaban"
auth_file <- "/usr/share/fk-retail-ipc-azkaban-exec/resources/fks-ip-azkaban-sak.json"
gcs_bucket <- "fk-ipc-data-sandbox"
gcs_bucket2 <- "fk-ipc-adhoc-data-sandbox"
Sys.setenv("GCS_DEFAULT_BUCKET" = gcs_bucket, "GCS_AUTH_FILE" = auth_file)
library(googleCloudStorageR)
options(googleCloudStorageR.upload_limit = 2000000000L)

############# Summary Functions ########################

ds_it = gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_IT.csv", bucket = gcs_bucket)
setDT(ds_it)

ds_it_check = ds_it[,.SD]
ds_it_check = ds_it_check[Inwarding_Status %in% c('IN_TRANSIT','INWARDING','INITIATED')]
ds_it = ds_it[Inwarding_Status %in% c('IN_TRANSIT','INWARDING','INITIATED')]
ds_it[,`:=`(Creation_Date = as.POSIXct(Creation_Date, format="%Y-%m-%d %H:%M:%S"))]
ds_it[,`:=`(hours = as.numeric(Sys.time()-Creation_Date,units = "hours"))]
ds_it[,`:=`(lt_36 = ifelse(hours <37,1,0))]
ds_it[,`:=`(create_date = as.Date(Creation_Date))]
ds_it_1 = ds_it[,.SD]
nl_hub = gcs_get_object("ipc/Nav/IPC_Hyperlocal/nl_hub.csv", bucket = gcs_bucket2)
nl_hub2 = gcs_get_object("ipc/Nav/IPC_Hyperlocal/nl_hub2.csv", bucket = gcs_bucket2)

sourcing_hub = gcs_get_object("ipc/Nav/IPC_Hyperlocal/sourcing_hub.csv", bucket = gcs_bucket2)

ds_store = gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
nl_to_ds = ds_it[Src_FC %in% unique(nl_hub$code)]
nl_to_ds = nl_to_ds[Dest_FC %in% unique(ds_store$DS)]

gcs_upload(nl_to_ds, name = "ipc/Nav/Hyperlocal_IPC/nl_to_ds.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

sh_to_ds = ds_it[Src_FC %in% unique(sourcing_hub$code)]
sh_to_ds = sh_to_ds[Dest_FC %in% unique(ds_store$DS)]
gcs_upload(sh_to_ds, name = "ipc/Nav/Hyperlocal_IPC/sh_to_ds.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

####Reverse ds to sh
ds_it_reverse = gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_Reverse_IT.csv", bucket = gcs_bucket)
setDT(ds_it_reverse)
ds_to_sh = ds_it_reverse[Dest_FC %in% unique(sourcing_hub$code)]
ds_to_sh = ds_to_sh[Src_FC %in% unique(ds_store$DS)]
gcs_upload(ds_to_sh, name = "ipc/Nav/Hyperlocal_IPC/ds_to_sh.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

####Reverse sh to nl
sh_to_nl = data.table(gcs_get_object("ipc/Nav/Anuj/IWIT_InTransit_FSN.csv", bucket = gcs_bucket))
sh_to_nl = sh_to_nl[Src_FC %in% unique(sourcing_hub$code)]
sh_to_nl = sh_to_nl[Dest_FC %in% unique(nl_hub$code)]
gcs_upload(sh_to_nl, name = "ipc/Nav/Hyperlocal_IPC/sh_to_nl.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

###NL to sh

nl_to_sh = data.table(gcs_get_object("ipc/Nav/Anuj/IWIT_InTransit_FSN.csv", bucket = gcs_bucket))
nl_to_sh = nl_to_sh[Src_FC %in% unique(nl_hub$code) | Src_FC %in% unique(nl_hub2$code)]
nl_to_sh = nl_to_sh[Dest_FC %in% unique(sourcing_hub$code)]
gcs_upload(nl_to_sh, name = "ipc/Nav/Hyperlocal_IPC/nl_to_sh.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")


###DS to nl

###DS to nl

ds_to_nl = data.table(gcs_get_object("ipc/Nav/Anuj/IWIT_InTransit_FSN.csv", bucket = gcs_bucket))
ds_to_nl = ds_to_nl[Dest_FC %in% unique(nl_hub$code) | Src_FC %in% unique(nl_hub2$code)]
ds_to_nl = ds_to_nl[Src_FC %in% unique(ds_store$DS)]
gcs_upload(ds_to_nl, name = "ipc/Nav/Hyperlocal_IPC/ds_to_nl.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
