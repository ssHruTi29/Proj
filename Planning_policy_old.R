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
Rep_Master<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_Target_Master.csv", bucket = gcs_bucket)
MLE_Patch<-Rep_Master %>% filter(BU %in% c("EmergingElectronics","CoreElectronics","Mobile","LargeAppliances","Lifestyle"))

Rep_Master$min_inventory_target<-ifelse(Rep_Master$top200=="Y",5,1)
Rep_Master<-Rep_Master %>% select(FSN,Cluster,Target_Units,min_inventory_target)

Rep_Master$priority_score<-1
Rep_Master$use_case<-"hl_nl_ds"
Rep_Master$min_doh_target<-1
Rep_Master$roc<-1
##Rep_Master$min_inventory_target<-ifelse(Rep_Master$top200=="Y",5,1)
Rep_Master<-Rep_Master %>% select(FSN,priority_score,use_case,Cluster,min_doh_target,roc,Target_Units,min_inventory_target)
colnames(Rep_Master)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
Rep_Master <- Rep_Master %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)

#####MLE AND LIFESTYLE PATCH

MLE_Patch$min_inventory_target<-ifelse(MLE_Patch$top200=="Y",5,1)
MLE_Patch<-MLE_Patch %>% select(FSN,Cluster,Target_Units,min_inventory_target)

MLE_Patch$priority_score<-1
MLE_Patch$use_case<-"nl_ds"
MLE_Patch$min_doh_target<-1
MLE_Patch$roc<-1
##Rep_Master$min_inventory_target<-ifelse(Rep_Master$top200=="Y",5,1)
MLE_Patch<-MLE_Patch %>% select(FSN,priority_score,use_case,Cluster,min_doh_target,roc,Target_Units,min_inventory_target)
colnames(MLE_Patch)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
MLE_Patch <- MLE_Patch %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)

Rep_Master = rbind(Rep_Master,MLE_Patch)

####fsn -> changes & usecase


Trop_Master<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/Tropical_Master.csv", bucket = gcs_bucket2)

Trop_Master$'Input Date' <- as.Date(Trop_Master$'Input Date', format = "%d-%m-%Y")
Trop_Master$'End Date' <- as.Date(Trop_Master$'End Date', format = "%d-%m-%Y")
Trop_Master$Current_date<-Sys.Date()
Trop_Master$Current_date <- as.Date(Trop_Master$Current_date, format = "%Y-%m-%d")
Trop_Master$check <- ifelse(Trop_Master$Current_date >= Trop_Master$'Input Date' & Trop_Master$Current_date <= Trop_Master$'End Date', 1, 0)
Trop_Master<-subset(Trop_Master,check>0)
Trop_Master<-Trop_Master %>% select(Darkstore,FSN,Qty)
Trop_Master<-subset(Trop_Master,Qty>0)

DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
Trop_Master<-merge(Trop_Master,DS,by.x="Darkstore",by.y="DS")

Trop_Master<-Trop_Master %>% select(FSN,Cluster,Qty)
colnames(Trop_Master)<-c('FSN','Cluster','Target_Units')
Trop_Master$priority_score<-1
Trop_Master$use_case<-"hl_nl_ds"
Trop_Master$min_doh_target<-1
Trop_Master$roc<-1
Trop_Master$min_inventory_target<-Trop_Master$Target_Units
Trop_Master<-Trop_Master %>% select(FSN,priority_score,use_case,Cluster,min_doh_target,roc,Target_Units,min_inventory_target)
colnames(Trop_Master)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
Trop_Master<- Trop_Master[order(-Trop_Master$inventory_target), ]
Trop_Master <- Trop_Master %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)

IWIT_Master<-rbind(Rep_Master,Trop_Master)
#new iwit master with mle patch
#IWIT_Master<-rbind(Rep_Master,Trop_Master,MLE_Patch)

IWIT_Master<- IWIT_Master[order(-IWIT_Master$inventory_target), ]

FSN<-IWIT_Master %>% select(fsn)

Src_Cluster <- data.frame(cluster = c("CLUS_SFC_bhi_pad", "CLUS_SFC_BEN_HOS2", "CLUS_SFC_new_new","CLUS_SFC_ulu_sh"))
FSN<-distinct(FSN)
FSN$priority_score<-1
FSN$use_case<-"hl_nl_ds"
FSN<-merge(FSN,Src_Cluster)
FSN$min_doh_target<-0
FSN$roc<-0
FSN$Target_Units<-0
FSN$min_inventory_target<-0

colnames(FSN)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
IWIT_Master<-rbind(IWIT_Master,FSN)

IWIT_Master$fsn<-trimws(IWIT_Master$fsn)
IWIT_Master<- IWIT_Master[order(-IWIT_Master$inventory_target), ]
IWIT_Master <- IWIT_Master %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)

###MLE PATCH 2

mle_FSN<-MLE_Patch %>% select(fsn)
mle_Src_Cluster <- data.frame(cluster = c("CLUS_E_00001_NON_LARGE", "CLUS_N_00001_NON_LARGE", "CLUS_S_00001_NON_LARGE","CLUS_W_00001_NON_LARGE"))
mle_FSN<-distinct(mle_FSN)
mle_FSN$priority_score<-1
mle_FSN$use_case<-"nl_ds"
mle_FSN<-merge(mle_FSN,mle_Src_Cluster)
mle_FSN$min_doh_target<-0
mle_FSN$roc<-0
mle_FSN$Target_Units<-0
mle_FSN$min_inventory_target<-0

colnames(mle_FSN)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')

IWIT_Master<-rbind(IWIT_Master,mle_FSN)

IWIT_Master$fsn<-trimws(IWIT_Master$fsn)
IWIT_Master<- IWIT_Master[order(-IWIT_Master$inventory_target), ]
IWIT_Master <- IWIT_Master %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)


###source cluster & usecase change

temp_file <- tempfile(fileext = ".csv")
write.csv(IWIT_Master, temp_file, row.names = FALSE)


gcs_upload(file = temp_file, name = "ipc/Nav/Hyperlocal_IPC/HL_Planning_Master_Policy.csv", predefinedAcl = "bucketLevel")