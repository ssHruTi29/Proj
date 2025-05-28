options(java.parameters = "-Xmx8000m")
library(RJDBC)
library(plyr)
library(dplyr)
library(reshape)
library(lubridate)
library(data.table)
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
Rep_Master<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_Target_Master.csv", bucket = gcs_bucket,saveToDisk = "temp_file.csv",overwrite = TRUE)
Rep_Master<-read.csv("temp_file.csv")

Exclusion<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/Exclusion.csv", bucket = gcs_bucket2,,saveToDisk = "temp_file.csv",overwrite = TRUE)
Exclusion<-read.csv("temp_file.csv")
Rep_Master<-merge(Rep_Master,Exclusion,by.x = c('City','FSN'),by.y = c('City','FSN'),all.x = TRUE)
Rep_Master$Remove[is.na(Rep_Master$Remove)]<-0
Rep_Master<-subset(Rep_Master,Rep_Master$Remove<1)

MLE_Patch<-Rep_Master %>% filter(BU %in% c("EmergingElectronics","CoreElectronics","Mobile","LargeAppliances","Lifestyle"))

#casepack exclusion
DS_Casepack<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_FSN_Casepack.csv", bucket = gcs_bucket2,,saveToDisk = "temp_file.csv",overwrite = TRUE)
DS_Casepack<-read.csv("temp_file.csv")
DS_Casepack$Casepack_Activated<-1
DS_Casepack<- DS_Casepack[ , !(names(DS_Casepack) %in% "Case_Pack")]
Rep_Master<-merge(Rep_Master,DS_Casepack,by.x = c('City','FSN'),by.y = c('City','FSN'),all.x = TRUE)
Rep_Master$Casepack_Activated[is.na(Rep_Master$Casepack_Activated)]<-0
Rep_Master<-subset(Rep_Master,Casepack_Activated<1)

###DS _Master
gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2, saveToDisk = "DS_Master.csv",overwrite=TRUE)
ds_master<-read.csv("DS_Master.csv")
ds_master<-setDT(ds_master)
##ds_master= data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2))
ds_master = ds_master[,`:=`(use_case_m = ifelse(Cron_Timing %in% c("ME","M"),"hl_nl_ds",""))]
ds_master = ds_master[,`:=`(use_case_e = ifelse(Cron_Timing %in% c("ME","E"),"hl_nl_ds_02",""))]
ds_1 = ds_master[,.SD]
ds_2 = ds_master[,.SD]

ds_master_m = ds_1[,`:=`(use_case_e = NULL)]
ds_master_e = ds_2[,`:=`(use_case_m = NULL)]
ds_master_m = ds_master_m[use_case_m !='']
ds_master_e = ds_master_e[use_case_e !='']
setnames(ds_master_m, old='use_case_m',new='use_case')
setnames(ds_master_e, old='use_case_e',new='use_case')

Rep_Master<-Rep_Master %>% select(FSN,Cluster,Target_Units)


Rep_Master$priority_score<-1
Rep_Master$use_case<-"hl_nl_ds"
Rep_Master$min_doh_target<-1
Rep_Master$roc<-1
Rep_Master$min_inventory_target<-Rep_Master$Target_Units
Rep_Master<-Rep_Master %>% select(FSN,priority_score,use_case,Cluster,min_doh_target,roc,Target_Units,min_inventory_target)
colnames(Rep_Master)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
Rep_Master <- Rep_Master %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)

DBEFM<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/target_based_tetrapack_base.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
DBEFM<-read.csv("temp_file.csv")
DBEFM$fsn<-toupper(DBEFM$fsn)
DBEFM$Norm<-round(DBEFM$Norm,0)

DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv", overwrite = TRUE)
DS<-read.csv("temp_file.csv")

##DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
DS1<-DS %>% select(DS,Cluster)
DBEFM<-merge(DBEFM,DS1,by.x="fc",by.y="DS",all.x = TRUE)
DBEFM$Cluster <- ifelse(DBEFM$fc %in% c('ulu_sh_wh_nl_01nl'), "CLUS_SFC_ulu_sh", 
                        ifelse(DBEFM$fc %in% c('ben_hos_wh_nl_02nl'), "CLUS_SFC_BEN_HOS2", 
                               ifelse(DBEFM$fc %in% c('bhi_pad_wh_nl_05nl'), "CLUS_SFC_bhi_pad", 
                                      ifelse(DBEFM$fc %in% c('new_new_wh_nl_01nl'), "CLUS_SFC_new_new", 
                                             ifelse(DBEFM$fc %in% c('bin_sh_wh_nl_01nl'), "CLUS_SFX_bin_sh",DBEFM$Cluster)))))
DBEFM$priority_score<-1
DBEFM$use_case<-"hl_nl_ds"
DBEFM$min_doh_target<-1
DBEFM$roc<-1
DBEFM$min_inventory_target<-DBEFM$Norm

DBEFM<-DBEFM %>% select(fsn,priority_score,use_case,Cluster,min_doh_target,roc,Norm,min_inventory_target)
colnames(DBEFM)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')

Rep_Master<-rbind(Rep_Master,DBEFM)



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

#####Trop Master

Trop_Master<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/Tropical_Master.csv", bucket = gcs_bucket2,saveToDisk = "temp.csv",overwrite=TRUE)
Trop_Master<-read.csv("temp.csv")

Trop_Master$'Input.Date' <- as.Date(Trop_Master$'Input.Date', format = "%d-%m-%Y")
Trop_Master$'End.Date' <- as.Date(Trop_Master$'End.Date', format = "%d-%m-%Y") 
Trop_Master$Current_date<-Sys.Date()
Trop_Master$Current_date <- as.Date(Trop_Master$Current_date, format = "%Y-%m-%d") 
Trop_Master$check <- ifelse(Trop_Master$Current_date >= Trop_Master$'Input.Date' & Trop_Master$Current_date <= Trop_Master$'End.Date', 1, 0)
Trop_Master<-subset(Trop_Master,check>0)
Trop_Master<-Trop_Master %>% select(Darkstore,FSN,Qty)
Trop_Master<-subset(Trop_Master,Qty>0)

DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv", overwrite = TRUE)
DS<-read.csv("temp_file.csv")

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
setDT(IWIT_Master)
iwit_3 = IWIT_Master[use_case=='nl_ds']
iwit_12 =IWIT_Master[use_case!='nl_ds']
iwit_12[,`:=`(use_case= NULL)]
iwit_12_2 = iwit_12[,.SD]
setDT(ds_master_m)
ds_master_m = ds_master_m[,.(Cluster,use_case)]
setnames(ds_master_m, old='Cluster',new='cluster')
iwit_12 = ds_master_m[iwit_12, on =c('cluster')]
iwit_12 = iwit_12[,.(fsn,priority_score,use_case,cluster,min_doh_target,roc,inventory_target,min_inventory_target)]
iwit_12= iwit_12[!is.na(use_case)]

ds_master_e = ds_master_e[,.(Cluster,use_case)]
setnames(ds_master_e, old='Cluster',new='cluster')

iwit_12_2 = ds_master_e[iwit_12_2, on =c('cluster')]
iwit_12_2 = iwit_12_2[,.(fsn,priority_score,use_case,cluster,min_doh_target,roc,inventory_target,min_inventory_target)]
iwit_12_2= iwit_12_2[!is.na(use_case)]


temp=rbind(iwit_12,iwit_12_2)
iwit_12<-rbind(iwit_12,iwit_3,iwit_12_2)

##########
IWIT_Master_temp<-IWIT_Master
IWIT_Master<-iwit_12

IWIT_Master<- IWIT_Master[order(-IWIT_Master$inventory_target), ]
IWIT_Master<-subset(IWIT_Master,!(cluster %in% c('0')))

FSN<-IWIT_Master %>% select(fsn)
FSN2<-IWIT_Master %>% select(fsn)
Src_Cluster <- data.frame(cluster = c("CLUS_SFC_bhi_pad", "CLUS_SFC_BEN_HOS2", "CLUS_SFC_new_new","CLUS_SFC_ulu_sh","CLUS_SFX_bin_sh"))

temp_master<-IWIT_Master
######Source code 1

FSN<-distinct(FSN)
FSN$priority_score<-1
FSN$use_case<-"hl_nl_ds"
setDT(FSN)
setDT(Src_Cluster)
FSN[,`:=`(flag =1)]
Src_Cluster[,`:=`(flag =1)]
FSN = Src_Cluster[FSN, on=c('flag'),allow = TRUE]
FSN[,`:=`(flag = NULL)]

FSN$min_doh_target<-0
FSN$roc<-0
FSN$Target_Units<-0
FSN$min_inventory_target<-0

FSN<-FSN %>% select(fsn,priority_score,use_case,cluster,min_doh_target,roc,Target_Units,min_inventory_target)

colnames(FSN)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
IWIT_Master<-rbind(IWIT_Master,FSN)

#####Source code 2

FSN2<-distinct(FSN2)
FSN2$priority_score<-1
FSN2$use_case<-"hl_nl_ds_02"
setDT(FSN2)
setDT(Src_Cluster)
FSN2[,`:=`(flag =1)]
Src_Cluster[,`:=`(flag =1)]
FSN2 = Src_Cluster[FSN2, on=c('flag'),allow = TRUE]
FSN2[,`:=`(flag = NULL)]
FSN2$min_doh_target<-0
FSN2$roc<-0
FSN2$Target_Units<-0
FSN2$min_inventory_target<-0

FSN2<-FSN2 %>% select(fsn,priority_score,use_case,cluster,min_doh_target,roc,Target_Units,min_inventory_target)

colnames(FSN2)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
IWIT_Master<-rbind(IWIT_Master,FSN2)

###MLE PATCH 2

mle_FSN<-MLE_Patch %>% select(fsn)
mle_Src_Cluster <- data.frame(cluster = c("CLUS_N_00001_NON_LARGE", "CLUS_S_00001_NON_LARGE","CLUS_W_00001_NON_LARGE"))
#mle_Src_Cluster <- data.frame(cluster = c("CLUS_S_00001_NON_LARGE","CLUS_W_00001_NON_LARGE"))
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

IWIT_Master$min_doh_target <- ifelse(IWIT_Master$use_case %in% c("hl_nl_ds", "hl_nl_ds_02") & IWIT_Master$inventory_target > 5,0.7,1)

IWIT_Master$min_inventory_target<-ifelse(IWIT_Master$inventory_target<1,0,1)
##temp_file <- tempfile(filetext = ".csv")
##write.csv(IWIT_Master, temp_file, row.names = FALSE)

trim_spaces <- function(df) {
  df[] <- lapply(df, function(x) {
    if (is.character(x)) {
      # Trim leading, trailing, and multiple spaces
      gsub("^\\s+|\\s+$", "", gsub("\\s+", " ", x))
    } else {
      x
    }
  })
  return(df)
}

IWIT_Master <- trim_spaces(IWIT_Master)





DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv", overwrite = TRUE)
DS<-read.csv("temp_file.csv")

Delhi<-subset(DS,City %in% c('Delhi','Lucknow'))
temp<-subset(IWIT_Master,cluster %in% Delhi$Cluster)
temp1<-temp
temp$use_case<-"hl_nl_ds_02"
temp1$use_case<-"hl_nl_ds"
temp<-distinct(temp)
temp1<-distinct(temp1)
temp<-rbind(temp,temp1)

IWIT_Master<-rbind(IWIT_Master,temp)

##rechechking src
FSN<-IWIT_Master %>% select(fsn)
FSN<-distinct(FSN)
FSN$priority_score<-1
FSN$use_case<-"hl_nl_ds"
setDT(FSN)
setDT(Src_Cluster)
FSN[,`:=`(flag =1)]
Src_Cluster[,`:=`(flag =1)]
FSN = Src_Cluster[FSN, on=c('flag'),allow = TRUE]
FSN[,`:=`(flag = NULL)]

FSN$min_doh_target<-0
FSN$roc<-0
FSN$Target_Units<-0
FSN$min_inventory_target<-0

FSN<-FSN %>% select(fsn,priority_score,use_case,cluster,min_doh_target,roc,Target_Units,min_inventory_target)

colnames(FSN)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
IWIT_Master<-rbind(IWIT_Master,FSN)

FSN2<-IWIT_Master %>% select(fsn)

FSN2<-distinct(FSN2)
FSN2$priority_score<-1
FSN2$use_case<-"hl_nl_ds_02"
setDT(FSN2)
setDT(Src_Cluster)
FSN2[,`:=`(flag =1)]
Src_Cluster[,`:=`(flag =1)]
FSN2 = Src_Cluster[FSN2, on=c('flag'),allow = TRUE]
FSN2[,`:=`(flag = NULL)]
FSN2$min_doh_target<-0
FSN2$roc<-0
FSN2$Target_Units<-0
FSN2$min_inventory_target<-0

FSN2<-FSN2 %>% select(fsn,priority_score,use_case,cluster,min_doh_target,roc,Target_Units,min_inventory_target)

colnames(FSN2)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
IWIT_Master<-rbind(IWIT_Master,FSN2)




IWIT_Master <- IWIT_Master %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)




gcs_upload(IWIT_Master, name = "ipc/Nav/Hyperlocal_IPC/HL_Planning_Master_Policy.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

##gcs_upload(IWIT_Master, name = "ipc/Nav/Hyperlocal_IPC/HL_Planning_Master_Policy.csv", predefinedAcl = "bucketLevel")

