options(java.parameters = "-Xmx16000m")
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


############# READ DS TARGET MASTER  ########################
gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_Target_Master.csv",bucket = gcs_bucket,saveToDisk = "temp_file.csv",overwrite = TRUE)
Rep_Master<-read.csv("temp_file.csv")    
CP_check<-Rep_Master
#######################################################

print(1)

###############TAKE MLE SUBSET FROM DS TARGET MASTER 
MLE_Patch<-Rep_Master %>% filter(BU %in% c("EmergingElectronics","CoreElectronics","Mobile","LargeAppliances","Lifestyle"))
###################################################

###############Take BGM Permanent Patch from DS TARGET MASTER
BGM_FSNs=gcs_get_object("ipc/Nav/IPC_Hyperlocal/BGM_Permanent_IWIT.csv",bucket = gcs_bucket2)
BGM_permananet_Patch<-Rep_Master %>% filter(FSN %in% BGM_FSNs$FSN)

################Read DS _Master
ds_master=gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
ds_master<-setDT(ds_master)  #set as data table
ds_master = ds_master[,`:=`(use_case_m = ifelse(Cron_Timing %in% c("ME","M"),"hl_nl_ds",""))]
ds_master = ds_master[,`:=`(use_case_e = ifelse(Cron_Timing %in% c("ME","E"),"hl_nl_ds_02",""))]
#################################
print(3)

# To split the DS master for morning evening separate
ds_1 = ds_master[,.SD]
ds_2 = ds_master[,.SD]
ds_master_m = ds_1[,`:=`(use_case_e = NULL)] #take morning ds data
ds_master_e = ds_2[,`:=`(use_case_m = NULL)]  #take evening ds data
ds_master_m = ds_master_m[use_case_m !='']  #remove blank from usecase 
ds_master_e = ds_master_e[use_case_e !='']  #remove blank from usecase 
setnames(ds_master_m, old='use_case_m',new='use_case') #rename the usecase column
setnames(ds_master_e, old='use_case_e',new='use_case') #rename the usecase column
#####################################################
print(4)

############Take required columns from DS target master
Rep_Master<-Rep_Master %>% select(FSN,Cluster,Target_Units)
Rep_Master$priority_score<-1 #add priority column =1
Rep_Master$use_case<-"hl_nl_ds" #add default usecase 
Rep_Master$min_doh_target<-1  #add default min_doh = 1
Rep_Master$roc<-1 #add default roc = 1
Rep_Master$min_inventory_target<-Rep_Master$Target_Units
################## Keep selected column below
Rep_Master<-Rep_Master %>% select(FSN,priority_score,use_case,Cluster,min_doh_target,roc,Target_Units,min_inventory_target)
colnames(Rep_Master)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
Rep_Master <- Rep_Master %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)
#########################
print(5)

####################### Reading Tetra norm 
DBEFM<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/target_based_tetrapack_base.csv", bucket = gcs_bucket2)
#DBEFM<-read.csv("temp_file.csv")
DBEFM$fsn<-toupper(DBEFM$fsn)
DBEFM$Norm<-round(DBEFM$Norm,0)
#####################################

##############NOT RUNNING
####################### IGNORE TILL TETRA NORM ARE ACCOUNTED AGAIN
while(FALSE){ # to ignore below lines 
  DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
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
}
######################################

#####MLE AND LIFESTYLE PATCH
MLE_Patch$min_inventory_target<-ifelse(MLE_Patch$top200=="Y",5,1)
MLE_Patch<-MLE_Patch %>% select(FSN,Cluster,Target_Units,min_inventory_target)
MLE_Patch$priority_score<-1
MLE_Patch$min_doh_target<-1
MLE_Patch$roc<-1
MLE_Patch<-MLE_Patch %>% select(FSN,priority_score,Cluster,min_doh_target,roc,Target_Units,min_inventory_target)
setDT(MLE_Patch)
###NL morning evening patch
ds_master_mle = ds_master[,`:=`(use_case_m = ifelse(Cron_Timing %in% c("ME","M"),"nl_ds",""))]
ds_master_mle = ds_master[,`:=`(use_case_e = ifelse(Cron_Timing %in% c("ME","E"),"nl_ds",""))]
ds_1_mle = ds_master_mle[,.SD]
ds_2_mle = ds_master_mle[,.SD]
ds_master_m_mle = ds_1_mle[,`:=`(use_case_e = NULL)]
ds_master_e_mle = ds_2_mle[,`:=`(use_case_m = NULL)]
ds_master_m_mle = ds_master_m_mle[use_case_m !='']
ds_master_e_mle = ds_master_e_mle[use_case_e !='']
setnames(ds_master_m_mle, old='use_case_m',new='use_case')
setnames(ds_master_e_mle, old='use_case_e',new='use_case')
ds_master_m_mle = ds_master_m_mle[,.(Cluster,use_case)]
ds_master_e_mle = ds_master_e_mle[,.(Cluster,use_case)]
##########################################################
######################Make Policy table for MLE
iwit_m_mle = ds_master_m_mle[MLE_Patch, on =c('Cluster')]
iwit_m_mle = iwit_m_mle[,.(FSN,priority_score,use_case,Cluster,min_doh_target,roc,Target_Units,min_inventory_target)]
iwit_m_mle= iwit_m_mle[!is.na(use_case)]

iwit_e_mle = ds_master_e_mle[MLE_Patch, on =c('Cluster')]
iwit_e_mle = iwit_e_mle[,.(FSN,priority_score,use_case,Cluster,min_doh_target,roc,Target_Units,min_inventory_target)]
iwit_e_mle= iwit_e_mle[!is.na(use_case)]

temp_mle=rbind(iwit_m_mle,iwit_e_mle)
colnames(temp_mle)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
MLE_Patch <- temp_mle %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)

##########Merge MLE and other master
Rep_Master = rbind(Rep_Master,MLE_Patch)
setDT(Rep_Master)
#########################################


##### BGM Permanent Patch
BGM_permananet_Patch$min_inventory_target<-ifelse(BGM_permananet_Patch$top200=="Y",5,1)
BGM_permananet_Patch<-BGM_permananet_Patch %>% select(FSN,Cluster,Target_Units,min_inventory_target)
BGM_permananet_Patch$priority_score<-1
BGM_permananet_Patch$min_doh_target<-1
BGM_permananet_Patch$roc<-1
BGM_permananet_Patch<-BGM_permananet_Patch %>% select(FSN,priority_score,Cluster,min_doh_target,roc,Target_Units,min_inventory_target)
setDT(BGM_permananet_Patch)
###NL morning evening patch
ds_master_bgm = ds_master[,`:=`(use_case_m = ifelse(Cron_Timing %in% c("ME","M"),"nl_ds",""))]
ds_master_bgm = ds_master[,`:=`(use_case_e = ifelse(Cron_Timing %in% c("ME","E"),"nl_ds",""))]
ds_master_bgm = ds_master_bgm[,.SD]
ds_2_bgm = ds_master_bgm[,.SD]
ds_master_m_bgm = ds_master_bgm[,`:=`(use_case_e = NULL)]
ds_master_e_bgm = ds_2_mle[,`:=`(use_case_m = NULL)]
ds_master_m_bgm = ds_master_m_bgm[use_case_m !='']
ds_master_e_bgm = ds_master_e_bgm[use_case_e !='']
setnames(ds_master_m_bgm, old='use_case_m',new='use_case')
setnames(ds_master_e_bgm, old='use_case_e',new='use_case')
ds_master_m_bgm = ds_master_m_bgm[,.(Cluster,use_case)]
ds_master_e_bgm = ds_master_e_bgm[,.(Cluster,use_case)]
##########################################################
######################Make Policy table for BGM Permanent
iwit_m_bgm = ds_master_m_bgm[BGM_permananet_Patch, on =c('Cluster'),allow.cartesian = TRUE]
iwit_m_bgm = iwit_m_bgm[,.(FSN,priority_score,use_case,Cluster,min_doh_target,roc,Target_Units,min_inventory_target)]
iwit_m_bgm= iwit_m_bgm[!is.na(use_case)]

iwit_e_bgm = ds_master_e_bgm[BGM_permananet_Patch, on =c('Cluster'),allow.cartesian = TRUE]
iwit_e_bgm = iwit_e_bgm[,.(FSN,priority_score,use_case,Cluster,min_doh_target,roc,Target_Units,min_inventory_target)]
iwit_e_bgm= iwit_e_bgm[!is.na(use_case)]

temp_bgm=rbind(iwit_m_bgm,iwit_e_bgm)
colnames(temp_bgm)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
BGM_permananet_Patch <- temp_bgm %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)

##########Merge BGM Permanent and other master
Rep_Master = rbind(Rep_Master,BGM_permananet_Patch)
setDT(Rep_Master)
#########################################


################Tropical Master target for active FSNs
Trop_Master<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/Tropical_Master.csv", bucket = gcs_bucket2)
DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
Trop_Master$'Input Date' <- as.Date(Trop_Master$'Input Date', format = "%d-%m-%Y")
Trop_Master$'End Date' <- as.Date(Trop_Master$'End Date', format = "%d-%m-%Y") 
Trop_Master$Current_date<-Sys.Date()
Trop_Master$Current_date <- as.Date(Trop_Master$Current_date, format = "%Y-%m-%d") 
Trop_Master$check <- ifelse(Trop_Master$Current_date >= Trop_Master$'Input Date' & Trop_Master$Current_date <= Trop_Master$'End Date', 1, 0)
Trop_Master<-subset(Trop_Master,check>0)
Trop_Master<-Trop_Master %>% select(Darkstore,FSN,Qty)
Trop_Master<-subset(Trop_Master,Qty>0)
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
#################################################

iwit_3 = IWIT_Master[use_case %in% c('nl_ds','nl_ds')] #NL to DS policy
iwit_12 =IWIT_Master[!use_case %in% c('nl_ds','nl_ds')] # everything else

############ creating morning SH cron policy
iwit_12[,`:=`(use_case= NULL)] #remove use_case column
iwit_12_2 = iwit_12[,.SD] # clone of iwit_12
setDT(ds_master_m)
ds_master_m = ds_master_m[,.(Cluster,use_case)]
setnames(ds_master_m, old='Cluster',new='cluster')
iwit_12 = ds_master_m[iwit_12, on =c('cluster'),allow.cartesian = TRUE]
iwit_12 = iwit_12[,.(fsn,priority_score,use_case,cluster,min_doh_target,roc,inventory_target,min_inventory_target)]
iwit_12= iwit_12[!is.na(use_case)]
##################################
###########    creating evening SH cron policy
ds_master_e = ds_master_e[,.(Cluster,use_case)]
setnames(ds_master_e, old='Cluster',new='cluster')
iwit_12_2 = ds_master_e[iwit_12_2, on =c('cluster')]
iwit_12_2 = iwit_12_2[,.(fsn,priority_score,use_case,cluster,min_doh_target,roc,inventory_target,min_inventory_target)]
iwit_12_2= iwit_12_2[!is.na(use_case)]
###################################

##Merge morning evening SH policy
temp=rbind(iwit_12,iwit_12_2)
###Merge all the use cases
iwit_12<-rbind(iwit_12,iwit_3,iwit_12_2) #iwit_12= morning SH,iwit_3=NL to DS,iwit_12_2=evening SH
IWIT_Master_temp<-IWIT_Master
IWIT_Master<-iwit_12
IWIT_Master<- IWIT_Master[order(-IWIT_Master$inventory_target), ] # to take highest target duing overrides 
IWIT_Master<-subset(IWIT_Master,!(cluster %in% c('0')))
setDT(IWIT_Master)
#################

##############NOT RUNNING
####Patch for Bionla & DIC split for destination
while(FALSE){ # not using currently
  bin_dic<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/bin_dic.csv", bucket = gcs_bucket2)
  setDT(bin_dic)
  IWIT_Master[,`:=`(check = ifelse(cluster %in% unique(bin_dic$`Destination Cluster code`) & use_case %in% c('hl_nl_ds_02','hl_nl_ds'),1,0))]
  IWIT_Master[,`:=`(use_case = ifelse(check ==1,'hl_ds_ahdoc',use_case))] 
  IWIT_Master[,`:=`(check = NULL)]
  IWIT_Master = unique(IWIT_Master)
}

#######****************SOURCE WORKING STARTS HERE *************************
#### Take unique FSN three data copy 
FSN<-IWIT_Master %>% select(fsn)
FSN2<-IWIT_Master %>% select(fsn)
FSN3<-IWIT_Master %>% select(fsn)
Src_Cluster <- data.frame(cluster = c("CLUS_SFC_bhi_pad", "CLUS_SFC_BEN_HOS2", "CLUS_SFC_new_new","CLUS_SFC_ulu_sh","CLUS_SFX_bin_sh","CLUS_SFX_luc_gsh","CLUS_SFX_guw_gsh"))
temp_master<-IWIT_Master
#############################

######To replice usecase for all source
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
########################################

##############NOT RUNNING
while(FALSE){
  ####binola patch 1
  binola_fsn <-gcs_get_object("ipc/Nav/IPC_Hyperlocal/binola_exclusive_fsn.csv", bucket = gcs_bucket2)
  setDT(binola_fsn) 
  binola_fsn = unique(binola_fsn[,.(fsn)])
  FSN[,`:=`(check = ifelse(fsn %in% unique(binola_fsn$fsn) & cluster == 'CLUS_SFX_bin_sh',1,0))]
  FSN = FSN[check == 0]
  FSN[,`:=`(check = NULL)]
}
#########################

#####To replice usecase for all source x usecase
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
######################################

#############NOT RUNNING - Ignore till line 304#######################
while(FALSE){
  ####binola patch 2
  FSN2[,`:=`(check = ifelse(fsn %in% unique(binola_fsn$fsn) & cluster == 'CLUS_SFX_bin_sh',1,0))]
  FSN2 = FSN2[check == 0]
  FSN2[,`:=`(check = NULL)]
  ####Adding Patch for New phase  - adhoc
  FSN3<-distinct(FSN3)
  FSN3$priority_score<-1
  FSN3$use_case<-"hl_ds_ahdoc"
  setDT(FSN3)
  Src_Cluster_del = unique(bin_dic[,.(`Source Cluster code`)])
  setnames(Src_Cluster_del, old ='Source Cluster code',new='cluster')
  setDT(Src_Cluster_del)
  FSN3[,`:=`(flag =1)]
  Src_Cluster_del[,`:=`(flag =1)]
  FSN3 = Src_Cluster_del[FSN3, on=c('flag'),allow = TRUE]
  FSN3[,`:=`(flag = NULL)]
  FSN3$min_doh_target<-0
  FSN3$roc<-0
  FSN3$Target_Units<-0
  FSN3$min_inventory_target<-0
  FSN3<-FSN3 %>% select(fsn,priority_score,use_case,cluster,min_doh_target,roc,Target_Units,min_inventory_target)
  colnames(FSN3)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
  IWIT_Master<-rbind(IWIT_Master,FSN3)
}
#############Ignore till line 304#######################

###MLE PATCH 2
mle_FSN<-MLE_Patch %>% select(fsn)
mle_Src_Cluster <- data.frame(cluster = c("CLUS_N_00001_NON_LARGE", "CLUS_S_00001_NON_LARGE","CLUS_W_00001_NON_LARGE"))
mle_FSN<-distinct(mle_FSN)
mle_FSN$priority_score<-1
mle_FSN$use_case<-"nl_ds"
setDT(mle_FSN)
setDT(mle_Src_Cluster)
mle_FSN[,`:=`(flag =1)]
mle_Src_Cluster[,`:=`(flag =1)]
mle_FSN = mle_Src_Cluster[mle_FSN, on=c('flag'),allow = TRUE]
mle_FSN[,`:=`(flag =NULL)]
mle_FSN$min_doh_target<-0
mle_FSN$roc<-0
mle_FSN$Target_Units<-0
mle_FSN$min_inventory_target<-0
mle_FSN=mle_FSN[,.(fsn,priority_score,use_case,cluster,min_doh_target,roc,Target_Units,min_inventory_target)]
colnames(mle_FSN)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
IWIT_Master<-rbind(IWIT_Master,mle_FSN)
#################################


###MLE PATCH 3
mle_FSN<-MLE_Patch %>% select(fsn)
mle_Src_Cluster <- data.frame(cluster = c("CLUS_N_00001_NON_LARGE", "CLUS_S_00001_NON_LARGE","CLUS_W_00001_NON_LARGE"))
#mle_Src_Cluster <- data.frame(cluster = c("CLUS_S_00001_NON_LARGE","CLUS_W_00001_NON_LARGE"))
mle_FSN<-distinct(mle_FSN)
mle_FSN$priority_score<-1
mle_FSN$use_case<-"nl_ds"
setDT(mle_FSN)
setDT(mle_Src_Cluster)
mle_FSN[,`:=`(flag =1)]
mle_Src_Cluster[,`:=`(flag =1)]
mle_FSN = mle_Src_Cluster[mle_FSN, on=c('flag'),allow = TRUE]
mle_FSN[,`:=`(flag =NULL)]
mle_FSN$min_doh_target<-0
mle_FSN$roc<-0
mle_FSN$Target_Units<-0
mle_FSN$min_inventory_target<-0
mle_FSN=mle_FSN[,.(fsn,priority_score,use_case,cluster,min_doh_target,roc,Target_Units,min_inventory_target)]
colnames(mle_FSN)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
IWIT_Master<-rbind(IWIT_Master,mle_FSN)
################################



###BGM Permanent PATCH 2
bgm_FSN<-BGM_permananet_Patch %>% select(fsn)
bgm_Src_Cluster <- data.frame(cluster = c("CLUS_N_00001_NON_LARGE", "CLUS_S_00001_NON_LARGE","CLUS_W_00001_NON_LARGE"))
bgm_FSN<-distinct(bgm_FSN)
bgm_FSN$priority_score<-1
bgm_FSN$use_case<-"nl_ds"
setDT(bgm_FSN)
setDT(bgm_Src_Cluster)
bgm_FSN[,`:=`(flag =1)]
bgm_Src_Cluster[,`:=`(flag =1)]
bgm_FSN = bgm_Src_Cluster[bgm_FSN, on=c('flag'),allow = TRUE]
bgm_FSN[,`:=`(flag =NULL)]
bgm_FSN$min_doh_target<-0
bgm_FSN$roc<-0
bgm_FSN$Target_Units<-0
bgm_FSN$min_inventory_target<-0
bgm_FSN=bgm_FSN[,.(fsn,priority_score,use_case,cluster,min_doh_target,roc,Target_Units,min_inventory_target)]
colnames(bgm_FSN)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
IWIT_Master<-rbind(IWIT_Master,bgm_FSN)
#################################


###BGM Permanent PATCH 3
bgm_FSN<-BGM_permananet_Patch %>% select(fsn)
#mle_Src_Cluster <- data.frame(cluster = c("CLUS_N_00001_NON_LARGE", "CLUS_S_00001_NON_LARGE","CLUS_W_00001_NON_LARGE"))
#mle_Src_Cluster <- data.frame(cluster = c("CLUS_S_00001_NON_LARGE","CLUS_W_00001_NON_LARGE"))
bgm_FSN<-distinct(bgm_FSN)
bgm_FSN$priority_score<-1
bgm_FSN$use_case<-"nl_ds"
setDT(bgm_FSN)
setDT(bgm_Src_Cluster)
bgm_FSN[,`:=`(flag =1)]
bgm_Src_Cluster[,`:=`(flag =1)]
bgm_FSN = bgm_Src_Cluster[bgm_FSN, on=c('flag'),allow = TRUE]
bgm_FSN[,`:=`(flag =NULL)]
bgm_FSN$min_doh_target<-0
bgm_FSN$roc<-0
bgm_FSN$Target_Units<-0
bgm_FSN$min_inventory_target<-0
bgm_FSN=bgm_FSN[,.(fsn,priority_score,use_case,cluster,min_doh_target,roc,Target_Units,min_inventory_target)]
colnames(bgm_FSN)<-c('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
IWIT_Master<-rbind(IWIT_Master,bgm_FSN)
################################


###############
IWIT_Master$fsn<-trimws(IWIT_Master$fsn) #trim whitespace
IWIT_Master<- IWIT_Master[order(-IWIT_Master$inventory_target), ]
IWIT_Master$min_doh_target <- ifelse(IWIT_Master$use_case %in% c("hl_nl_ds", "hl_nl_ds_02") & IWIT_Master$inventory_target > 5,0.7,1)
IWIT_Master$min_inventory_target<-ifelse(IWIT_Master$inventory_target<1,0,1)
####################################

######## Trim white spaces
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
IWIT_Master <- trim_spaces(IWIT_Master) #function call
##########################

##### to replicate delhi and lucknow for both use case
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
#######################################
IWIT_Master<-rbind(IWIT_Master,temp)
###########################


#########NOT RUNNING
while(FALSE){
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
  FSN[,`:=`(check = ifelse(fsn %in% unique(binola_fsn$fsn) & cluster == 'CLUS_SFX_bin_sh',1,0))]
  FSN = FSN[check == 0]
  FSN[,`:=`(check = NULL)]
  IWIT_Master<-rbind(IWIT_Master,FSN)
  
  ########################
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
  FSN2[,`:=`(check = ifelse(fsn %in% unique(binola_fsn$fsn) & cluster == 'CLUS_SFX_bin_sh',1,0))]
  FSN2 = FSN2[check == 0]
  FSN2[,`:=`(check = NULL)]
  
  IWIT_Master<-rbind(IWIT_Master,FSN2)
}
##############ignore till line 440

################
##removing min-max logic for Casepack fsns###########################
CP_check<-subset(CP_check,Case_Pack>1)
CP_check<-CP_check %>% select(Cluster,FSN)
CP_check$check<-1
IWIT_Master<-merge(IWIT_Master,CP_check,by.x = c('cluster','fsn'),by.y = c('Cluster','FSN'),all.x = TRUE)
IWIT_Master$check[is.na(IWIT_Master$check)]<-0
IWIT_Master$min_doh_target<-ifelse(IWIT_Master$check>0,1,IWIT_Master$min_doh_target)
IWIT_Master<- IWIT_Master%>% select(-check) 
IWIT_Master<-IWIT_Master %>% select('fsn','priority_score','use_case','cluster','min_doh_target','roc','inventory_target','min_inventory_target')
##############################

####################PATCH TO MAKE OOM POLICY
oom <-gcs_get_object("ipc/Nav/IPC_Hyperlocal/hl_oom_master.csv",bucket = gcs_bucket2)
oom <- subset(oom,select = c(FSN,Cluster))
oom <- oom %>% mutate(fsn = FSN,
                      cluster = Cluster) %>% select(-FSN,-Cluster)
oom <- oom %>% mutate(priority_score = 1,
                      use_case = "dummy",
                      min_doh_target = 1,
                      roc = 1,
                      inventory_target = 0,
                      min_inventory_target=0) 
# Step 1: Join oom with a mapping table containing DS, Cluster, Cron_Timing
# Replace `cron_map` with your actual source table containing this mapping
oom <- oom %>%
  left_join(DS %>% select(Cluster, Cron_Timing), by = c("cluster"="Cluster"))

# Step 2: Create a base with M and E cases
oom_single <- oom %>%
  filter(Cron_Timing %in% c("M", "E")) %>%
  mutate(
    use_case = case_when(
      Cron_Timing == "M" ~ "hl_nl_ds",
      Cron_Timing == "E" ~ "hl_nl_ds_02"
    )
  )

# Step 3: Handle ME separately by duplicating rows
oom_dual <- oom %>%
  filter(Cron_Timing == "ME") %>%
  slice(rep(1:n(), each = 2)) %>%  # duplicate each row
  mutate(
    use_case = rep(c("hl_nl_ds", "hl_nl_ds_02"), times = nrow(.) / 2)
  )

# Step 4: Combine all together
oom <- bind_rows(oom_single, oom_dual)
oom <- oom %>% select(-Cron_Timing)
oom <- oom %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)
IWIT_Master<-rbind(IWIT_Master,oom)

FSN<-oom %>% select(fsn)
FSN2<-oom %>% select(fsn)
temp_master<-IWIT_Master
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
########################################
#####To replice usecase for all source x usecase
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
######################################


##################################################

#################################

# Step 1: Save initial target before any modification
initial_target <- IWIT_Master %>%
  group_by(cluster, use_case) %>%
  summarise(initial_target = sum(inventory_target, na.rm = TRUE), .groups = "drop")


##################PATCH TO MAP NEW TARGET FROM SH ORCHESTRATOIN
dsmaster <- gcs_get_object("ipc/Nav/IPC_Hyperlocal/sh_orchestration_master.csv",bucket=gcs_bucket2)
dsmaster <- subset(dsmaster,select = c(DS,Cluster,FSN,Target_Units_New))
dsmaster <- dsmaster %>% distinct(DS, FSN, .keep_all = TRUE)
#listA <- c("nl_ds")  # Define exclusion list
IWIT_Master <- IWIT_Master %>%
  left_join(dsmaster, by = c("cluster" = "Cluster", "fsn" = "FSN")) %>%
  mutate(
    inventory_target = ifelse(
      !is.na(Target_Units_New) & !(use_case %in% ('nl_ds')),
      Target_Units_New,
      inventory_target
    )
  ) %>%
  select(-Target_Units_New, -DS)



IWIT_Master <- IWIT_Master %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)
aa =IWIT_Master[,.SD]
###### Seller Removal Patch getting added 
prod_cat = gcs_get_object("ipc/Nav/IPC_Hyperlocal/prod_cat.csv", bucket = gcs_bucket2)
setnames(prod_cat, old='product_id',new='fsn')
prod_cat = unique(prod_cat)
setDT(prod_cat)
prod_cat[,`:=`(flag =1)]
prod_cat[,`:=`(cu =cumsum(flag)),by=.(fsn)]
prod_cat = prod_cat[cu ==1]
prod_cat[,`:=`(cu = NULL,flag = NULL)]
setDT(IWIT_Master)
IWIT_Master_src = IWIT_Master[!cluster %in% DS$Cluster]
IWIT_Master_dest = IWIT_Master[cluster %in% DS$Cluster]

check = unique(IWIT_Master_dest[,.(fsn,cluster)])
check= prod_cat[check, on =c('fsn')]
missing = check[is.na(cms_vertical)]
missing = unique(missing[,.(fsn)])
gcs_upload(missing, name = "ipc/Nav/Hyperlocal_IPC/missing_fsn_pp.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

#input files
fsn_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/seller_inputs/fdp_exclusion.csv", bucket = gcs_bucket2)
setDT(fsn_master)
vertical_master=gcs_get_object("ipc/Nav/IPC_Hyperlocal/seller_inputs/TCR_HL_Seller_Mapping - vertical.csv", bucket = gcs_bucket2)
setDT(vertical_master)
vertical_master[,`:=`(VERTICAL = tolower(VERTICAL))]
brand_vertical=gcs_get_object("ipc/Nav/IPC_Hyperlocal/seller_inputs/TCR_HL_Seller_Mapping - brand_vertical.csv", bucket = gcs_bucket2)
setDT(brand_vertical)
brand_vertical[,`:=`(vertical = tolower(vertical),brand =tolower(brand))]

check = check[,`:=`(cms_vertical = tolower(cms_vertical))]
check = check[,`:=`(brand = tolower(brand))]

check = check[,`:=`(cms_flag = ifelse(cms_vertical %in% vertical_master$VERTICAL,1,0))]
check = check[,`:=`(cms_flag = ifelse(is.na(cms_flag),0,cms_flag))]

check[,`:=`(brand_vertical_flag = ifelse(cms_vertical %in% unique(brand_vertical$vertical) & brand %in% unique(brand_vertical$brand),1,0))]
check[,`:=`(brand_vertical_flag = ifelse(is.na(brand_vertical_flag),0,brand_vertical_flag))]

check[,`:=`(FSN_tag = ifelse(fsn %in% unique(fsn_master$a.fsn),1,0))]
check[,`:=`(FSN_tag = ifelse(is.na(FSN_tag),0,FSN_tag))]
check[,`:=`(final = FSN_tag + cms_flag + brand_vertical_flag)]
check[,`:=`(final = ifelse(final>0,1,final))]
check = check[final ==1]
check = check[,.(fsn,cluster)]
setDT(DS)
DS_temp = DS[,.(DS,Cluster)]
setnames(DS_temp, old ='Cluster',new='cluster')
check = DS_temp[check, on =c('cluster'),allow.cartesian = TRUE]

exclusion_list=gcs_get_object("ipc/Nav/IPC_Hyperlocal/seller_inputs/exclusion_list.csv", bucket = gcs_bucket2)
setDT(exclusion_list)
exclusion_list = exclusion_list[date <= Sys.Date()]
exclusion_list[,`:=`(to_exclude =1)]
exclusion_list[,`:=`(date = NULL)]
exclusion_list = unique(exclusion_list)
exclusion_list[,`:=`(fsn = as.character(fsn),DS =as.character(DS))]
exclusion_list_all = exclusion_list[ DS =='all']
exclusion_list = exclusion_list[ DS!='all']

check =exclusion_list[check, on=c('fsn','DS')]

check[,`:=`(to_exclude = ifelse(is.na(to_exclude),0,to_exclude))]
check[,`:=`(to_exclude = ifelse(fsn %in% unique(exclusion_list_all$fsn),1,to_exclude))]
check[,`:=`(to_exclude = ifelse(is.na(to_exclude),0,to_exclude))]

check[,`:=`(norm_0 = 1)]
check[,`:=`(norm_0 = ifelse(is.na(norm_0),0,norm_0))]
check[,`:=`(norm_0 = ifelse(to_exclude ==1,0,norm_0))]
check= check[norm_0 == 1]
gcs_upload(check, name = "ipc/Nav/Hyperlocal_IPC/seller_norm_0.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
check = check[,.(fsn,cluster,norm_0)]

IWIT_Master_dest = check[IWIT_Master_dest, on =c('fsn','cluster')]
IWIT_Master_dest[,`:=`(norm_0 = ifelse(is.na(norm_0),0,norm_0))]
IWIT_Master_dest = IWIT_Master_dest[,`:=`(inventory_target=ifelse(norm_0 ==1,0,inventory_target)) ]
IWIT_Master_dest[,`:=`(norm_0 = NULL)]
IWIT_Master = rbind(IWIT_Master_dest,IWIT_Master_src)
IWIT_Master$min_inventory_target<-IWIT_Master$inventory_target
IWIT_Master <- IWIT_Master %>% distinct(fsn, use_case, cluster, .keep_all = TRUE)
############
# Step 2: Compute new target after modification
new_target <- IWIT_Master %>%
  group_by(cluster, use_case) %>%
  summarise(new_target = sum(inventory_target, na.rm = TRUE), .groups = "drop")

# Step 3: Compare
comparison <- initial_target %>%
  full_join(new_target, by = c("cluster", "use_case")) %>%
  mutate(
    target_diff = new_target - initial_target
  ) %>%
  arrange(desc(abs(target_diff)))

print(comparison)

# make data type standard for policy
IWIT_Master <- IWIT_Master %>%
  mutate(
    fsn = as.character(fsn),
    cluster = as.character(cluster),
    priority_score = as.integer(priority_score),
    use_case = as.character(use_case),
    min_doh_target = as.numeric(min_doh_target),
    roc = as.integer(roc),
    inventory_target = as.integer(inventory_target),
    min_inventory_target = as.integer(min_inventory_target)
  )



gcs_upload(IWIT_Master, name = "ipc/Nav/Hyperlocal_IPC/HL_Planning_Master_Policy.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
gcs_upload(comparison, name = "ipc/Nav/Hyperlocal_IPC/HL_Planning_Master_Policy_Report.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

print('files uploaded')
print('deleting cache')
rm(list = ls())
print('deleted')
