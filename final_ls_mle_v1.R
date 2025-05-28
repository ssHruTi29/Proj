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
auth_file <- "D:\\vishruti.vora\\fks-ip-azkaban-sak (2).json"
gcs_bucket <- "fk-ipc-data-sandbox"
gcs_bucket2 <- "fk-ipc-adhoc-data-sandbox"
gcs_auth(json_file = auth_file)
gcs_global_bucket("fk-ipc-data-sandbox")
Sys.setenv("GCS_DEFAULT_BUCKET" = gcs_bucket, "GCS_AUTH_FILE" = auth_file)
options(googleCloudStorageR.upload_limit = 5000000L)
gcs_get_object

#####Input Files 
#atp = data.table(fread("E:\\Vishruti\\Hyperlocal\\HL Final  inventory_snapshot_19 Nov 2024 04_36_58 .csv"))
atp = gcs_get_object("ipc/Nav/Hyperlocal_IPC/Inventory_Snapshot.csv", bucket = gcs_bucket)
#atp<-read.csv("temp_file.csv")
setDT(atp)
atp = atp[fc_area == 'store']
Inv = atp[,.SD]

ds_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
#ds_master<-read.csv("temp_file.csv")
setDT(ds_master)

setnames(ds_master, old ='DS',new='business_zone')
ds_master[,`:=`(three = substr(business_zone,1,3))]
ds_master[,`:=`(City = ifelse(three =='pun','Pune',City))]
ds_master[,`:=`(three = NULL)]
store_master = ds_master[,.SD]

facility_mapper = gcs_get_object("ipc/Nav/IPC_Hyperlocal/Facility_Mapper_Dump.csv",bucket = gcs_bucket2)
#facility_mapper<-read.csv("temp_file.csv")
setDT(facility_mapper)

facility_mapper_full = facility_mapper[,.SD]
facility_mapper = facility_mapper[,.(code,sub_type,city)]
setnames(facility_mapper, old ='code',new='business_zone')

###Reading instock DRR
instock_corrected_drr = gcs_get_object("Avinash_HL_Data/Instock_CorrectedDRR_14Days.csv", bucket = gcs_bucket2)
#instock_corrected_drr<-read.csv("temp_file.csv")
setDT(instock_corrected_drr)


###Joining DRR with facility_mapper
instock_corrected_drr = facility_mapper[instock_corrected_drr, on =c('business_zone')]
instock_corrected_drr = store_master[instock_corrected_drr, on =c('business_zone')]
missing_store = unique(instock_corrected_drr[is.na(City)])
missing_store = unique(missing_store[,.(business_zone)])

instock_corrected_drr = instock_corrected_drr[,`:=`(city = NULL)]
instock_corrected_drr_p = instock_corrected_drr[,.(DRR = mean(DRR)),by=.(business_zone,fsn)]
setnames(instock_corrected_drr_p, old ='fsn',new = 'FSN')

selection_master_ds = gcs_get_object("ipc/Nav/IPC_Hyperlocal/mle_master.csv", bucket = gcs_bucket2)
#selection_master_ds<-read.csv("temp_file.csv")
setDT(selection_master_ds)
setnames(selection_master_ds,old=c("DS","date"),new=c("business_zone","Date"))
selection_master_ds[,`:=`(flag =1)]
selection_master_ds$Date<-as.Date(selection_master_ds$Date)

lifestyle_master_ds = gcs_get_object("ipc/Nav/IPC_Hyperlocal/lifestyle_master.csv", bucket = gcs_bucket2)
#lifestyle_master_ds<-read.csv("temp_file.csv")
setDT(lifestyle_master_ds)

setnames(lifestyle_master_ds, old='DS',new='business_zone')
lifestyle_master_ds[,`:=`(flag =1)]
lifestyle_master_ds$Date<-as.Date(NA)
#lifestyle_master_ds[,`:=`(cu=cumsum(flag)),by=.(FSN,business_zone)]
#lifestyle_master_ds = lifestyle_master_ds[cu==1]
#lifestyle_master_ds = lifestyle_master_ds[,`:=`(.is.na(Date),SAMs_Club ='No')]
lifestyle_master_ds = lifestyle_master_ds[,.(business_zone,FSN,City,BU,SC,Analytical_Vertical,brand,Title,NOOS,City_DRR,P1P2P3,Reporting,Buy_POC,Date,flag)]
#lifestyle_master_ds$Date<-as.Date(lifestyle_master_ds$Date)
selection_master_ds =rbind(selection_master_ds,lifestyle_master_ds)

###SH details 
sh_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/sourcing_hub.csv", bucket = gcs_bucket2)
#sh_master<-read.csv("temp_file.csv")
setDT(sh_master)
sh_master[,`:=`(facility_id = NULL)]
setnames(sh_master, old =c('code','city'),new=c('sh_code','City'))

selection_master_ds = sh_master[selection_master_ds, on =c('City')]
selection_master_ds[,`:=`(ds_city = City)]
selection_master_ds[,`:=`(City = ifelse(City == 'Pune','Mumbai',City))]
selection_master_ds[,`:=`(City = ifelse(City == 'Lucknow','Delhi',City))]

temp<-selection_master_ds
### add NL details

####NL - set 1 (main FCs)
nl_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/nl_hub.csv", bucket = gcs_bucket2)
#nl_master<-read.csv("temp_file.csv")
setDT(nl_master)
setnames(nl_master, old =c('code','city'),new=c('nl_code','City'))
selection_master_ds = nl_master[selection_master_ds, on =c('City')]

####NL - set 2 (Second FCs)
nl_master2 = gcs_get_object("ipc/Nav/IPC_Hyperlocal/nl_hub2.csv", bucket = gcs_bucket2)
#nl_master2<-read.csv("temp_file.csv")
setDT(nl_master2)
setnames(nl_master2, old =c('code','city'),new=c('nl_code2','City'))
selection_master_ds = nl_master2[selection_master_ds, on =c('City')]

####DS ATP
setDT(atp)
atp = atp[,.(atp = sum(atp)),by =.(fsn,fc)]
setnames(atp, old =c('fsn','fc'),new=c('FSN','business_zone'))

selection_master_ds = atp[selection_master_ds , on =c('FSN','business_zone')]
selection_master_ds = selection_master_ds[,`:=`(atp = ifelse(is.na(atp),0,atp))]

####Mapping SH to DS
ds_it = gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_IT.csv", bucket = gcs_bucket)
#ds_it<-read.csv("temp_file.csv")
setDT(ds_it)

ds_it = ds_it[Inwarding_Status %in% c('IN_TRANSIT','INWARDING','INITIATED')]
#ds_it = ds_it[Creation_Date >= (Sys.Date() - 5)]
ds_iwit = ds_it[,.(iwit = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]

ds_it[,`:=`(Creation_Date = as.POSIXct(Creation_Date, format="%Y-%m-%d %H:%M:%S"))]
ds_it[,`:=`(hours = as.numeric(Sys.time()-Creation_Date,units = "hours"))]
ds_it[,`:=`(lt_36 = ifelse(hours <37,1,0))]
ds_it[,`:=`(lt_48 = ifelse(hours <49,1,0))]

ds_iwit_lt_36 = ds_it[lt_36== 1,.(iwit_lt_36 = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]
ds_iwit_gt_36 = ds_it[lt_36== 0,.(iwit_gt_36 = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]

ds_iwit_lt_48 = ds_it[lt_48== 1,.(iwit_lt_48 = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]
ds_iwit_gt_48 = ds_it[lt_48== 0,.(iwit_gt_48 = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]

setnames(ds_iwit, old =c('fsn','Dest_FC','Src_FC'),new=c('FSN','business_zone','sh_code'))
selection_master_ds = ds_iwit[selection_master_ds, on =c('FSN','business_zone','sh_code')]

#Adding SH_to_DS_times
setnames(ds_iwit_lt_36, old =c('fsn','Dest_FC','Src_FC'),new=c('FSN','business_zone','sh_code'))
selection_master_ds = ds_iwit_lt_36[selection_master_ds, on =c('FSN','business_zone','sh_code')]
selection_master_ds[,`:=`(iwit_lt_36 = ifelse(is.na(iwit_lt_36),0,iwit_lt_36))]


setnames(ds_iwit_gt_36, old =c('fsn','Dest_FC','Src_FC'),new=c('FSN','business_zone','sh_code'))
selection_master_ds = ds_iwit_gt_36[selection_master_ds, on =c('FSN','business_zone','sh_code')]
selection_master_ds[,`:=`(iwit_gt_36 = ifelse(is.na(iwit_gt_36),0,iwit_gt_36))]


###Adding Patch for NL to DS
setnames(ds_iwit,old='sh_code',new='nl_code')
setnames(ds_iwit,old='iwit',new='nl_ds_iwit')
selection_master_ds = ds_iwit[selection_master_ds, on =c('FSN','business_zone','nl_code')]
selection_master_ds[,`:=`(iwit = ifelse(is.na(iwit),0,iwit))]
selection_master_ds[,`:=`(nl_ds_iwit = ifelse(is.na(nl_ds_iwit),0,nl_ds_iwit))]

#Adding NL_to_DS_time
setnames(ds_iwit_lt_48, old =c('fsn','Dest_FC','Src_FC'),new=c('FSN','business_zone','nl_code'))
selection_master_ds = ds_iwit_lt_48[selection_master_ds, on =c('FSN','business_zone','nl_code')]
selection_master_ds[,`:=`(iwit_lt_48 = ifelse(is.na(iwit_lt_48),0,iwit_lt_48))]


setnames(ds_iwit_gt_48, old =c('fsn','Dest_FC','Src_FC'),new=c('FSN','business_zone','nl_code'))
selection_master_ds = ds_iwit_gt_48[selection_master_ds, on =c('FSN','business_zone','nl_code')]
selection_master_ds[,`:=`(iwit_gt_48 = ifelse(is.na(nl_ds_iwit),0,nl_ds_iwit))]


####Adding SH Atp
setnames(atp, old ='atp',new ='sh_atp')
setnames(atp, old ='business_zone',new ='sh_code')

selection_master_ds = atp[selection_master_ds , on =c('FSN','sh_code')]
selection_master_ds[,`:=`(sh_atp = ifelse(is.na(sh_atp),0,sh_atp))]

####Adding NL Atp
setnames(atp, old ='sh_atp',new ='nl_atp')
setnames(atp, old ='sh_code',new ='nl_code')

selection_master_ds = atp[selection_master_ds , on =c('FSN','nl_code')]
selection_master_ds[,`:=`(nl_atp = ifelse(is.na(nl_atp),0,nl_atp))]

####Adding NL2 Atp
setnames(atp, old ='nl_atp',new ='nl2_atp')
setnames(atp, old ='nl_code',new ='nl_code2')

selection_master_ds = atp[selection_master_ds , on =c('FSN','nl_code2')]
selection_master_ds[,`:=`(nl2_atp = ifelse(is.na(nl2_atp),0,nl2_atp))]


####Adding DRR
selection_master_ds = instock_corrected_drr_p[selection_master_ds, on =c('FSN','business_zone')]
selection_master_ds[,`:=`(DRR = ifelse(is.na(DRR),0,DRR))]

####Adding DS Norms 
ds_norms = gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_Target_Master.csv")
#ds_norms<-read.csv("temp_file.csv")
setDT(ds_norms)
ds_norms = ds_norms[,.(ds_norms = mean(Target_Units)),by=.(FSN,DS)]
setnames(ds_norms, old = 'DS',new= 'business_zone')

selection_master_ds = ds_norms[selection_master_ds, on =c('FSN','business_zone')]
max_sale = selection_master_ds[,.(max_sale =max(DRR)),by=.(City,FSN)]
max_sale = max_sale[,`:=`(ds_norms_ns = ifelse(max_sale<0.2,1,ifelse(max_sale<1.1,2,ceiling(3*max_sale))))]

selection_master_ds = max_sale[selection_master_ds, on =c('City','FSN')]
new_store =store_master[Live ==0]
selection_master_ds = selection_master_ds[,`:=`(ds_norms = ifelse(business_zone %in% unique(new_store$business_zone),ds_norms_ns,ds_norms))] 

####creating instock

selection_master_ds = selection_master_ds[,`:=`(ds_deficit = ifelse(ds_norms - atp <0,0,ds_norms- atp))]
selection_master_ds [,`:=`(flag =1)]
selection_master_ds = selection_master_ds[,`:=`(sum_check = sum(flag)), by=.(FSN,sh_code)]
selection_master_ds = selection_master_ds[,`:=`(sh_atp_pivot = sh_atp/sum_check)]
selection_master_ds = selection_master_ds[,`:=`(nl_atp_pivot = nl_atp/sum_check)]
selection_master_ds = selection_master_ds[,`:=`(nl2_atp_pivot = nl2_atp/sum_check)]

selection_master_ds[,`:=`(doh_2 = round(DRR*2,0),doh_3 = round(DRR*3,0),doh_10 = round(DRR*10,0))]
selection_master_ds[,`:=`(apt_it = iwit+atp+nl_ds_iwit)]
selection_master_ds[,`:=`(instock_post_iwit = ifelse(apt_it - doh_3 >0,1,0))]
selection_master_ds[,`:=`(atp_post_iwit_initiation = round(sh_atp_pivot +nl_atp_pivot+ apt_it,0))]
selection_master_ds[,`:=`(instock_post_iwit_movement = ifelse(atp_post_iwit_initiation - doh_3 >0,1,0))]
selection_master_ds[,`:=`(ds_norms = ifelse(is.na(ds_norms),1,ds_norms))]
selection_master_ds[,`:=`(deficit = ifelse(ds_norms -apt_it<0,0,ds_norms - apt_it))]
selection_master_ds[,`:=`(stn_creation_sh = ifelse(sh_atp_pivot ==0 ,0,
                                                   ifelse(sh_atp_pivot >=deficit ,deficit,0)))]

selection_master_ds[,`:=`(nl_deficit = ifelse(ds_norms -apt_it -stn_creation_sh<=0,0,ds_norms - apt_it - stn_creation_sh))]
selection_master_ds[,`:=`(stn_creation_nl = ifelse(nl_atp_pivot ==0 ,0,
                                                   ifelse(nl_atp_pivot >=nl_deficit ,nl_deficit,0)))]



stn_creation_sh = selection_master_ds[stn_creation_sh >0,.(City,FSN,business_zone,stn_creation_sh,sh_atp_pivot)]
stn_creation_nl = selection_master_ds[stn_creation_nl >0,.(City,FSN,business_zone,stn_creation_nl,nl_atp_pivot)]

# selection_master_ds[,`:=`(total_deficit = sum(deficit)),by=.(FSN,City)]
# selection_master_ds[,`:=`(check_check = ifelse(stn_creation_sh =='check' & sh_atp >=total_deficit ,deficit,stn_creation_sh))]
# selection_master_ds[,`:=`(cu_2 = cumsum(flag)),by=.(FSN,City)]
# selection_master_ds[,`:=`(final_stn_sh = ifelse(check_check == "check" & sh_atp>=cu_2,pmin(deficit,sh_atp), 
#                                              ifelse(check_check =='check' & sh_atp<cu_2,0,check_check)))]
# 
# selection_master_ds[,`:=`(final_stn_nl = ifelse(check_check == "check" & nl_atp>=cu_2 & final_stn_sh == 0,deficit, 
#                                              ifelse(check_check =='check' & nl_atp<cu_2,0,check_check)))]

selection_master_ds[,`:=`(instock = ifelse(atp >0,1,0))]

##Adding Live to selection master
Live_flag<-unique(ds_master[,c("business_zone","Live")])
selection_master_ds<-merge(selection_master_ds,Live_flag,by="business_zone",all.x=T)

###Adding_buckets

selection_master_ds$buckets <- with(selection_master_ds, ifelse(
  instock == 1, 
  "instock", 
  ifelse(
    instock_post_iwit == 1, 
    "instock-iwit", 
    ifelse(
      nl_atp_pivot > 2 & stn_creation_sh == 0, 
      "Stock_in_NL", 
      ifelse(
        nl2_atp_pivot > 2 & stn_creation_sh == 0, 
        "Stock_in_NL2", 
        "OOS"
      )
    )
  )
))

selection_master_ds$buckets<-as.character(selection_master_ds$buckets)
setDT(selection_master_ds)
# selection_master_ds$Date <- as.Date(unlist(selection_master_ds$Date))

gcs_upload(selection_master_ds, name = "ipc/Nav/Hyperlocal_IPC/selection_master_ds_mle_ls.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")


# stn_creation = selection_master_ds[final_stn_sh >0 |final_stn_nl>0 ,.(City,FSN,business_zone,final_stn_sh,final_stn_nl,sh_atp_pivot,nl_atp_pivot)]
# stn_creation = stn_creation[,`:=`(final_stn =as.integer(final_stn))]
gcs_upload(stn_creation_sh, name = "ipc/Nav/Hyperlocal_IPC/mle_stn_creation_sh.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
gcs_upload(stn_creation_nl, name = "ipc/Nav/Hyperlocal_IPC/mle_stn_creation_nl.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
gcs_upload(selection_master_ds, name = "ipc/Nav/Hyperlocal_IPC/mle_ls_final_master.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

#####Reverse patch

# nl_to_sh = data.table(data.table(gcs_get_object("ipc/Nav/Anuj/IWIT_InTransit_FSN.csv", bucket = gcs_bucket)))
# reverse = nl_to_sh[Src_FC %in% unique(sh_master$sh_code)]
# 
# reverse = reverse[!Dest_FC %in% unique(ds_master$business_zone)]
# 
# nl_to_sh = nl_to_sh[fsn %in% unique(selection_master_ds$FSN)]
# nl_to_sh = nl_to_sh[Dest_FC %in% unique(sh_master$sh_code)]
# nl_to_sh = nl_to_sh[!Src_FC %in% unique(ds_master$business_zone)]
# gcs_upload(nl_to_sh, name = "ipc/Nav/Hyperlocal_IPC/mle_nl_to_sh.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# gcs_upload(reverse, name = "ipc/Nav/Hyperlocal_IPC/mle_reverse.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# 
# 
# nl_to_sh_p = nl_to_sh[,.(nl_to_sh = sum(IWIT_Intransit)), by =.(fsn,Dest_FC)]
# setnames(nl_to_sh_p, old =c('fsn','Dest_FC'),new=c('FSN','sh_code'))
# 
# selection_master_ds[,`:=`(comments = ifelse(instock ==1 ,'instock', ifelse(instock == 0 & sh_atp_pivot <=1,'sh_insufficiency',
#                                                                            ifelse(instock ==0 & iwit ==0,'iwit_not_created',
#                                                                                   ifelse(instock ==0 & iwit >=1,'iwit','')))))]
# 
# mle_p = selection_master_ds[,.(business_zone,FSN,City,DRR,sh_code,ds_norms,dispatch,pending_disptach,iwit_n_1,iwit,schedule_quantity, PO_qty,atp,apt_it,atp_post_iwit_initiation,instock,instock_post_iwit,instock_post_iwit_movement,BU,SC,Analytical_Vertical,brand,Title,flag,ds_deficit,sh_atp_pivot,comments,doh_2,doh_3,doh_10,final_stn)]
# mle_p = nl_to_sh_p[mle_p, on =c('FSN','sh_code')]
# 
# 
# ds_it = ds_it[fsn %in% unique(mle_p$FSN)]
# ds_it = ds_it[Src_FC %in% unique(sh_master$sh_code)]
# ds_it = ds_it[Dest_FC %in% unique(ds_master$business_zone)]
# ds_it[,`:=`(create_date = as.Date(Creation_Date))]
# 
# 
# gcs_upload(mle_p, name = "ipc/Nav/Hyperlocal_IPC/mle_p.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# 
# gcs_upload(mle_p, name = "ipc/Nav/Hyperlocal_IPC/mle_sh_to_ds.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

