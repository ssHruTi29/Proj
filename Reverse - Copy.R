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

#####Files to be changed everyday
#atp = data.table(fread("E:\\Vishruti\\Hyperlocal\\HL Final  inventory_snapshot_19 Nov 2024 04_36_58 .csv"))
atp = gcs_get_object("ipc/Nav/Hyperlocal_IPC/Inventory_Snapshot.csv", bucket = gcs_bucket)
setDT(atp)
fc_areas = data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/FC_Area_Split.csv", bucket = gcs_bucket2))
fc_areas = fc_areas[Consider =='Yes']
atp = atp[fc_area %in% unique(fc_areas$fc_area)]
gc()
###Reading facility mapper

facility_mapper = data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/Facility_Mapper_Dump.csv", bucket = gcs_bucket2))
facility_mapper_full = facility_mapper[,.SD]
facility_mapper = facility_mapper[,.(code,sub_type,city)]
setnames(facility_mapper, old ='code',new='business_zone')

###Reading store_city_master

#store_master = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1mIxIfmVOJ1whZO4bDKD51Wlcqud2l2rzuOgbFt4ges4/edit?gid=966141711#gid=966141711',sheet='stores'))
ds_master= data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2))
setnames(ds_master, old ='DS',new='business_zone')
#ds_master = ds_master[Live ==1]
store_master = ds_master[,.SD]

###Reading instock DRR

instock_corrected_drr= data.table(gcs_get_object("Avinash_HL_Data/Instock_CorrectedDRR_14Days.csv", bucket = gcs_bucket2))

###Joining DRR with facility_mapper
instock_corrected_drr = facility_mapper[instock_corrected_drr, on =c('business_zone')]

instock_corrected_drr = store_master[instock_corrected_drr, on =c('business_zone')]
missing_store = unique(instock_corrected_drr[is.na(City)])
missing_store = unique(missing_store[,.(business_zone)])
if (nrow(unique(missing_store))>0)
{
  print("missing stores")
}


instock_corrected_drr = instock_corrected_drr[,`:=`(city = NULL)]
instock_corrected_drr_p = instock_corrected_drr[,.(DRR = sum(DRR)),by=.(business_zone,fsn)]
setnames(instock_corrected_drr_p, old ='fsn',new = 'FSN')
instock_corrected_drr_sh = instock_corrected_drr[,.(DRR = sum(DRR)),by=.(City,fsn)]

####Adding patch for MLE Master

mle_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/mle_master.csv", bucket = gcs_bucket2)
setDT(mle_master)
mle_master = mle_master[,.(City,DS,FSN,BU,SC,Analytical_Vertical,brand,Title)]
setnames(mle_master, old='DS',new='business_zone')

ls_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/lifestyle_master.csv", bucket = gcs_bucket2)
setDT(ls_master)
ls_master = ls_master[,.(City,DS,FSN,BU,SC,Analytical_Vertical,brand,Title)]
setnames(ls_master, old='DS',new='business_zone')

mle_master = rbind(mle_master,ls_master)
####Store_master & Sh master
sh_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/sourcing_hub.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
sh_master<-read.csv("temp_file.csv")
setDT(sh_master)


sh_master[,`:=`(facility_id = NULL)]
setnames(sh_master, old =c('code','city'),new=c('sh_code','City'))

ds_master= data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2))
setnames(ds_master, old ='DS',new='business_zone')
ds_master[,`:=`(three = substr(business_zone,1,3))]
ds_master[,`:=`(City = ifelse(three =='pun','Pune',City))]
ds_master[,`:=`(three = NULL)]
ds_master = ds_master[Live ==1]
store_master = ds_master[,.SD]


####ATP Filter
prod_cat = data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/prod_cat.csv", bucket = gcs_bucket2))
setnames(prod_cat , old ='product_id',new='FSN')

atp_mle = atp[fc %in% unique(sh_master$sh_code) | fc %in% unique(store_master$business_zone)]
setnames(atp_mle , old ='fsn',new='FSN')

atp_mle = prod_cat[atp_mle, on =c('FSN')]

missing_fsn_details = atp_mle[is.na(analytic_super_category)]
missing_fsn_details = unique(missing_fsn_details[,.(FSN)])

gcs_upload(missing_fsn_details, name = "ipc/Nav/IPC_Hyperlocal/missing_fsn.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel",bucket = gcs_bucket2)

atp_mle = atp_mle[analytic_business_unit %in% c('CoreElectronics','EmergingElectronics','Mobile','LargeAppliances','LifeStyle')]

#atp_mle = atp_mle[fc_area =='store']
atp_mle_ds = atp_mle[fc %in% unique(store_master$business_zone)]
atp_mle_sh = atp_mle[fc %in% unique(sh_master$sh_code)]
setnames(atp_mle_ds, old='fc',new='business_zone')
setnames(atp_mle_sh, old='fc',new='business_zone')

atp_mle_ds_p = atp_mle_ds[,.(atp=sum(atp),qoh= sum(qoh)),by=.(FSN,business_zone,fc_area)]
atp_mle_ds_p[,`:=`(flag =1)]
atp_mle_ds_p[,`:=`(cu =sum(flag)),by=.(FSN,business_zone)]

atp_mle_sh_p = atp_mle_sh[,.(atp=sum(atp),qoh= sum(qoh)),by=.(FSN,business_zone,fc_area)]
atp_mle_sh_p[,`:=`(flag =1)]
atp_mle_sh_p[,`:=`(cu =sum(flag)),by=.(FSN,business_zone)]
###SH TO DS

ds_it= data.table(gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_IT.csv", bucket = gcs_bucket))
ds_it = ds_it[Inwarding_Status %in% c('IN_TRANSIT')]
ds_it = ds_it[,`:=`(IWIT_Intransit =as.numeric(IWIT_Intransit))]
ds_iwit = ds_it[,.(iwit = sum(IWIT_Intransit)), by =.(fsn,Dest_FC)]

setnames(ds_iwit, old =c('fsn','Dest_FC'),new=c('FSN','business_zone'))

atp_mle_ds_p = ds_iwit[atp_mle_ds_p, on =c('FSN','business_zone')]
atp_mle_ds_p[,`:=`(iwit = ifelse(is.na(iwit),0,iwit))]
atp_mle_ds_p[,`:=`(it_p = iwit/cu)]
###NLTOSH
nl_to_sh = data.table(data.table(gcs_get_object("ipc/Nav/Anuj/IWIT_InTransit_FSN.csv", bucket = gcs_bucket)))
nl_to_sh = nl_to_sh[Inwarding_Status =='IN_TRANSIT']

nl_to_sh_p = nl_to_sh[,.(nl_to_sh = sum(IWIT_Intransit)), by =.(fsn,Dest_FC)]

setnames(nl_to_sh_p, old =c('fsn','Dest_FC'),new=c('FSN','business_zone'))

atp_mle_sh_p =nl_to_sh_p[atp_mle_sh_p, on=c('FSN','business_zone')]
atp_mle_sh_p[,`:=`(nl_to_sh = ifelse(is.na(nl_to_sh),0,nl_to_sh))]
atp_mle_sh_p[,`:=`(it = nl_to_sh/cu)]

atp_mle_sh_p[,`:=`(total_units = it+atp)]
atp_mle_ds_p[,`:=`(total_units = it_p+atp)]

atp_mle_sh_p[,`:=`(total_units_qoh = it+qoh)]
atp_mle_ds_p[,`:=`(total_units_qoh = it_p+qoh)]

###Add NLC
#https://drive.google.com/drive/u/0/folders/1kwJbZG8v-dLfZV6PMUxGJRfagUko3fSg
nlc = data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/NLC.csv", bucket = gcs_bucket2))
nlc = unique(nlc[,.(fsn,NLC)])
setnames(nlc, old ='fsn',new='FSN')

atp_mle_sh_p = nlc[atp_mle_sh_p, on =c('FSN')]
atp_mle_ds_p = nlc[atp_mle_ds_p, on =c('FSN')]
atp_mle_ds_p[,`:=`(NLC =ifelse(is.na(NLC),0,NLC))]
atp_mle_sh_p[,`:=`(NLC =ifelse(is.na(NLC),0,NLC))]

atp_mle_ds_p[,`:=`(qoh_value = NLC*total_units_qoh,atp_value = NLC*total_units)]
atp_mle_sh_p[,`:=`(qoh_value = NLC*total_units_qoh,atp_value = NLC*total_units)]

atp_mle_sh_p = prod_cat[atp_mle_sh_p, on=c('FSN')]
atp_mle_ds_p = prod_cat[atp_mle_ds_p, on=c('FSN')]

gcs_upload(atp_mle_ds_p, name = "ipc/Nav/Hyperlocal_IPC/atp_mle_ds_p.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
gcs_upload(atp_mle_sh_p, name = "ipc/Nav/Hyperlocal_IPC/atp_mle_sh_p.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

atp_mle_ds_p = atp_mle_ds_p[fc_area =='store']
atp_mle_sh_p = atp_mle_sh_p[fc_area =='store']

atp_mle_ds_p = atp_mle_ds[,.(atp=sum(atp),qoh= sum(qoh)),by=.(FSN,business_zone)]
atp_mle_sh_p = atp_mle_sh[,.(atp=sum(atp),qoh= sum(qoh)),by=.(FSN,business_zone)]

###### Master DS
final_master_ds =mle_master[,.(business_zone,FSN)]
atp_ds_temp = atp_mle_ds[,.(business_zone,FSN)]

final_master_ds = rbind(atp_ds_temp,final_master_ds)
final_master_ds = unique(final_master_ds)

final_master_ds =atp_mle_ds_p[final_master_ds, on =c('FSN','business_zone')]
final_master_ds[,`:=`(atp = ifelse(is.na(atp),0,atp),qoh =ifelse(is.na(qoh),0,qoh))]
mle_master[,`:=`(master =1)]

final_master_ds = mle_master[final_master_ds, on =c('FSN','business_zone')]

final_master_ds_1 = final_master_ds[is.na(BU)]
final_master_ds_2 = final_master_ds[!is.na(BU)]
setnames(prod_cat, old =c('analytic_business_unit','analytic_sub_category','analytic_vertical','title'),new=c('BU','SC','Analytical_Vertical','Title'))
final_master_ds_1 = final_master_ds_1[,`:=`(BU = NULL,SC=NULL,Analytical_Vertical = NULL,brand = NULL,Title = NULL)]
prod_cat = prod_cat[,.(FSN,BU,SC,Analytical_Vertical,brand,Title)]
final_master_ds_1 = prod_cat[final_master_ds_1, on =c('FSN')]
final_master_ds_1 = store_master[final_master_ds_1, on =c('business_zone')]
final_master_ds_1[,`:=`(master =0)]
final_master_ds_1 = final_master_ds_1[,.(City,business_zone,FSN,BU,SC,Analytical_Vertical,brand,Title,master,atp,qoh)]

final_ds = rbind(final_master_ds_1,final_master_ds_2)
final_ds = unique(final_ds)
####SH master

final_master_sh =mle_master[,.(City,FSN)]
final_master_sh =sh_master[final_master_sh, on =c('City')]
final_master_sh =final_master_sh[,.(sh_code,FSN)]
setnames(final_master_sh, old='sh_code',new='business_zone')
atp_sh_temp = atp_mle_sh[,.(business_zone,FSN)]

final_master_sh = rbind(atp_sh_temp,final_master_sh)
final_master_sh = unique(final_master_sh)

final_master_sh =atp_mle_sh_p[final_master_sh, on =c('FSN','business_zone')]
final_master_sh[,`:=`(atp = ifelse(is.na(atp),0,atp),qoh =ifelse(is.na(qoh),0,qoh))]

mle_master = sh_master[mle_master, on =c('City')]
mle_master_sh =mle_master[,`:=`(business_zone= NULL)]
setnames(mle_master_sh, old ='sh_code',new='business_zone')
final_master_sh = mle_master_sh[final_master_sh, on =c('FSN','business_zone')]

final_master_sh_1 = final_master_sh[is.na(BU)]
final_master_sh_2 = final_master_sh[!is.na(BU)]
#setnames(prod_cat, old =c('analytic_business_unit','analytic_sub_category','analytic_vertical','title'),new=c('BU','SC','Analytical_Vertical','Title'))
final_master_sh_1 = final_master_sh_1[,`:=`(BU = NULL,SC=NULL,Analytical_Vertical = NULL,brand = NULL,Title = NULL)]
prod_cat = prod_cat[,.(FSN,BU,SC,Analytical_Vertical,brand,Title)]
final_master_sh_1 = prod_cat[final_master_sh_1, on =c('FSN')]
setnames(sh_master, old='sh_code',new='business_zone')
sh_master[,`:=`(flag =1)]
sh_master[,`:=`(cu = cumsum(flag)),by=.(business_zone)]
sh_master =sh_master[cu==1]
final_master_sh_1 = sh_master[final_master_sh_1, on =c('business_zone')]
final_master_sh_1[,`:=`(master =0)]
final_master_sh_1 = final_master_sh_1[,.(City,business_zone,FSN,BU,SC,Analytical_Vertical,brand,Title,master,atp,qoh)]

final_sh = rbind(final_master_sh_1,final_master_sh_2)
final_sh = unique(final_sh)

###Add DRR 
final_ds = instock_corrected_drr_p[final_ds, on =c('FSN','business_zone')]
final_ds[,`:=`(DRR = ifelse(is.na(DRR),0,DRR))]


instock_corrected_drr_sh = sh_master[instock_corrected_drr_sh, on =c('City')]
instock_corrected_drr_sh[,`:=`(City = NULL)]
setnames(instock_corrected_drr_sh, old='fsn',new='FSN')
final_sh = instock_corrected_drr_sh[final_sh, on =c('FSN','business_zone')]
final_sh[,`:=`(DRR = ifelse(is.na(DRR),0,DRR))]

###Add NLC


final_ds = nlc[final_ds, on =c('FSN')]
final_sh = nlc[final_sh, on =c('FSN')]

final_ds = final_ds[,`:=`(NLC = ifelse(is.na(NLC),0,NLC))]
final_ds = final_ds[,`:=`(inv_value_atp = NLC *atp,qoh_value_atp = NLC * qoh)]

final_sh = final_sh[,`:=`(NLC = ifelse(is.na(NLC),0,NLC))]
final_sh = final_sh[,`:=`(inv_value_atp = NLC *atp,qoh_value_atp = NLC * qoh)]

####Corrected_DRR
final_ds = final_ds[,`:=`(fwd_drr = 2*DRR)]
final_sh = final_sh[,`:=`(fwd_drr = 2*DRR)]

#####norm cal

final_ds = final_ds[,`:=`(ds_norm = ifelse(fwd_drr<0.2,1,ifelse(fwd_drr<0.6,2,ceiling(fwd_drr*3))))]
final_sh = final_sh[,`:=`(sh_norm = ifelse(ceiling(fwd_drr*10) < 11,10,fwd_drr*10))]

final_sh = final_sh[,`:=`(sh_norm = ifelse(BU == 'CoreElectronics' & sh_norm == 10,7,sh_norm))]

####return_Cal
final_ds = final_ds[,`:=`(reverse_qty =ifelse(master ==0,atp,ifelse(atp -(ds_norm * 3)<=0,0,atp - (ds_norm*3))))]
final_ds = final_ds[,`:=`(comments = ifelse(master ==0,"OOM",ifelse(reverse_qty>0,"Excess","")))]


final_sh = final_sh[,`:=`(reverse_qty =ifelse(master ==0,atp,ifelse(atp -(sh_norm * 3)<=0,0,atp - (sh_norm*3))))]
final_sh = final_sh[,`:=`(comments = ifelse(master ==0,"OOM",ifelse(reverse_qty>0,"Excess","")))]

coin_fsns=data.frame(fsn=c('CONH52YFAHB52QMG','CONH52YFGTQUEZHQ','CONH52YFHW5KXSUK','CONH4WH6GZZN7QRH','CONH4WH6N5NBQSBY','CONH4WH6Z9N6ZWUF','CONH4WH6ZFGMH9G6','CONH4WH6SMUDAJPU','CONH4WH68JMZFWYG','CONH4WH6CB5DTY2M','CONH4WH6GGPHCY6B','CONH4WH68GKADXZR','CONH4WH6MHPKUEYZ','CONH57FDUQMY7WHC','CONH57FDZZE3ZYEV','CONH5ZYPDN2TSEF9','CONH5ZYPF6EJYFTX','CONH5ZYPGRTPCH4Q','CONH5ZYPKBHYRSPC','CONH4XCNZHFHSZPH','CONH4XCNCWMPGYG6','CONH4XCNFKG9QHFP','CONH4XCNUZTHCXPY','CONH4XCNZHZ4YD9F','CONH4XCNHJZD5YGY'))
final_ds=final_ds[!final_ds$FSN %in% coin_fsns$fsn,]
final_ds <- final_ds %>% 
  filter(!Analytical_Vertical %in% c("GoldCoinsBar","SilverCoinsBar"))

final_sh=final_sh[!final_sh$FSN %in% coin_fsns$fsn,]
final_sh <- final_sh %>% 
  filter(!Analytical_Vertical %in% c("GoldCoinsBar","SilverCoinsBar"))
final_sh <- final_sh %>% 
  filter(!Analytical_Vertical %in% c('Allopathy','RxMedicine','MedicalEquipmentAndAccessories','PregnancyKit','FertilityKit','HealthCareApplianceCombo','GlucometerStrips','HearingAid','RespiratoryExerciser','DigitalThermometer','BloodPressureMonitor','GlucometerLancet','Glucometer','Nebulizer','ContactLenses','MedicineTreatment','HealthCareDevice','PulseOximeter','HealthCareAccessory','Sterilisation&InfectionPrevention','HealthTestKit','LensSolution','Vaporizer','HeatingPad'))

gcs_upload(final_ds, name = "ipc/Nav/Hyperlocal_IPC/value_ds_2.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
gcs_upload(final_sh, name = "ipc/Nav/Hyperlocal_IPC/value_sh_2.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

gc()