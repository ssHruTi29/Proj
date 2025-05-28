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

##### Reading inventory and ds_master
atp = gcs_get_object("ipc/Nav/Hyperlocal_IPC/Inventory_Snapshot.csv", bucket = gcs_bucket)
setDT(atp)
atp = atp[fc_area == 'store']
Inv = atp[,.SD]

ds_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
setDT(ds_master)

setnames(ds_master, old ='DS',new='business_zone')
ds_master[,`:=`(three = substr(business_zone,1,3))]
ds_master[,`:=`(City = ifelse(three =='pun','Pune',City))]
ds_master[,`:=`(three = NULL)]
store_master = ds_master[,.SD]

facility_mapper = gcs_get_object("ipc/Nav/IPC_Hyperlocal/Facility_Mapper_Dump.csv",bucket = gcs_bucket2)
setDT(facility_mapper)

facility_mapper_full = facility_mapper[,.SD]
facility_mapper = facility_mapper[,.(code,sub_type,city)]
setnames(facility_mapper, old ='code',new='business_zone')

###Reading instock DRR
instock_corrected_drr = gcs_get_object("Avinash_HL_Data/Instock_CorrectedDRR_14Days.csv", bucket = gcs_bucket2)
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
setDT(selection_master_ds)
setnames(selection_master_ds,old=c("DS","date"),new=c("business_zone","Date"))
selection_master_ds[,`:=`(flag =1)]
selection_master_ds$Date<-as.Date(selection_master_ds$Date)

lifestyle_master_ds = gcs_get_object("ipc/Nav/IPC_Hyperlocal/lifestyle_master.csv", bucket = gcs_bucket2)
setDT(lifestyle_master_ds)

lifestyle_master_ds[,`:=`(flag =1)]
setnames(lifestyle_master_ds,old=c("DS","date"),new=c("business_zone","Date"))
lifestyle_master_ds$Date<-as.Date(lifestyle_master_ds$Date)
lifestyle_master_ds = lifestyle_master_ds[,.(business_zone,FSN,City,BU,SC,Analytical_Vertical,brand,Title,NOOS,City_DRR,P1P2P3,Reporting,Buy_POC,Date,flag)]
lifestyle_master_ds$Date<-as.Date(lifestyle_master_ds$Date)

##Combining mle_master and LS master
selection_master_ds =rbind(selection_master_ds,lifestyle_master_ds)

### SH details 
sh_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/sourcing_hub.csv", bucket = gcs_bucket2)
setDT(sh_master)
sh_master[,`:=`(facility_id = NULL)]
setnames(sh_master, old =c('code','city'),new=c('sh_code','City'))
sh_master$sh_code=ifelse(sh_master$City=="Guwahati","ulu_sh_wh_nl_01nl",sh_master$sh_code)

##Adding SH codes to selection master(mle+Ls)
selection_master_ds = sh_master[selection_master_ds, on =c('City')]
selection_master_ds[,`:=`(ds_city = City)]


### NL details 
####NL - set 1 (main FCs)
nl_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/nl_hub.csv", bucket = gcs_bucket2)
setDT(nl_master)
setnames(nl_master, old =c('code','city'),new=c('nl_code','City'))
selection_master_ds = nl_master[selection_master_ds, on =c('City')]

####NL - set 2 (Second FCs)
nl_master2 = gcs_get_object("ipc/Nav/IPC_Hyperlocal/nl_hub2.csv", bucket = gcs_bucket2)
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
setDT(ds_it)
ds_it[, IWIT_Intransit := as.numeric(IWIT_Intransit)]
setDT(ds_it)

ds_it = ds_it[Inwarding_Status %in% c('IN_TRANSIT','INWARDING','INITIATED')]
ds_it[,`:=`(Creation_Date = as.POSIXct(Creation_Date, format="%Y-%m-%d %H:%M:%S"))]
ds_it[,`:=`(days = as.numeric(Sys.time()-Creation_Date,units = "days"))]
ds_it = ds_it[days < 7]

ds_iwit = ds_it[,.(iwit = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]

ds_it[,`:=`(Creation_Date = as.POSIXct(Creation_Date, format="%Y-%m-%d %H:%M:%S"))]
ds_it[,`:=`(hours = as.numeric(Sys.time()-Creation_Date,units = "hours"))]
ds_it[,`:=`(lt_36 = ifelse(hours <37,1,0))]
ds_it[,`:=`(lt_48 = ifelse(hours <49,1,0))]

ds_iwit_lt_36 = ds_it[lt_36== 1,.(iwit_lt_36 = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]
ds_iwit_gt_36 = ds_it[lt_36== 0,.(iwit_gt_36 = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]

ds_iwit_lt_48 = ds_it[lt_48== 1,.(iwit_lt_48 = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]
ds_iwit_gt_48 = ds_it[lt_48== 0,.(iwit_gt_48 = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]

###SH to DS
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


###NL1 to SH IWIT
nl1_to_sh = gcs_get_object("ipc/Nav/Hyperlocal_IPC/nl_to_sh.csv", bucket = gcs_bucket)
setDT(nl1_to_sh)
nl1_to_sh = nl1_to_sh[Inwarding_Status %in% c('IN_TRANSIT','INWARDING','INITIATED')]
nl1_to_sh_iwit = nl1_to_sh[,.(nl1_to_sh_iwit = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]
setnames(nl1_to_sh_iwit,old=c("fsn","Dest_FC","Src_FC"),new=c("FSN","sh_code","nl_code"))
selection_master_ds = nl1_to_sh_iwit[selection_master_ds, on =c('FSN','sh_code','nl_code')]
selection_master_ds[,`:=`(nl1_to_sh_iwit = ifelse(is.na(nl1_to_sh_iwit),0,nl1_to_sh_iwit))]


###NL2 to SH IWIT
nl2_to_sh = gcs_get_object("ipc/Nav/Hyperlocal_IPC/nl_to_sh.csv", bucket = gcs_bucket)
setDT(nl2_to_sh)
nl2_to_sh = nl2_to_sh[Inwarding_Status %in% c('IN_TRANSIT','INWARDING','INITIATED')]
nl2_to_sh_iwit = nl2_to_sh[,.(nl2_to_sh_iwit = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]
setnames(nl2_to_sh_iwit,old=c("fsn","Dest_FC","Src_FC"),new=c("FSN","sh_code","nl_code2"))
selection_master_ds = nl2_to_sh_iwit[selection_master_ds, on =c('FSN','sh_code','nl_code2')]
selection_master_ds[,`:=`(nl2_to_sh_iwit = ifelse(is.na(nl2_to_sh_iwit),0,nl2_to_sh_iwit))]


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

selection_master_ds$nl1_to_sh_iwit<-selection_master_ds$nl1_to_sh_iwit/selection_master_ds$sum_check
selection_master_ds$nl2_to_sh_iwit<-selection_master_ds$nl2_to_sh_iwit/selection_master_ds$sum_check

selection_master_ds = selection_master_ds[,`:=`(sh_atp_pivot = sh_atp/sum_check)]
selection_master_ds = selection_master_ds[,`:=`(nl_atp_pivot = nl_atp/sum_check)]
selection_master_ds = selection_master_ds[,`:=`(nl2_atp_pivot = nl2_atp/sum_check)]


selection_master_ds[,`:=`(doh_2 = round(DRR*2,0),doh_3 = round(DRR*3,0),doh_10 = round(DRR*10,0))]
selection_master_ds[,`:=`(apt_it = iwit+atp+nl_ds_iwit)]
selection_master_ds[,`:=`(instock_post_iwit = ifelse(apt_it - doh_3 >0,1,0))]
selection_master_ds[,`:=`(atp_post_iwit_initiation = round(sh_atp_pivot +nl_atp_pivot+ apt_it,0))]
selection_master_ds[,`:=`(instock_post_iwit_movement = ifelse(atp_post_iwit_initiation - doh_3 >0,1,0))]
#selection_master_ds[,`:=`(instock_post_iwit_movement = ifelse(atp_post_iwit_initiation - doh_3 >0,1,0))]

selection_master_ds[,`:=`(ds_norms = ifelse(is.na(ds_norms),1,ds_norms))]
selection_master_ds[,`:=`(deficit = ifelse(ds_norms -apt_it<0,0,ds_norms - apt_it))]
selection_master_ds[, `:=`(stn_creation_sh = ifelse(sh_atp_pivot == 0, 0,
                                                    ifelse(sh_atp_pivot >= deficit, deficit, pmax(sh_atp_pivot, 0))))]

selection_master_ds[,`:=`(nl_deficit = ifelse(ds_norms -apt_it -stn_creation_sh<=0,0,ds_norms - apt_it - stn_creation_sh))]
selection_master_ds[(City =='Kolkata'|City=='Guwahati'),`:=`(kol_deficit = ifelse(ds_norms*10/4 -apt_it-nl1_to_sh_iwit-nl2_to_sh_iwit -stn_creation_sh<=0,0,ds_norms*10/4 - apt_it-nl1_to_sh_iwit-nl2_to_sh_iwit - stn_creation_sh))]
selection_master_ds[City =='Delhi',`:=`(delhi_deficit = ifelse(ds_norms*10/4 -apt_it -stn_creation_sh<=0,0,ds_norms*10/4 - apt_it - stn_creation_sh))]

selection_master_ds[,`:=`(stn_creation_nl = ifelse(nl_atp_pivot ==0 ,0,
                                                   ifelse(nl_atp_pivot >=nl_deficit ,nl_deficit,0)))]

selection_master_ds[,`:=`(kol_stn_creation_nl = ifelse(nl_atp_pivot ==0 ,0,
                                                       ifelse(nl_atp_pivot >=kol_deficit ,kol_deficit,0)))]

selection_master_ds[,`:=`(kol_stn_creation_nl2 = ifelse(nl2_atp_pivot ==0 ,0,
                                                        ifelse(nl2_atp_pivot >=kol_deficit ,kol_deficit,0)))]

selection_master_ds[,`:=`(delhi_stn_creation_nl2 = ifelse(nl2_atp_pivot ==0 ,0,
                                                          ifelse(nl2_atp_pivot >=delhi_deficit ,delhi_deficit,0)))]

stn_creation_sh = selection_master_ds[stn_creation_sh >0,.(City,FSN,business_zone,stn_creation_sh,sh_atp_pivot)]
stn_creation_nl = selection_master_ds[stn_creation_nl >0,.(City,FSN,business_zone,stn_creation_nl,nl_atp_pivot)]
kol_stn_creation_nl = selection_master_ds[kol_stn_creation_nl >0,.(City,FSN,business_zone,kol_stn_creation_nl,nl_atp_pivot)]
# selection_master_ds[,`:=`(total_deficit = sum(deficit)),by=.(FSN,City)]
# selection_master_ds[,`:=`(check_check = ifelse(stn_creation_sh =='check' & sh_atp >=total_deficit ,deficit,stn_creation_sh))]
# selection_master_ds[,`:=`(cu_2 = cumsum(flag)),by=.(FSN,City)]
# selection_master_ds[,`:=`(final_stn_sh = ifelse(check_check == "check" & sh_atp>=cu_2,pmin(deficit,sh_atp), 
#                                              ifelse(check_check =='check' & sh_atp<cu_2,0,check_check)))]
# 
# selection_master_ds[,`:=`(final_stn_nl = ifelse(check_check == "check" & nl_atp>=cu_2 & final_stn_sh == 0,deficit, 
#                                              ifelse(check_check =='check' & nl_atp<cu_2,0,check_check)))]

selection_master_ds[,`:=`(instock = ifelse(atp >0,1,0))]
selection_master_ds[,`:=`(instock_sale = ifelse(atp>=ds_norms,1,0))]


##Adding Live to selection master
Live_flag<-unique(ds_master[,c("business_zone","Live")])
selection_master_ds<-merge(selection_master_ds,Live_flag,by="business_zone",all.x=T)


stn_creation_NL1_kol = selection_master_ds[kol_stn_creation_nl >0 & Live==1,.(City,FSN,nl_code,sh_code,BU,kol_stn_creation_nl)]
stn_creation_NL2_kol = selection_master_ds[kol_stn_creation_nl2 >0,.(City,FSN,nl_code2,sh_code,BU,kol_stn_creation_nl2)]

selection_master_ds$kol_stn_creation_nl<-ifelse(sum(selection_master_ds$kol_stn_creation_nl,na.rm=T)>7000,selection_master_ds$kol_stn_creation_nl*7000/sum(selection_master_ds$kol_stn_creation_nl,na.rm=T),selection_master_ds$kol_stn_creation_nl)

seller = gcs_get_object("ipc/Nav/Hyperlocal_IPC/seller_norm_0.csv", bucket = gcs_bucket)
setDT(seller)

seller=seller[,c("fsn")]
seller$S_Falg=1
seller=unique(seller)
selection_master_ds=merge(selection_master_ds,seller,by.x=c("FSN"),by.y=c("fsn"),all.x=T)
selection_master_ds$S_Falg=ifelse(is.na(selection_master_ds$S_Falg),0,selection_master_ds$S_Falg)

###Adding_buckets

selection_master_ds$buckets <- with(selection_master_ds, ifelse(
  instock_sale == 1, 
  "instock", 
  ifelse(
    instock_post_iwit == 1, 
    "instock-iwit",
    ifelse( S_Falg == 1, "OOS_due_to_seller_issue",
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
  )))
selection_master_ds$buckets<-as.character(selection_master_ds$buckets)
setDT(selection_master_ds)
# selection_master_ds$Date <- as.Date(unlist(selection_master_ds$Date))
selection_master_ds<-unique(selection_master_ds)
#new_store<-data.frame(business_zone=c('ben_118_wh_hl_01','kol_122_wh_hl_01','luc_114_wh_hl_01','pun_108_wh_hl_01','pun_119_wh_hl_01','guw_106_wh_hl_01','ahm_109_wh_hl_01','mum_116_wh_hl_01','ben_108_wh_hl_01','luc_105_wh_hl_01'))
#new_store$new_store<-new_store$business_zone
#selection_master_ds<-merge(selection_master_ds,new_store,by=c("business_zone"),all.x=T)



################ Store IB input check#####################
# # Create a string of 31 'c's to treat all columns as character
# col_types_str <- paste(rep("c", 31), collapse = "")
# 
# # Read sheet with all columns as character
# store_IB_input <- data.table(
#   read_sheet(
#     'https://docs.google.com/spreadsheets/d/1NnGVNgIhpbS_BBXV7F9Iz2axWWEWtayHrKe7y63ZAkc/edit?gid=0',
#     sheet = 'Dark stores IB dates',
#     col_types = col_types_str
#   )
# )


# store_IB_input=store_IB_input[,c("Warehouse Code","IB start date")]
# store_IB_input$`IB start date` <- dmy(paste0(store_IB_input$`IB start date`, "-2024")) 
# store_IB_input=store_IB_input[!is.na(store_IB_input$`Warehouse Code`),]
# store_IB_input_act=store_IB_input[!is.na(store_IB_input$`IB start date`),]
# store_IB_input_act=store_IB_input[store_IB_input$`IB start date`<=today()+5,]

#####Kolkata stns
##kolkata NL1toSH
#new_store_STNs = selection_master_ds[(selection_master_ds$Live==0 & (selection_master_ds$City !="Kolkata"|selection_master_ds$City !="Guwahati")& (selection_master_ds$BU !="Large")),]
new_store_STNs = selection_master_ds[
  selection_master_ds$Live == 0 & 
    selection_master_ds$City != "Kolkata" & 
    selection_master_ds$City != "Guwahati" & 
    selection_master_ds$BU != "Large", 
]

#new_store_STNs=new_store_STNs[new_store_STNs$business_zone %in% store_IB_input_act$`Warehouse Code`,]
new_store_STNs_final=new_store_STNs[,c("FSN","business_zone","nl_code","stn_creation_nl")]
setnames(new_store_STNs_final, 
         old = c("FSN", "business_zone", "nl_code", "stn_creation_nl"), 
         new = c("FSN", "Dest_code", "Src_Code", "Units"))
new_store_STNs_final$Units=ceiling(new_store_STNs_final$Units)
final_stn_file <- new_store_STNs_final[0]

# Bind the new STNs
final_stn_file <- rbind(final_stn_file, new_store_STNs_final)

## bind the kol stns
new_store_STNs_kol = selection_master_ds[(selection_master_ds$Live==0 & (selection_master_ds$City =="Kolkata"| selection_master_ds$City =="Guwahati")&(selection_master_ds$BU !="Large")),]

#new_store_STNs=new_store_STNs[,c("business_zone","City","FSN","nl_code","stn_creation_nl","BU")]
new_store_STNs_kol_sh=new_store_STNs_kol[,c("sh_code","City","FSN","business_zone","stn_creation_sh","BU")]
new_store_STNs_kol_sh$sh_code="ulu_sh_wh_nl_01nl"
new_store_STNs_kol_nl=new_store_STNs_kol[,c("sh_code","City","FSN","nl_code","stn_creation_nl","BU")]
new_store_STNs_kol_nl$sh_code="ulu_sh_wh_nl_01nl"

#####Adding kolkata to final file
##SH to New stores
new_store_STNs_kol_sh_final=new_store_STNs_kol_sh[,c("FSN","business_zone","sh_code","stn_creation_sh")]
setnames(new_store_STNs_kol_sh_final, 
         old = c("FSN", "business_zone", "sh_code", "stn_creation_sh"), 
         new = c("FSN", "Dest_code", "Src_Code", "Units"))
new_store_STNs_kol_sh_final$Units=ceiling(new_store_STNs_kol_sh_final$Units)
final_stn_file <- rbind(final_stn_file, new_store_STNs_kol_sh_final)

##NL to SH

new_store_STNs_kol_nl_final=new_store_STNs_kol_nl[,c("FSN","sh_code","nl_code","stn_creation_nl")]
setnames(new_store_STNs_kol_nl_final, 
         old = c("FSN", "sh_code", "nl_code", "stn_creation_nl"), 
         new = c("FSN", "Dest_code", "Src_Code", "Units"))
new_store_STNs_kol_nl_final$Units=ceiling(new_store_STNs_kol_nl_final$Units)
final_stn_file <- rbind(final_stn_file, new_store_STNs_kol_nl_final)

###NL1_to_DS_Manual
#NL_to_DS_STNs=selection_master_ds[(selection_master_ds$Live==1 & selection_master_ds$City !="Kolkata"& selection_master_ds$City !="Guwahati"& selection_master_ds$BU !="Large" & selection_master_ds$buckets =="Stock_in_NL"),]
NL_to_DS_STNs=selection_master_ds[(selection_master_ds$Live==1 & selection_master_ds$City !="Kolkata"& selection_master_ds$City !="Guwahati"& selection_master_ds$BU !="Large"),]
NL_to_DS_STNs=NL_to_DS_STNs[,c("FSN","business_zone","City","BU","nl_code","stn_creation_nl")]

NL_to_DS_STNs_final=NL_to_DS_STNs[,c("FSN","business_zone","nl_code","stn_creation_nl")]
setnames(NL_to_DS_STNs_final, 
         old = c("FSN", "business_zone", "nl_code", "stn_creation_nl"), 
         new = c("FSN", "Dest_code", "Src_Code", "Units"))
NL_to_DS_STNs_final$Units=ceiling(NL_to_DS_STNs_final$Units)
final_stn_file <- rbind(final_stn_file, NL_to_DS_STNs_final)


###NL2_to_binola
NL2_to_Binola_STNs=selection_master_ds[(selection_master_ds$Live==1 & selection_master_ds$City =="Delhi"& selection_master_ds$BU =="LargeAppliances" & selection_master_ds$buckets =="Stock_in_NL2"),]
NL2_to_Binola_STNs=NL2_to_Binola_STNs[,c("FSN","business_zone","City","BU","nl_code2","delhi_stn_creation_nl2")]
# NL2_to_Binola_STNs$delhi_stn_creation_nl2 <- pmin(
#   NL2_to_Binola_STNs$delhi_stn_creation_nl2, 
#   NL2_to_Binola_STNs$delhi_stn_creation_nl2 * (1000 / sum(NL2_to_Binola_STNs$delhi_stn_creation_nl2))
#)

NL2_to_Binola_STNs_final=NL2_to_Binola_STNs[,c("FSN","business_zone","nl_code2","delhi_stn_creation_nl2")]
setnames(NL2_to_Binola_STNs_final, 
         old = c("FSN", "business_zone", "nl_code2", "delhi_stn_creation_nl2"), 
         new = c("FSN", "Dest_code", "Src_Code", "Units"))
NL2_to_Binola_STNs_final$Units=ceiling(NL2_to_Binola_STNs_final$Units)
final_stn_file <- rbind(final_stn_file, NL2_to_Binola_STNs_final)

########## sh to ds

stn_creation_sh=merge(stn_creation_sh,sh_master,by="City",all.x=T)
stn_creation_sh_final=stn_creation_sh[,c("FSN","business_zone","sh_code","stn_creation_sh")]
setnames(stn_creation_sh_final, 
         old = c("FSN","business_zone","sh_code","stn_creation_sh"), 
         new = c("FSN", "Dest_code", "Src_Code", "Units"))
stn_creation_sh_final$Units=ceiling(stn_creation_sh_final$Units)
final_stn_file <- rbind(final_stn_file, stn_creation_sh_final)

######## nl to sh kolkata

stn_creation_NL1_kol_final=stn_creation_NL1_kol[,c("FSN","sh_code","nl_code","kol_stn_creation_nl")]
setnames(stn_creation_NL1_kol_final, 
         old = c("FSN","sh_code","nl_code","kol_stn_creation_nl"), 
         new = c("FSN", "Dest_code", "Src_Code", "Units"))
stn_creation_NL1_kol_final$Units=ceiling(stn_creation_NL1_kol_final$Units)
final_stn_file <- rbind(final_stn_file, stn_creation_NL1_kol_final)

stn_creation_NL2_kol_final=stn_creation_NL2_kol[,c("FSN","sh_code","nl_code2","kol_stn_creation_nl2")]
setnames(stn_creation_NL2_kol_final, 
         old = c("FSN","sh_code","nl_code2","kol_stn_creation_nl2"), 
         new = c("FSN", "Dest_code", "Src_Code", "Units"))
stn_creation_NL2_kol_final$Units=ceiling(stn_creation_NL2_kol_final$Units)
final_stn_file <- rbind(final_stn_file, stn_creation_NL2_kol_final)

########### adding Sale Flag for May BSD ##################
BSD_FSNs = gcs_get_object("ipc/Nav/IPC_Hyperlocal/BSD Relevant FSNs.csv", bucket = gcs_bucket2)
setDT(BSD_FSNs)
selection_master_ds[, BSD_Flag := ifelse(
  BU %in% c("CoreElectronics", "EmergingElectronics") | FSN %in% BSD_FSNs$FSN,
  1,
  0
)]

########### Consolidate the results ##################

final_stn_file=unique(final_stn_file)
# gcs_upload(final_stn_file, name = "ipc/Nav/Hyperlocal_IPC/total_manual_stn.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
gcs_upload(selection_master_ds, name = "ipc/Nav/Hyperlocal_IPC/selection_master_ds_mle_ls_sale.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# gcs_upload(stn_creation_sh, name = "ipc/Nav/Hyperlocal_IPC/mle_stn_creation_sh.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# #gcs_upload(stn_creation_nl, name = "ipc/Nav/Hyperlocal_IPC/mle_stn_creation_nl.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# gcs_upload(stn_creation_NL1_kol, name = "ipc/Nav/Hyperlocal_IPC/mle_stn_creation_NL1_kol.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# gcs_upload(stn_creation_NL2_kol, name = "ipc/Nav/Hyperlocal_IPC/mle_stn_creation_NL2_kol.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# gcs_upload(NL_to_DS_STNs, name = "ipc/Nav/Hyperlocal_IPC/NL_to_DS_STNs.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# gcs_upload(NL2_to_Binola_STNs, name = "ipc/Nav/Hyperlocal_IPC/NL2_to_Binola_STNs.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# 
# 
# gcs_upload(new_store_STNs, name = "ipc/Nav/Hyperlocal_IPC/new_store_STNs.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# gcs_upload(new_store_STNs_kol_sh, name = "ipc/Nav/Hyperlocal_IPC/new_store_STNs_kol_sh.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# gcs_upload(new_store_STNs_kol_nl, name = "ipc/Nav/Hyperlocal_IPC/new_store_STNs_kol_nl.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# 
# 
# #reporting = selection_master_ds[,.(business_zone,City,FSN,ds_norms,DRR,nl_code2,nl_code,sh_code,iwit,atp,BU,SC,Analytical_Vertical,brand,flag,sum_check,sh_atp_pivot,nl2_atp_pivot,apt_it,instock_post_iwit,deficit,instock,Live,buckets)]         
# 
# #write_sheet(reporting,'https://docs.google.com/spreadsheets/d/1ehTJqcgp5Y3CVvMYIqheSZrCFLMAKjZ2D3wA7vA7LtQ/edit?gid=0#gid=0')
# 
# ##############ATP View

# master<-unique(selection_master_ds)
# master_wide <- dcast(master, FSN + business_zone ~ nl_code, value.var = "nl_atp")
# #df_wide <- dcast(master, FSN + business_zone ~ nl_cod, value.var = "nl_atp")  # Typo fixed here
# 
# master_wide <- dcast(master, ... ~ nl_code, value.var = "nl_atp")
# master_wide <- dcast(master_wide, ... ~ nl_code2, value.var = "nl2_atp")
# 
# master_wide<-master_wide[,-c("NA")]
# 
# master_wide$ban_ven_wh_nl_01nl <- ifelse(is.na(master_wide$ban_ven_wh_nl_01nl / master_wide$sum_check), 0, master_wide$ban_ven_wh_nl_01nl / master_wide$sum_check)
# master_wide$bhi_pad_wh_nl_01nl<-ifelse(is.na(master_wide$bhi_pad_wh_nl_01nl/master_wide$sum_check),0,master_wide$bhi_pad_wh_nl_01nl/master_wide$sum_check)
# master_wide$frk_bts<-ifelse(is.na(master_wide$frk_bts/master_wide$sum_check),0,master_wide$frk_bts/master_wide$sum_check)
# master_wide$nad_har_wh_kl_nl_01nl<-ifelse(is.na(master_wide$nad_har_wh_kl_nl_01nl/master_wide$sum_check),0,master_wide$nad_har_wh_kl_nl_01nl/master_wide$sum_check)
# 
# master_wide$bhi_vas_wh_nl_01nl<-ifelse(is.na(master_wide$bhi_vas_wh_nl_01nl/master_wide$sum_check),0,master_wide$bhi_vas_wh_nl_01nl/master_wide$sum_check)
# master_wide$gur_san_wh_nl_01nl<-ifelse(is.na(master_wide$gur_san_wh_nl_01nl/master_wide$sum_check),0,master_wide$gur_san_wh_nl_01nl/master_wide$sum_check)
# master_wide$malur_bts<-ifelse(is.na(master_wide$malur_bts/master_wide$sum_check),0,master_wide$malur_bts/master_wide$sum_check)
# master_wide$ulub_bts<-ifelse(is.na(master_wide$ulub_bts/master_wide$sum_check),0,master_wide$ulub_bts/master_wide$sum_check)
# 
# master_wide$total <- rowSums(master_wide[, 57:72], na.rm = TRUE)
# master_wide<-master_wide[,-c("buckets")]
# 
# master_wide$buckets <- with(master_wide, ifelse(
#   instock == 1,
#   "instock",
#   ifelse(
#     instock_post_iwit == 1,
#     "instock-iwit",
#     ifelse(
#       total > 2*ds_deficit ,
#       "National_stock",
#         "OOS"
#       )
#     )
#   )
# )
# 
# gcs_upload(master_wide, name = "ipc/Nav/Hyperlocal_IPC/selection_master_ds_mle_ls_atp_view.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# 
# 
# 

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

# instock_stn = gcs_get_object("ipc/Nav/Hyperlocal_IPC/total_manual_stn.csv", bucket = gcs_bucket)
