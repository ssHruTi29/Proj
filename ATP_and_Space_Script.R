options(java.parameters = "-Xmx8000m")
library(RJDBC)
library(plyr)
library(dplyr)
library(reshape)
library(lubridate)
library(data.table)
library(lubridate)


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

#####Input Files 

atp_details = gcs_get_object("ipc/Nav/Hyperlocal_IPC/HL_Inv_details.csv", bucket = gcs_bucket)
Bu_Vertical = gcs_get_object("ipc/Nav/IPC_Hyperlocal/analytical_bu.csv", bucket = gcs_bucket2)
Bu_Vertical$BusinessUnit = ifelse(Bu_Vertical$final_bu!="null",Bu_Vertical$final_bu,Bu_Vertical$analytic_business_unit)
Bu_Vertical=Bu_Vertical[,c("analytic_vertical","final_bu")]
setDT(atp_details)
Bu_Vertical=unique(Bu_Vertical)
atp_details = merge(atp_details, Bu_Vertical, 
                    by.x = "Vertical", 
                    by.y = "analytic_vertical", 
                    all.x = TRUE)
atp_details$BU=ifelse(atp_details$final_bu=="null",atp_details$BusinessUnit,atp_details$final_bu) 

####Remove Na columns in vertical
atp_details=atp_details[!is.na(atp_details$Vertical),]

###Reading our master
Rep_Master<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_Target_Master.csv", bucket = gcs_bucket)

###Tags added for different sections - reading the FSNs for each tag
Pharma = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1bbTxOUZ4iIhXb3g5voR7FFTLXKH3ZUExj6f-Z0nZgkk/edit?gid=530428005#gid=530428005',sheet='Pharma'))
fsn_values <- Pharma$fsn
Pharma_data <- unique(data.frame(fsn = fsn_values, Tag = "Pharma"))

DBEFM = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1htB3QuvCwUO63hq1llxhkeTBpGrKtwadSTQKxDBC-f0/edit?gid=318885363#gid=318885363',sheet='New Master'))
fsn_values <- DBEFM$FSN
DBEFM_data <- unique(data.frame(fsn = fsn_values, Tag = "DBEFM"))

Tobacco = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1bbTxOUZ4iIhXb3g5voR7FFTLXKH3ZUExj6f-Z0nZgkk/edit?gid=530428005#gid=530428005',sheet='Tobbaco'))
fsn_values <- Tobacco$fsn
Tobacco_data <- unique(data.frame(fsn = fsn_values, Tag = "Tobacco"))

FnV = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1bbTxOUZ4iIhXb3g5voR7FFTLXKH3ZUExj6f-Z0nZgkk/edit?gid=530428005#gid=530428005',sheet='FnV'))
fsn_values <- FnV$FSN
FnV_data <- unique(data.frame(fsn = fsn_values, Tag = "FnV"))

Tropical = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1FQJHMKJxxtgA4EH5Qun9WWQcC56-z66gwrR2TExLrhk/edit?gid=0#gid=0',sheet='FSN_City_Plan'))
fsn_values <- Tropical$product_id
Tropical_data <- unique(data.frame(fsn = fsn_values, Tag = "Tropical"))

Sampling = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1eRFQTJv3UXTfS716NprepJn2JMsyVcVlVIVoYkLSqp0/edit?gid=0#gid=0',sheet='Final FSN List'))
fsn_values <- Sampling$FSN
Sampling_data <- unique(data.frame(fsn = fsn_values, Tag = "Sampling"))

###Add the Tags to atp data
All_Tags=rbind(Pharma_data,Tobacco_data,FnV_data,Tropical_data,Sampling_data,DBEFM_data)
atp_details= merge(atp_details,All_Tags,by="fsn",all.x=T)

##Filtering only for store data
atp_details=atp_details[atp_details$fc_area=="store",]

###Reading the DS master
DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
DS=DS[,c("DS","Store Type","City")]

atp_details$npd=as.Date(atp_details$non_promisable_date)

############## Creating Atp master

###filtering the master from our atp_details based on fsnXfc combination from rep_master=ds_target master  
atp_master=atp_details[paste0(atp_details$fsn,atp_details$fc) %in% paste0(Rep_Master$FSN,Rep_Master$DS),]
setDT(atp_details)
##summarizing the atp_details
atp_details_summary = atp_details[, .(atp_units = sum(atp)), by = .(fsn, fc, title, BU, SuperCategory, Vertical,Tag,npd,seller_id)]

##summarizing the atp_master
setDT(atp_master)
atp_master_summary = atp_master[, .(atp_units_m = sum(atp)), by = .(fsn, fc, title, BU, SuperCategory, Vertical,Tag,npd,seller_id)]

###SH details 
sh_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/sourcing_hub.csv", bucket = gcs_bucket2)

##DS details
ds_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
atp_details_summary$sh_code_flag=ifelse(atp_details_summary$fc %in% sh_master$code,1,0)
atp_details_summary$ds_code_flag=ifelse(atp_details_summary$fc %in% ds_master$DS,1,0)
atp_details_summary = merge(atp_details_summary, atp_master_summary, 
                            by = c("fsn", "fc", "title", "BU", "SuperCategory", "Vertical","Tag","npd","seller_id"), 
                            all.x = TRUE)

###Changing the BU based Tags - this is if the FSN wasn't present in eg pharma sheet but the BU is tagged as Pharmacy, we tag it as Pharma
atp_details_summary$Tag=ifelse(is.na(atp_details_summary$Tag)&atp_details_summary$BU=="Pharmacy","Pharma",atp_details_summary$Tag)
atp_details_summary$Tag=ifelse(is.na(atp_details_summary$Tag)&atp_details_summary$BU=="F&V","FnV",atp_details_summary$Tag)
atp_details_summary$Tag=ifelse(is.na(atp_details_summary$Tag)&atp_details_summary$BU=="Shopsy","Shopsy",atp_details_summary$Tag)
atp_details_summary$Tag=ifelse(is.na(atp_details_summary$Tag)&atp_details_summary$BU=="WrongProcurement","WrongProcurement",atp_details_summary$Tag)
atp_details_summary$Tag=ifelse(is.na(atp_details_summary$Tag)&atp_details_summary$BU=="Chocolates","Chocolates",atp_details_summary$Tag)
atp_details_summary$Tag=ifelse(is.na(atp_details_summary$Tag)&atp_details_summary$BU=="Furniture","Furniture",atp_details_summary$Tag)
atp_details_summary$Tag=ifelse(is.na(atp_details_summary$Tag)&atp_details_summary$BU=="DBEFM","DBEFM",atp_details_summary$Tag)
atp_details_summary$Tag=ifelse(is.na(atp_details_summary$Tag)&atp_details_summary$BU=="Refurbished","Refurbished",atp_details_summary$Tag)

atp_details_summary=atp_details_summary[,-c("title")]
atp_details_summary=merge(atp_details_summary,DS,by.x=c("fc"),by.y=("DS"),all.x=T)

# temp=atp_details_summary
# atp_details_summary=temp


atp_details_summary$Final_Tag <- ifelse(
  atp_details_summary$Tag %in% c("Tropical", "Sampling"), "Tropical&Sampling",
  ifelse(
    atp_details_summary$Tag %in% c("Pharma", "DBEFM", "FnV", "Tobacco", "Chocolates", "Furniture", "WrongProcurement", "Shopsy", "Refurbished"), "Non_Dry&others",
    ifelse(
      is.na(atp_details_summary$Tag) & !is.na(atp_details_summary$atp_units_m), "Master",
      ifelse(
        is.na(atp_details_summary$npd), "OOM",  # handle NA here
        ifelse(atp_details_summary$npd < Sys.Date() %m+% months(3), "OOM-Expired", "OOM")
      )
    )
  )
)

#####Adding ACU Logic for space calculation #########

lbh = gcs_get_object("ipc/Nav/IPC_Hyperlocal/lbh.csv", bucket = gcs_bucket2)
setDT(lbh)
lbh = lbh[,.(l =mean(length_avg),b =mean(breadth_avg),h = mean(height_avg),w=mean(weight_avg)),by=.(product_detail_fsn)]
lbh = lbh[,`:=`(volumetric = l*b*h/1728)]
setnames(lbh,old="product_detail_fsn",new="fsn")
atp_details_summary = lbh[atp_details_summary, on=c('fsn')]

acu_avg = gcs_get_object("ipc/Nav/IPC_Hyperlocal/analytical_vertical.csv", bucket = gcs_bucket2)
setDT(acu_avg)
setnames(acu_avg,old="analytic_vertical",new="Vertical")
atp_details_summary = acu_avg[atp_details_summary, on=c('Vertical')]

atp_details_summary = atp_details_summary[,`:=`(volumetric = ifelse(`Average of ACU`*3 > volumetric,`Average of ACU`,volumetric))]
#atp_details_summary = atp_details_summary[,`:=`(Space = volumetric * atp_units) ]

##################################

# temp=atp_details_summary
# atp_details_summary=temp
setDT(atp_details_summary)
verticals_to_exclude=data.frame(Vertical=c("PaneerTofu", "IceCubes", "Chicken", "Vegetables", "Fruits",
                       "ButterMargarine", "Cheese", "Chocolates", "CurdYogurt", "Eggs",
                       "FishSeafood", "FruitDrinks", "HealthDrinkMixes", "IceCreams",
                       "ReadyMeals", "Breads", "Milk", "FreshCream", "Buttermilk",
                       "SweetsMithai", "Cigarettes", "Tobacco"),Vertical_Flag=1)
atp_details_summary=merge(atp_details_summary,verticals_to_exclude,by=c("Vertical"),all.x=T)
atp_details_summary$Vertical_Flag=ifelse(is.na(atp_details_summary$Vertical_Flag),0,atp_details_summary$Vertical_Flag)

atp_details_summary[, space_Flag := ifelse(!is.na(w) & w >= 5, "Pallet", 
                                           ifelse(BU %in% c("CoreElectronics", "EmergingElectronics", "Mobile"), 
                                                  "HVC", 
                                                  "Food&NonFood"))]


Space_Avl = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1Ghu4rYp3lWo7J1zrrI4C9t0h2h-5jhsFOLN5lJF_6wc/edit?gid=0#gid=0',sheet="Final"))
Space_Avl=Space_Avl[,c("Fc Code","Space_Flag","Space")]
setnames(Space_Avl, old = c("Fc Code", "Space_Flag", "Space"), new = c("fc", "space_Flag", "Actual_Space"))
Space_Avl=Space_Avl[!is.na(Space_Avl$fc),]
Space_Avl=unique(Space_Avl)
Space_Avl <- Space_Avl[, .(Actual_Space = max(Actual_Space, na.rm = TRUE)), by = .(fc, space_Flag)]


atp_details_summary=merge(atp_details_summary,Space_Avl,by=c("fc","space_Flag"),all.x=T)
atp_details_summary[, row_count := sum(Final_Tag != "Non_Dry&others"), by = .(fc, space_Flag)]
atp_details_summary$Actual_Space=ifelse(atp_details_summary$Final_Tag=="Non_Dry&others",0,atp_details_summary$Actual_Space)
atp_details_summary[,Actual_Space_p:=Actual_Space/row_count]
atp_details_summary$Actual_Space_p=ifelse(is.na(atp_details_summary$Actual_Space_p),0,atp_details_summary$Actual_Space_p)

#####Seller Off-boaded filter
atp_details_summary$Seller_check=ifelse(atp_details_summary$seller_id=="d591418b408940a0",1,0)


###Reading instock DRR

instock_corrected_drr= data.table(gcs_get_object("ipc/Nav/Hyperlocal_IPC/Instock_CorrectedDRR.csv", bucket = gcs_bucket))

###merging DRR to atp_details_summary
atp_details_summary = merge(
  atp_details_summary,
  instock_corrected_drr,
  by.x = c("fsn", "fc"),
  by.y = c("fsn", "business_zone"),
  all.x = TRUE
)
atp_details_summary=unique(atp_details_summary)

###adding Live DS Flag
ds_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
setDT(ds_master)
ds_master=ds_master[,c("DS","Live")]
setnames(ds_master,old="DS",new="fc")
atp_details_summary=merge(atp_details_summary,ds_master,by=c("fc"),all.x=T)

############Adding top 2500 Flag
top_2500 = gcs_get_object("ipc/Nav/Hyperlocal_IPC/top_2500_master_daily.csv", bucket = gcs_bucket)
setDT(top_2500)
top_2500 = top_2500[,c("ds_fkint_cp_santa_hl_core_selection_1_0.store_id","ds_fkint_cp_santa_hl_core_selection_1_0.rank_bucket","ds_fkint_cp_santa_hl_core_selection_1_0.product_id")]
setnames(top_2500,old=c("ds_fkint_cp_santa_hl_core_selection_1_0.store_id","ds_fkint_cp_santa_hl_core_selection_1_0.rank_bucket","ds_fkint_cp_santa_hl_core_selection_1_0.product_id"),new=c("fc","rank","fsn"))
top_2500$rank=ifelse(top_2500$rank=="Rest","Top 501-2500",top_2500$rank)

atp_details_summary=merge(atp_details_summary,top_2500,by=c("fc","fsn"),all.x=T)


############################ uploaded #############################

gcs_upload(
  atp_details_summary, 
  name = "ipc/Nav/Hyperlocal_IPC/atp_sh_ds.csv", 
  object_function = function(input, output) write.csv(input, output, row.names = FALSE, fileEncoding = "UTF-8"),
  predefinedAcl = "bucketLevel"
)

# atp_details_mle=atp_details_summary[atp_details_summary$BU %in% c("LargeAppliances","EmergingElectronics","CoreElectronics","Mobile","Lifestyle","LifeStyle"),]
# write.csv(atp_details_mle,"D:\\shruti.shahi\\Outputs\\atp_details_mle.csv")

####################### Seller data in transit #####################

##DS details
ds_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
ds_it = gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_IT.csv", bucket = gcs_bucket)
setDT(ds_it)
ds_it[, IWIT_Intransit := as.numeric(IWIT_Intransit)]
setDT(ds_it)

ds_it = ds_it[Inwarding_Status %in% c('IN_TRANSIT','INWARDING','INITIATED')]
ds_it[,`:=`(Creation_Date = as.POSIXct(Creation_Date, format="%Y-%m-%d %H:%M:%S"))]
ds_it[,`:=`(days = as.numeric(Sys.time()-Creation_Date,units = "days"))]

ds_it_seller = ds_it[ds_it$Seller=="d591418b408940a0",]
ds_it_seller=ds_it_seller[ds_it_seller$Dest_FC %in% ds_master$DS,]
setDT(ds_it_seller)

gcs_upload(
  ds_it_seller, 
  name = "ipc/Nav/Hyperlocal_IPC/ds_it_seller.csv", 
  object_function = function(input, output) write.csv(input, output, row.names = FALSE, fileEncoding = "UTF-8"),
  predefinedAcl = "bucketLevel"
)

### MLE Impact summary

# atp_data=atp_details_summary[atp_details_summary$Seller_check==1 & atp_details_summary$BU %in% c("EmergingElectronics","CoreElectronics","LargeAppliances","Mobile"),]
# ds_it_data=ds_it[ds_it$BusinessUnit %in% c("EmergingElectronics","CoreElectronics","LargeAppliances","Mobile"), ]
# 
# write.csv(atp_data,"D:\\shruti.shahi\\Outputs\\atp_data.csv")
# write.csv(ds_it_data,"D:\\shruti.shahi\\Outputs\\ds_it_data.csv")

###########




############ ATP Total and Master
# 
# atp_details = gcs_get_object("ipc/Nav/Hyperlocal_IPC/HL_Inv_details.csv", bucket = gcs_bucket)
# atp_details = atp_details[atp_details$BusinessUnit %in% c("BGM","LifeStyle","CoreElectronics","EmergingElectronics","Home","LargeAppliances","Mobile"),]
# atp_details = atp_details[atp_details$fc_area=="store",]
# 
# Target_Master<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_Target_Master.csv", bucket = gcs_bucket)
# atp_master=atp_details[atp_details$fsn %in% Target_Master$FSN,]
# # atp_master=atp_master[atp_master$fc_area=="store",]
# 
# setDT(atp_details)
# atp_details_summary = atp_details[, .(atp_units = sum(atp)), by = .(fsn, fc, title, BusinessUnit, SuperCategory, Vertical)]
# 
# setDT(atp_master)
# atp_master_summary = atp_master[, .(atp_units_m = sum(atp)), by = .(fsn, fc, title, BusinessUnit, SuperCategory, Vertical)]
# 
# ###NL details
# nl_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/nl_hub.csv", bucket = gcs_bucket2)
# 
# ###SH details 
# sh_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/sourcing_hub.csv", bucket = gcs_bucket2)
# 
# ##DS details
# ds_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
# 
# atp_details_summary$sh_code_flag=ifelse(atp_details_summary$fc %in% sh_master$code,1,0)
# atp_details_summary$ds_code=ifelse(atp_details_summary$fc %in% ds_master$DS,1,0)
# 
# atp_details_summary = merge(atp_details_summary, atp_master_summary, 
#                             by = c("fsn", "fc", "title", "BusinessUnit", "SuperCategory", "Vertical"), 
#                             all.x = TRUE)
# 
# atp_details_summary$atp_units_m=ifelse(is.na(atp_details_summary$atp_units_m),0,atp_details_summary$atp_units_m)
# # atp_LS= atp_details_summary[atp_details_summary$BusinessUnit=="LifeStyle",]
# # write.csv(atp_LS,"D:\\shruti.shahi\\Outputs\\lifestyle\\atp_LS_analysis.csv")
# atp_details_summary=atp_details_summary[,-c("title")]
# 
# 
# ########### Reverse
# ds_it_reverse = gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_Reverse_IT.csv", bucket = gcs_bucket)
# setDT(ds_it_reverse)
# ds_to_sh = ds_it_reverse[Dest_FC %in% unique(sourcing_hub$code)]
# ds_to_sh = ds_to_sh[Src_FC %in% unique(ds_store$DS)]
# gcs_upload(ds_to_sh, name = "ipc/Nav/Hyperlocal_IPC/ds_to_sh.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# 
# ####Reverse sh to nl
# 
# gcs_upload(sh_to_nl, name = "ipc/Nav/Hyperlocal_IPC/sh_to_nl.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# 
# ###NL to sh
# 
# nl_to_sh = data.table(gcs_get_object("ipc/Nav/Anuj/IWIT_InTransit_FSN.csv", bucket = gcs_bucket))
# nl_to_sh = nl_to_sh[Src_FC %in% unique(nl_hub$code) | Src_FC %in% unique(nl_hub2$code)]
# nl_to_sh = nl_to_sh[Dest_FC %in% unique(sourcing_hub$code)]
# gcs_upload(nl_to_sh, name = "ipc/Nav/Hyperlocal_IPC/nl_to_sh.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
# 
# 
# 
# 
# 
# ############# SH View
# atp_details_summary_SH=atp_details_summary[atp_details_summary$sh_code_flag==1,]
# 
# sh_to_nl = data.table(gcs_get_object("ipc/Nav/Anuj/IWIT_InTransit_FSN.csv", bucket = gcs_bucket))
# sh_to_nl = sh_to_nl[Src_FC %in% unique(sh_master$code)]
# sh_to_nl = sh_to_nl[Dest_FC %in% unique(nl_master$code)]
# 
# setDT(sh_to_nl)
# sh_to_nl_summary <- sh_to_nl[, .(IWIT_Intransit = sum(IWIT_Intransit)), 
#                              by = c("fsn", "Dest_FC", "Src_FC", "SuperCategory", "Vertical", "BusinessUnit", "Inwarding_Status")]
