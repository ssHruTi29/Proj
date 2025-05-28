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
new_instock = gcs_get_object("ipc/Nav/instock_inputs/instock_new_bucket.csv", bucket = gcs_bucket2)
Master = data.table(gcs_get_object("ipc/Nav/instock_inputs/master_ds.csv", bucket = gcs_bucket2))
br = data.table(gcs_get_object("ipc/Nav/instock_inputs/br.csv", bucket = gcs_bucket2))
top_2500 = data.table(gcs_get_object("ipc/Nav/instock_inputs/top_2500.csv", bucket = gcs_bucket2))


setDT(new_instock)
###Reading loss tree
loss_tree = gcs_get_object("Nivetha /Instock_Loss_Tree.csv", bucket = gcs_bucket2)
loss_tree <- rawToChar(loss_tree)
loss_tree<- fread(loss_tree)
setDT(loss_tree)

seller = gcs_get_object("ipc/Nav/Hyperlocal_IPC/seller_norm_0.csv", bucket = gcs_bucket)
setDT(seller)
seller = seller[,.(fsn,DS,norm_0)]
setnames(seller,old=c('fsn'),new=c('FSN'))

loss_tree = loss_tree[,.(FSN,City,Reason)]
loss_tree[,`:=`(sh_city = tolower(City))]
loss_tree[,`:=`(City = NULL)]
new_instock = loss_tree[new_instock, on=c('FSN','sh_city')]
new_instock  = top_2500[new_instock , on=c('FSN','DS')]
setnames(br, old='City',new='sh_city')
new_instock  = br[new_instock , on=c('FSN','sh_city')]
new_instock[,`:=`(flag =1)]
new_instock = seller[new_instock, on =c('FSN','DS')]
new_instock[, final_reason := fcase(
  instock == 1, "instock",
  iwit_gt_36 > 0, "intransit_gt_design",
  iwit_lt_36 > 0, "intransit_lt_design",
  norm_0 == 1, "seller",
  !is.na(Reason), Reason,
  default = "no_reason_tagged"
)]
setnames(new_instock, old=c('iwit_gt_36','iwit_lt_36'),new=c('iwit_gt_design','iwit_lt_design'))
ds_level_instock=new_instock
gcs_upload(new_instock, name = "ipc/Nav/Hyperlocal_IPC/instock_new.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

##############Creating SH data

ds_instock = new_instock[Live ==1,.(instock = sum(instock),instock_it =sum(instock_it),flag_original=sum(flag)),by=.(FSN,sh_city)]

future_drr_sh = gcs_get_object("ipc/Nav/Hyperlocal_IPC/future_proj_sh.csv", bucket = gcs_bucket,saveToDisk = "temp_file.csv",overwrite = TRUE)
future_drr_sh<-read.csv("temp_file.csv")
setDT(future_drr_sh)

future_drr_sh[,`:=`(City =tolower(City))]

sh_instock = new_instock[,.(DRR = sum(DRR),atp = sum(atp),sh_atp = mean(sh_atp),ds_deficit = sum(ds_deficit,na.rm=T),PO_qty = mean(PO_qty),schedule_quantity = mean(schedule_quantity),flag_nrows = sum(flag)), by=.(FSN,sh_city)]
sh_instock =ds_instock[sh_instock, on =c('FSN','sh_city')]

future_drr_sh[,`:=`(sh_city = ifelse(City == 'pune','mumbai',City))]
future_drr_sh[,`:=`(sh_city = ifelse(City == 'ahmedabad','mumbai',City))]

future_drr_sh[,`:=`(sh_city = ifelse(City == 'lucknow','delhincr',sh_city))]
future_drr_sh[,`:=`(sh_city = ifelse(City == 'jaipur','delhincr',sh_city))]
future_drr_sh[,`:=`(sh_city = ifelse(City == 'guwahati','kolkata',sh_city))]

future_drr_sh = future_drr_sh[,.( L14D = sum(L14D),L7D = sum(L7D),week_1_vol = sum(week_1_vol),week_2_vol = sum(week_2_vol),week_3_vol = sum(week_3_vol),week_4_vol = sum(week_4_vol)), by=.(FSN,sh_city)]
setnames(sh_instock, old='sh_city',new='City')

setnames(future_drr_sh, old='sh_city',new='City')
#part 2

sh_instock = future_drr_sh[sh_instock, on =c('FSN','City')]

sh_instock[,`:=`(week_4_vol = ifelse(is.na(week_4_vol),0,week_4_vol))]
sh_instock[,`:=`(ds_deficit = ifelse(is.na(ds_deficit),0,ds_deficit))]
sh_instock[,`:=`(sh_atp = ifelse(is.na(sh_atp),0,sh_atp))]

sh_instock[,`:=`(alert = ifelse(sh_atp <=week_4_vol*10+ds_deficit,1,0))]
sh_instock[,`:=`(sh_deficit = ifelse(alert ==1,(week_4_vol*10+ds_deficit)-sh_atp,0))]

#adding buy ready

#adding BU SC
Master[,`:=`(City = tolower(City))]
Master = unique(Master[,.(FSN,BU,SC,Analytical_Vertical,brand,Title,NOOS)])
Master[,`:=`(flag =1)]
Master = Master[,`:=`(cu = cumsum(flag)),by=.(FSN)]
Master = Master[cu ==1]
sh_instock =Master[sh_instock, on=c('FSN')]
sh_instock[,flag := NULL]
#setnames(sh_instock,old="i.flag",new="flag")
#adding top 200

#sh_instock = sams[sh_instock, on =c('FSN','City')]


# adding NOOS


####Addin patch for sh_instock & final ds_instock

instock = new_instock[Live ==1]
instock[, flag := 1]

#summerising the instock
instock <- instock[, .(
  instock = sum(instock, na.rm = TRUE),
  ds_deficit = sum(ds_deficit, na.rm = TRUE),
  instock_it = sum(instock_it, na.rm = TRUE),
  flag = sum(flag, na.rm = TRUE),
  sh_code = first(sh_code)
), by = .(FSN, sh_city)]

sh_instock <- merge(sh_instock, instock, 
                    by.x = c("FSN", "City"),  # Matching columns in sh_instock
                    by.y = c("FSN", "sh_city"),  # Matching columns in instock
                    all.x = TRUE)

sh_instock <- as.data.table(sh_instock)
sh_instock[, MSTN_L1 := ds_deficit.x]
sh_instock[, MSTN_L2 := round(ds_deficit.x + (10 * DRR), 0)]
sh_instock[, Instock_L1 := ifelse(sh_atp > MSTN_L1, 1, 0)]
sh_instock[, Instock_L2 := ifelse(sh_atp > MSTN_L2, 1, 0)]


#sh_instock <- sh_instock[!is.na(Tag)]

# merged_data <- merge(sh_subset, new_instock, 
#                      by = c("FSN", "sh_code", "City"),
#                      all.x = TRUE)  # Left join: keep all rows from sh_subset

tag = new_instock[,.(FSN,sh_city,Tag)]
setnames(tag, old ='sh_city',new='City')
tag[,`:=`(flag =1)]
tag[,`:=`(cu = cumsum(flag)),by=.(FSN,City)]
tag = tag[cu==1]
tag[,`:=`(cu= NULL,flag = NULL)]
sh_instock = tag[sh_instock, on=c('FSN','City')]
setnames(sh_instock,old=c("instock.x","instock_it.x","instock.y","ds_deficit.y","instock_it.y"),new=c("sh_instock","sh_instock_it","ds_instock","ds_deficit","ds_instock_it"))
setnames(sh_instock,old=c("Instock_L1","Instock_L2"),new=c("sh_instock_L1","sh_instock_L2"))
sh_instock[, ds_deficit.x := NULL]

write.csv(sh_instock,"C://Users//shruti.shahi//Documents//sh_instock.csv")
#adding Sams
gcs_upload(sh_instock, name = "ipc/Nav/Hyperlocal_IPC/sh_instock_new.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
