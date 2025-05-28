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


############## Getting the reverse file for DS ################################

reverse_data= gcs_get_object("ipc/Nav/Hyperlocal_IPC/value_ds_2.csv", bucket = gcs_bucket)
Rep_Master<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_Target_Master.csv", bucket = gcs_bucket)

reverse_data$key=paste0(reverse_data$FSN,reverse_data$business_zone)
Rep_Master$key=paste0(Rep_Master$FSN,Rep_Master$DS)

####To remove whatever was in reverse_data and also in Rep master( certain FSNs common for BGM and LS)

reverse_data1=reverse_data[!reverse_data$key %in% Rep_Master$key, ]
reverse_data2=reverse_data[reverse_data$key %in% Rep_Master$key,]


#####################
# reverse_data <- reverse_data %>%
#   mutate(key = paste0(FSN, business_zone)) %>%
#   anti_join(
#     Rep_Master %>% mutate(key = paste0(FSN, DS)),
#     by = "key"
#   ) %>%
#   select(-key)
reverse_data=reverse_data1[reverse_data1$comments=="OOM",]
#sale_30D=gcs_get_object("ipc/Nav/Hyperlocal_IPC/Sales_L30D.csv", bucket = gcs_bucket)

####Reading the sale data for last 30 days
sale_30D<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/Sales_L30D.csv", bucket = gcs_bucket,saveToDisk = "temp_file.csv",overwrite = TRUE)
sale_30D<-read.csv("temp_file.csv")
rm(temp_file.csv)
setDT(sale_30D)

#####Current inventory
atp = gcs_get_object("ipc/Nav/Hyperlocal_IPC/Inventory_Snapshot.csv", bucket = gcs_bucket)
setDT(atp)

atp_summary <- atp[, .(atp_units = sum(atp)), by = .(fsn, fc)]
sale_30D_summary=sale_30D[, .(sale_units = sum(units)), by = .(fuf.product_id, business_zone)]

atp_summary <- merge(
  atp_summary, 
  sale_30D_summary, 
  by.x = c("fsn", "fc"), 
  by.y = c("fuf.product_id", "business_zone"), 
  all.x = TRUE
)

prod_cat = data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/prod_cat.csv", bucket = gcs_bucket2))
setnames(prod_cat , old ='product_id',new='fsn')

atp_summary <- merge(
  atp_summary, 
  prod_cat, 
  by="fsn",
  all.x = TRUE
)

####If FSN is in ATP but no sale happened in last 30 days

atp_summary_no_sale=atp_summary[is.na(atp_summary$sale_units),]
reverse_data$key=paste0(reverse_data$FSN,reverse_data$business_zone)
atp_summary_no_sale$key=paste0(atp_summary_no_sale$fsn,atp_summary_no_sale$fc)

reverse_nosale=reverse_data[reverse_data$key %in% atp_summary_no_sale$key,]
#reverse_nosale_Ls=reverse_nosale[reverse_nosale$BU %in% c("LifeStyle","Lifestyle"),]
reverse_nosale_Ls=reverse_nosale

####Creating the capacity constraint that is aligned - ####### To be changed

#capacity=read_sheet("https://docs.google.com/spreadsheets/d/1v5clNfMeYikUdRhQMls4-DxQOF50CgZP/edit?gid=2084702985#gid=2084702985")
capacity_aligned=data.frame(City=c("Ahmedabad","Bangalore","Delhi","Guwahati","Jaipur","Kolkata","Lucknow","Mumbai","Pune"),Caps=c(554,5666,5000,197,372,1040,987,4195,1182))
capacity_aligned$Cap_final=ceiling(capacity_aligned$Caps/200)*200
reverse_nosale_Ls <- merge(
  reverse_nosale_Ls,
  capacity_aligned,
  by = "City",
  all.x = TRUE
)
reverse_nosale_Ls=reverse_nosale_Ls[order(reverse_nosale_Ls$City,-reverse_nosale_Ls$reverse_qty),]
setDT(reverse_nosale_Ls)

reverse_nosale_Ls[, prev_row := {
  out <- numeric(.N)
  out[1] <- reverse_qty[1]
  for(i in 2:.N) {
    if(City[i] == City[i-1]) {
      out[i] <- out[i-1] + reverse_qty[i]
    } else {
      out[i] <- reverse_qty[i]
    }
  }
  out
}]
reverse_nosale_Ls$cap_check_filter=ifelse(reverse_nosale_Ls$Cap_final<reverse_nosale_Ls$prev_row,1,0)
final_file_to_reverse=reverse_nosale_Ls[reverse_nosale_Ls$cap_check_filter==0,]

gcs_upload(final_file_to_reverse, name = "ipc/Nav/Hyperlocal_IPC/Daily_reverse.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

#write.csv(reverse_nosale_Ls,"D:\\shruti.shahi\\MLE\\reverse_LS.csv")

#####################################################################################################
############## Getting the reverse file for SH ################################

reverse_data= gcs_get_object("ipc/Nav/Hyperlocal_IPC/value_ds_2.csv", bucket = gcs_bucket)
reverse_data=reverse_data[reverse_data$comments=="OOM",]
#sale_30D=gcs_get_object("ipc/Nav/Hyperlocal_IPC/Sales_L30D.csv", bucket = gcs_bucket)

####Reading the sale data for last 30 days
sale_30D<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/Sales_L30D.csv", bucket = gcs_bucket,saveToDisk = "temp_file.csv",overwrite = TRUE)
sale_30D<-read.csv("temp_file.csv")
rm(temp_file.csv)
setDT(sale_30D)

#####Current inventory
atp = gcs_get_object("ipc/Nav/Hyperlocal_IPC/Inventory_Snapshot.csv", bucket = gcs_bucket)
setDT(atp)

atp_summary <- atp[, .(atp_units = sum(atp)), by = .(fsn, fc)]
sale_30D_summary=sale_30D[, .(sale_units = sum(units)), by = .(fuf.product_id, business_zone)]

atp_summary <- merge(
  atp_summary, 
  sale_30D_summary, 
  by.x = c("fsn", "fc"), 
  by.y = c("fuf.product_id", "business_zone"), 
  all.x = TRUE
)

prod_cat = data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/prod_cat.csv", bucket = gcs_bucket2))
setnames(prod_cat , old ='product_id',new='fsn')

atp_summary <- merge(
  atp_summary, 
  prod_cat, 
  by="fsn",
  all.x = TRUE
)

####If FSN is in ATP but no sale happened in last 30 days

atp_summary_no_sale=atp_summary[is.na(atp_summary$sale_units),]
reverse_data$key=paste0(reverse_data$FSN,reverse_data$business_zone)
atp_summary_no_sale$key=paste0(atp_summary_no_sale$fsn,atp_summary_no_sale$fc)

reverse_nosale=reverse_data[reverse_data$key %in% atp_summary_no_sale$key,]
#reverse_nosale_Ls=reverse_nosale[reverse_nosale$BU %in% c("LifeStyle","Lifestyle"),]
reverse_nosale_Ls=reverse_nosale

####Creating the capacity constraint that is aligned - ####### To be changed

capacity=read_sheet("https://docs.google.com/spreadsheets/d/1v5clNfMeYikUdRhQMls4-DxQOF50CgZP/edit?gid=2084702985#gid=2084702985")
capacity_aligned=data.frame(City=c("Ahmedabad","Bangalore","Delhi","Guwahati","Jaipur","Kolkata","Lucknow","Mumbai","Pune"),Caps=c(554,5666,5000,197,372,1040,987,4195,1182))
capacity_aligned$Cap_final=ceiling(capacity_aligned$Caps/200)*200
reverse_nosale_Ls <- merge(
  reverse_nosale_Ls,
  capacity_aligned,
  by = "City",
  all.x = TRUE
)
reverse_nosale_Ls=reverse_nosale_Ls[order(reverse_nosale_Ls$City,-reverse_nosale_Ls$reverse_qty),]
setDT(reverse_nosale_Ls)

reverse_nosale_Ls[, prev_row := {
  out <- numeric(.N)
  out[1] <- reverse_qty[1]
  for(i in 2:.N) {
    if(City[i] == City[i-1]) {
      out[i] <- out[i-1] + reverse_qty[i]
    } else {
      out[i] <- reverse_qty[i]
    }
  }
  out
}]
reverse_nosale_Ls$cap_check_filter=ifelse(reverse_nosale_Ls$Cap_final<reverse_nosale_Ls$prev_row,1,0)
final_file_to_reverse=reverse_nosale_Ls[reverse_nosale_Ls$cap_check_filter==0,]

gcs_upload(final_file_to_reverse, name = "ipc/Nav/Hyperlocal_IPC/Daily_reverse.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")

#write.csv(reverse_nosale_Ls,"D:\\shruti.shahi\\MLE\\reverse_LS.csv")

