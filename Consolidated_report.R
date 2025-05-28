library(stringr)
library(lubridate)
library(dplyr)
library(readxl)

BU_update<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BU Update file\\Bu Update Input1 2024-05-20 .csv",header = T)
List_fsn_combination<- BU_update[,c("fsn","vertical","super_category","bu","Band","active_inactive")]
cluster_list<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster Code_v2.xlsx",sheet="cluster",col_names = T)
cluster_fc_mapping<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster Code_v2.xlsx",sheet="cluster_fc",col_names = T)
List_fsn_combination<-cbind(List_fsn_combination)

repeated_df <- List_fsn_combination %>%
  slice(rep(1:nrow(List_fsn_combination), each = nrow(cluster_list))) %>%
  mutate(Cluster = rep(cluster_list$Cluster_list, nrow(List_fsn_combination)))  # Add id column from small_df
  #mutate(FC = rep(cluster_list$FC, nrow(List_fsn_combination)),Cluster = rep(cluster_list$Cluster, nrow(List_fsn_combination)))  # Add id column from small_df

ATP<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\ATP\\ATP May 20.csv",header = T)
ATP<-ATP[ATP$is_first_party_seller==1,]
ATP<-merge(ATP,cluster_fc_mapping,by.x=c("inventory_item_warehouse_id"),by.y=c("FC"),all.x=T)
ATP$inventory_item_quantity<-as.numeric(ATP$inventory_item_atp)
ATP_summary <- ATP %>%
  group_by(product_fsn,Cluster) %>%
  summarize(ATP_units = sum(inventory_item_atp))
#ATP_summary <-merge(ATP_summary,cluster_fc_mapping,by.x=c("inventory_item_warehouse_id"),by.y=c("FC"),all.x=T)
#ATP_summary<-ATP_summary[,-1]

repeated_df<-merge(repeated_df,ATP_summary,by.x=c("fsn","Cluster"),by.y=c("product_fsn","Cluster"),all.x=T)

### Calculating scheduling
sch_csv_files <- list.files(path = "C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Sch&PO\\May 20\\SchQty", pattern = "\\.csv$", full.names = TRUE)
sch_total<-0
for (csv_file in sch_csv_files) {
  print(csv_file)
  data <- read.csv(csv_file)
  sch_total<-rbind(sch_total,data)
}

sch_total <-merge(sch_total,cluster_fc_mapping,by.x=c("Warehouse"),by.y=c("FC"),all.x=T)
sch_summary<-sch_total %>%
  group_by(FSN,Cluster) %>%
  summarize(Sch_units = sum(Scheduled.Quantity))

#sch_summary <-merge(sch_summary,cluster_fc_mapping,by.x=c("Warehouse"),by.y=c("FC"),all.x=T)
#sch_summary<-sch_summary[,-1]

##Calculating open PO
PO_csv_files <- list.files(path = "C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Sch&PO\\May 20\\OpenPo", pattern = "\\.csv$", full.names = TRUE)
PO_total<-0
for (csv_file in PO_csv_files) {
  print(csv_file)
  data <- read.csv(csv_file)
  PO_total<-rbind(PO_total,data)
}

PO_total <-merge(PO_total,cluster_fc_mapping,by.x=c("origin_warehouse_id"),by.y=c("FC"),all.x=T)
PO_total$pending_quantity<-as.numeric(PO_total$pending_quantity)
PO_summary<-PO_total %>%
  group_by(fsn,Cluster) %>%
  summarize(PO_units = sum(pending_quantity,na.rm=T))

#PO_summary <-merge(PO_summary,cluster_fc_mapping,by.x=c("origin_warehouse_id"),by.y=c("FC"),all.x=T)
#PO_summary<-PO_summary[,-1]

repeated_df<-merge(repeated_df,sch_summary,by.x=c("fsn","Cluster"),by.y=c("FSN","Cluster"),all.x=T)
repeated_df<-merge(repeated_df,PO_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster"),all.x=T)
repeated_df$ATP_units[is.na(repeated_df$ATP_units)]<-0
repeated_df$Sch_units[is.na(repeated_df$Sch_units)]<-0
repeated_df$PO_units[is.na(repeated_df$PO_units)]<-0

Forecast<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Forecast\\Forecast_200524.csv",header = T)
Forecast_summary<-Forecast %>%
  group_by(fsn,fw_mapping) %>%
  summarize(Forecast_units = sum(forecast))

repeated_df<-merge(repeated_df,Forecast_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","fw_mapping"),all.x=T)
repeated_df$Forecast_units[is.na(repeated_df$Forecast_units)]<-0
no_of_days<-as.numeric(length(unique(Forecast$day)))
repeated_df$DRR<-repeated_df$Forecast_units/no_of_days

###IWIT
IWIT<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\IWIT\\IWIT 2024-05-20 .csv",header = T)
IWIT<-merge(IWIT,cluster_fc_mapping,by.x=c("Dest_FC"),by.y=c("FC"),all.x=T)
IWIT_summary<-IWIT %>%
  group_by(fsn,Cluster) %>%
  summarize(IWIT_units = sum(IWIT_Intransit))

#IWIT_summary <-merge(IWIT_summary,cluster_fc_mapping,by.x=c("Dest_FC"),by.y=c("FC"),all.x=T)
#IWIT_summary<-IWIT_summary[,-1]

repeated_df<-merge(repeated_df,IWIT_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster"),all.x=T)
repeated_df$IWIT_units[is.na(repeated_df$IWIT_units)]<-0

repeated_df$ATPReq_7day<-repeated_df$DRR*7
repeated_df$ATPavailable_7day<-pmin(repeated_df$ATPReq_7day,repeated_df$ATP_units)

repeated_df$ATPReq_14day<-repeated_df$DRR*14
repeated_df$ATPavailable_14day<-pmin(repeated_df$ATPReq_14day,repeated_df$ATP_units+repeated_df$Sch_units)

repeated_df$ATPReq_21day<-repeated_df$DRR*21
repeated_df$ATPavailable_21day<-pmin(repeated_df$ATPReq_21day,repeated_df$ATP_units+repeated_df$Sch_units+repeated_df$PO_units+repeated_df$IWIT_units)

##Over and understocks
repeated_df$Overstock<-pmax(repeated_df$ATP_units+repeated_df$Sch_units-(repeated_df$DRR*14),0)
repeated_df$Understock<-pmax((repeated_df$DRR*14)-repeated_df$ATP_units+repeated_df$Sch_units,0)

repeated_df_summary<-repeated_df %>%
  group_by(Cluster,bu,super_category,active_inactive) %>%
  summarize(ATP_units = sum(ATP_units),Sch_units=sum(Sch_units),PO_units=sum(PO_units),IWIT_units=sum(IWIT_units),DRR=sum(DRR),ATPReq_7day=sum(ATPReq_7day),ATPavailable_7day=sum(ATPavailable_7day),ATPReq_14day=sum(ATPReq_14day),ATPavailable_14day=sum(ATPavailable_14day),ATPReq_21day=sum(ATPReq_21day),ATPavailable_21day=sum(ATPavailable_21day),Overstock_units=sum(Overstock),Understock_units=sum(Understock))


###Checks
repeated_df_check<-repeated_df[repeated_df$super_category=='MakeupFragrances',]
# fc_list_summary<-ATP %>% group_by(inventory_item_warehouse_id) %>% summarize(ATP_units_atp=sum(ATP$inventory_item_atp,na.rm=T),ATP_units_qty=sum(ATP$inventory_item_quantity,na.rm=T))
# fc_list_summary <- ATP %>%
#   group_by(inventory_item_warehouse_id) %>%
#   summarize(ATP_units = sum(inventory_item_atp),ATP_unit_qty=sum(inventory_item_quantity))
# 
# ATP_check<-ATP[ATP$product_fsn=='EDOGFYADZG4FCZX5',]
#write.csv(repeated_df_check,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\po_check.csv",row.names = TRUE)




write.csv(repeated_df_check,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\MSTN_20th_MF_checkv1.csv",row.names = TRUE)
