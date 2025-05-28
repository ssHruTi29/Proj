library(stringr)
library(lubridate)
library(dplyr)
library(readxl)

BU_update<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BU Update file\\Bu Update Input1 2024-07-03 .csv",header = T)
List_fsn_combination<- BU_update[,c("fsn","vertical","super_category","bu","Band","active_inactive","brand")]
cluster_list<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster Code_v2.xlsx",sheet="cluster",col_names = T)
cluster_fc_mapping<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster Code_v2.xlsx",sheet="cluster_fc",col_names = T)
List_fsn_combination<-cbind(List_fsn_combination)

##Excluding SCs
exclude_sc<-c("0","BooksMedia","CoreEA","MensClothingEssentialsAndEthnic","Service","MensClothingCasualTopwear")
BU_update<-BU_update %>% filter(!(super_category %in% exclude_sc ))


###Excluding verticals
exclude_verticals<-c("PressureCooker","GasStove","Dinner Set","Cookware Set","Mop Set")
BU_update<-BU_update %>% filter(!(vertical %in% exclude_verticals ))
BU_update<-BU_update[!(BU_update$bu=="Mobile"&!grepl("^MOB",BU_update$fsn)),]

repeated_df <- List_fsn_combination %>%
  slice(rep(1:nrow(List_fsn_combination), each = nrow(cluster_list))) %>%
  mutate(Cluster = rep(cluster_list$Cluster_list, nrow(List_fsn_combination)))  # Add id column from small_df
#mutate(FC = rep(cluster_list$FC, nrow(List_fsn_combination)),Cluster = rep(cluster_list$Cluster, nrow(List_fsn_combination)))  # Add id column from small_df

Cluster_zone<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\cluster_zone.csv",header = T)
repeated_df<-merge(repeated_df,Cluster_zone,by=c("Cluster"),all.x=T)

##Cluster city mapping
cluster_city<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster_City.csv",header = T)
DI<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\DI List.csv",header = T)
DI<-data.frame(unique(DI[,c(1)]))
DI$Flag_DI<-1
colnames(DI)[1]<-"fsn"

repeated_df<-merge(repeated_df,cluster_city,by.x=c("Cluster"),by.y=c("Cluster_Code"),all.x=T)
repeated_df<-merge(repeated_df,DI,by.x =c("fsn"),by.y=c("fsn"),all.x=T)
repeated_df$Flag_DI[is.na(repeated_df$Flag_DI)]<-0

ATP<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\ATP\\ATP July 03.csv",header = T)
ATP<-ATP[ATP$is_first_party_seller==1,]
ATP<-merge(ATP,cluster_fc_mapping,by.x=c("inventory_item_warehouse_id"),by.y=c("FC"),all.x=T)
ATP$inventory_item_quantity<-as.numeric(ATP$inventory_item_atp)
ATP_summary <- ATP %>%
  group_by(product_fsn,Cluster) %>%
  summarize(ATP_units = sum(inventory_item_atp))
#ATP_summary <-merge(ATP_summary,cluster_fc_mapping,by.x=c("inventory_item_warehouse_id"),by.y=c("FC"),all.x=T)
#ATP_summary<-ATP_summary[,-1]

repeated_df<-merge(repeated_df,ATP_summary,by.x=c("fsn","Cluster"),by.y=c("product_fsn","Cluster"),all.x=T)


##Calculating open PO

PO_and_Sch<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Sch&PO\\Total_SCH_PO\\FWD_Deep_Zone_Data_V2 2024-07-03 .csv",header = T)
PO_and_Sch$Total_Sch<- PO_and_Sch$FKI_scheduling+PO_and_Sch$RAAS_Scheduling
PO_and_Sch$Total_PO<-PO_and_Sch$FKI_open_Po+PO_and_Sch$RAAS_open_Po

PO_summary<-PO_and_Sch %>%
  group_by(fsn,Cluster_Code) %>%
  summarize(PO_units = sum(Total_PO,na.rm=T),Sch_units=sum(Total_Sch,na.rm=T))


repeated_df<-merge(repeated_df,PO_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster_Code"),all.x=T)
#repeated_df<-merge(repeated_df,PO_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster"),all.x=T)
repeated_df$ATP_units[is.na(repeated_df$ATP_units)]<-0
repeated_df$Sch_units[is.na(repeated_df$Sch_units)]<-0
repeated_df$PO_units[is.na(repeated_df$PO_units)]<-0

Forecast<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Forecast\\Forecast_010724.csv",header = T)
Forecast_summary<-Forecast %>%
  group_by(fsn,fw_mapping) %>%
  summarize(Forecast_units = sum(forecast))

repeated_df<-merge(repeated_df,Forecast_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","fw_mapping"),all.x=T)
repeated_df$Forecast_units[is.na(repeated_df$Forecast_units)]<-0
no_of_days<-as.numeric(length(unique(Forecast$day)))
repeated_df$DRR<-repeated_df$Forecast_units/no_of_days

###IWIT
IWIT<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\IWIT\\IWIT_Monthly_FSN 2024-07-03 .csv",header = T)
#IWIT1<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\IWIT\\IWIT_Monthly_FSN 2024-05-29 .csv",header = T)

IWIT<-merge(IWIT,cluster_fc_mapping,by.x=c("Dest_FC"),by.y=c("FC"),all.x=T)
#IWIT1<-merge(IWIT1,cluster_fc_mapping,by.x=c("Dest_FC"),by.y=c("FC"),all.x=T)
IWIT_summary<-IWIT %>%
  group_by(fsn,Cluster) %>%
  summarize(IWIT_units = sum(IWIT_Intransit))

# IWIT_summary_new<-IWIT1 %>%
#   group_by(fsn,Cluster) %>%
#   summarize(IWIT_units = sum(IWIT_Intransit))


#IWIT_summary <-merge(IWIT_summary,cluster_fc_mapping,by.x=c("Dest_FC"),by.y=c("FC"),all.x=T)
#IWIT_summary<-IWIT_summary[,-1]

repeated_df<-merge(repeated_df,IWIT_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster"),all.x=T)
repeated_df$IWIT_units[is.na(repeated_df$IWIT_units)]<-0


Sourcing_hub_brands<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Rolling Plan _ SH.xlsx",sheet="FinalSHBrands",col_names = T)
# Sourcing_hub_brands<-Sourcing_hub_brands[-1,]
# colnames(Sourcing_hub_brands)<-Sourcing_hub_brands[1,]
# Sourcing_hub_brands<-Sourcing_hub_brands[-1,]
Sourcing_hub_brands<-unique(Sourcing_hub_brands[,c("Brand")])
Sourcing_hub_brands$Flag_SH_Brand<-1

repeated_df<-merge(repeated_df,Sourcing_hub_brands,by.x=c("brand"),by.y=c("Brand"),all.x=T)
repeated_df$Flag_SH_Brand[is.na(repeated_df$Flag_SH_Brand)]<-0

####################################

##Over and understocks
#repeated_df$Overstock<-pmax(repeated_df$ATP_units+repeated_df$Sch_units-(repeated_df$DRR*14),0)
#repeated_df$Understock<-pmax((repeated_df$DRR*14)-repeated_df$ATP_units+repeated_df$Sch_units,0)

##### Sourcing Hub Data ####

SH_base<-BU_update[,c("fsn","QOH_SH","PO_SH","IWIT_PACKING_TABLE")]

# SH_base<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\SH\\SH_data_10Jun.xlsx",sheet="mapped_data",col_names = T)
# SH_base<-SH_base[-1,]
# colnames(SH_base)<-SH_base[1,]
# SH_base<-SH_base[-1,]
# SH_base<-SH_base[,c("fsn","Cluster_mapping","QOH_SH","PO_SH","IWIT_PACKING_TABLE")]
# repeated_df<-merge(repeated_df,SH_base,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster_mapping"),all.x=T)
# repeated_df$QOH_SH<-as.numeric(repeated_df$QOH_SH)
# repeated_df$PO_SH<-as.numeric(repeated_df$PO_SH)
# repeated_df$IWIT_PACKING_TABLE<-as.numeric(repeated_df$IWIT_PACKING_TABLE)
# repeated_df$QOH_SH[is.na(repeated_df$QOH_SH)]<-0
# repeated_df$PO_SH[is.na(repeated_df$PO_SH)]<-0
# repeated_df$IWIT_PACKING_TABLE[is.na(repeated_df$IWIT_PACKING_TABLE)]<-0


# BU_Update_SH<- BU_update[,c("fsn","vertical","super_category","bu","Band","active_inactive","brand","QOH_SH","PO_SH","IWIT_PACKING_TABLE")]
# Sourcing_hub_raw<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Rolling Plan _ SH.xlsx",sheet="Rolling plan",col_names = T)
# Sourcing_hub_raw<-Sourcing_hub_raw[-1,]
# colnames(Sourcing_hub_raw)<-Sourcing_hub_raw[1,]
# Sourcing_hub_raw<-Sourcing_hub_raw[,c("Brand","Sourcing Hub")]
# Sourcing_hub_brands<-Sourcing_hub_brands[-1,]
# Sourcing_hub_raw<-unique(Sourcing_hub_raw)
# SH_FC<-c("pat_boh_wh_nl_01nl","bhi_pad_wh_nl_04nl","ben_hos_wh_nl_01nl","kol_mal_wh_nl_01nl")
# SH_Cluster<-c("CLUS_N_00001_NON_LARGE","CLUS_W_00001_NON_LARGE","CLUS_S_00001_NON_LARGE","CLUS_S_00001_NON_LARGE")
# SH_FC_Cluster<-data.frame(FC=SH_FC,Cluster=SH_Cluster)
# Sourcing_hub_raw<-merge(Sourcing_hub_raw,SH_FC_Cluster,by.x = c("Sourcing Hub"),by.y = c("FC"),all.x = T)
# BU_Update_SH$Brand_lowerc<- tolower(BU_Update_SH$brand)
# 
# 
# BU_Update_SH$brand<-iconv(BU_Update_SH$brand, from = "latin-1", to = "UTF-8")
# BU_Update_SH$Brand_lowerc<- tolower(iconv(BU_Update_SH$brand, from = "current", to = "UTF-8"))
# 
# 
# Sourcing_hub_raw<-Sourcing_hub_raw[,c("Brand","Cluster")]
# BU_Update_SH<-merge(BU_Update_SH,Sourcing_hub_raw,by.x=("brand"),by.y=("Brand"),all.x=T)
# 
# write.csv(BU_Update_SH,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\check_SH.csv",row.names = FALSE)



##NATIONAL#########################################################################

# repeated_df_summary_national<-repeated_df %>%
#   group_by(fsn,bu,super_category,active_inactive,brand,Flag_DI,Flag_SH_Brand) %>%
#   summarize(ATP_units = sum(ATP_units),Sch_units=sum(Sch_units),PO_units=sum(PO_units),IWIT_units=sum(IWIT_units),DRR=sum(DRR))
# 
# repeated_df_summary_national<-merge(repeated_df_summary_national,SH_base,by.x=c("fsn"),by.y=c("fsn"),all.x=T)
# 
# repeated_df_summary_national$ATPReq_1day<-repeated_df_summary_national$DRR*1
# repeated_df_summary_national$ATPavailable_1day<-pmin(repeated_df_summary_national$ATPReq_1day,repeated_df_summary_national$ATP_units)
# 
# repeated_df_summary_national$ATPReq_7day<-repeated_df_summary_national$DRR*7
# repeated_df_summary_national$ATPavailable_7day<-pmin(repeated_df_summary_national$ATPReq_7day,repeated_df_summary_national$ATP_units)
# 
# repeated_df_summary_national$ATPReq_14day<-repeated_df_summary_national$DRR*14
# repeated_df_summary_national$ATPavailable_14day<-pmin(repeated_df_summary_national$ATPReq_14day,repeated_df_summary_national$ATP_units+repeated_df_summary_national$Sch_units+repeated_df_summary_national$IWIT_units)
# 
# repeated_df_summary_national$ATPReq_21day<-repeated_df_summary_national$DRR*21
# repeated_df_summary_national$ATPavailable_21day<-pmin(repeated_df_summary_national$ATPReq_21day,repeated_df_summary_national$ATP_units+repeated_df_summary_national$PO_units+repeated_df_summary_national$IWIT_units+repeated_df_summary_national$QOH_SH+repeated_df_summary_national$PO_SH+repeated_df_summary_national$IWIT_PACKING_TABLE)
# 
# repeated_df_summary_national$Excess_Sch<-pmin(repeated_df_summary_national$Sch_units,pmax(0,repeated_df_summary_national$ATP_units+repeated_df_summary_national$Sch_units-(repeated_df_summary_national$DRR*30)))
# repeated_df_summary_national$Excess_PO<-pmin(repeated_df_summary_national$PO_units,pmax(0,repeated_df_summary_national$ATP_units+repeated_df_summary_national$PO_units-(repeated_df_summary_national$DRR*45)))
# 
# write.csv(repeated_df_summary_national,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\MSTN_1Jul_national_v1.csv",row.names = FALSE)


##Zonal#####################################################################################

repeated_df_summary_zone<-repeated_df %>%
  group_by(fsn,bu,super_category,active_inactive,Zone,brand,Flag_DI,Flag_SH_Brand) %>%
  summarize(ATP_units = sum(ATP_units),Sch_units=sum(Sch_units),PO_units=sum(PO_units),IWIT_units=sum(IWIT_units),DRR=sum(DRR))

repeated_df_summary_zone$ATPReq_1day<-repeated_df_summary_zone$DRR*1
repeated_df_summary_zone$ATPavailable_1day<-pmin(repeated_df_summary_zone$ATPReq_1day,repeated_df_summary_zone$ATP_units)

repeated_df_summary_zone$ATPReq_7day<-repeated_df_summary_zone$DRR*7
repeated_df_summary_zone$ATPavailable_7day<-pmin(repeated_df_summary_zone$ATPReq_7day,repeated_df_summary_zone$ATP_units)

repeated_df_summary_zone$ATPReq_14day<-repeated_df_summary_zone$DRR*14
repeated_df_summary_zone$ATPavailable_14day<-pmin(repeated_df_summary_zone$ATPReq_14day,repeated_df_summary_zone$ATP_units+repeated_df_summary_zone$Sch_units+repeated_df_summary_zone$IWIT_units)

repeated_df_summary_zone$ATPReq_21day<-repeated_df_summary_zone$DRR*21
repeated_df_summary_zone$ATPavailable_21day<-pmin(repeated_df_summary_zone$ATPReq_21day,repeated_df_summary_zone$ATP_units+repeated_df_summary_zone$PO_units+repeated_df_summary_zone$IWIT_units)

repeated_df_summary_zone$Excess_Sch<-pmin(repeated_df_summary_zone$Sch_units,pmax(0,repeated_df_summary_zone$ATP_units+repeated_df_summary_zone$Sch_units-(repeated_df_summary_zone$DRR*30)))
repeated_df_summary_zone$Excess_PO<-pmin(repeated_df_summary_zone$PO_units,pmax(0,repeated_df_summary_zone$ATP_units+repeated_df_summary_zone$PO_units-(repeated_df_summary_zone$DRR*45)))

###Speed fsns

repeated_df_summary_zone$instock<-ifelse(repeated_df_summary_zone$ATPReq_1day==0,ifelse(repeated_df_summary_zone$ATP_units!=0,1,0),(repeated_df_summary_zone$ATPavailable_1day/repeated_df_summary_zone$ATPReq_1day))
repeated_df_summary_zone$instockflag<-ifelse(repeated_df_summary_zone$instock>0.8,1,0)
repeated_df_summary_zone$fsn_zone <- ave(repeated_df_summary_zone$instockflag, repeated_df_summary_zone$fsn, FUN = sum)

FRU_FSN<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\FRU\\New FRU Tracker.xlsx",sheet="Current_list_on_FRU")
FRU_FSN_List<-FRU_FSN[,c("Fsn","Reasons")]
colnames(FRU_FSN_List)[1]<-"fsn"
FRU_FSN_List$FRU_Flag<-1
repeated_df_summary_zone<-merge(repeated_df_summary_zone,FRU_FSN_List,by=c("fsn"),all.x=T)
repeated_df_summary_zone$FRU_Flag[is.na(repeated_df_summary_zone$FRU_Flag)]<-0
repeated_df_summary_zone<-repeated_df_summary_zone %>% filter(repeated_df_summary_zone$FRU_Flag==1)

repeated_df_summary_zone<-repeated_df_summary_zone %>% filter(repeated_df_summary_zone$instockflag==0)

# Reshape
reshaped_df <- repeated_df_summary_zone %>%
  group_by(bu,super_category, Reasons) %>%
  summarise(total_DRR = sum(DRR)) %>%
  pivot_wider(names_from = Reasons, values_from = total_DRR, values_fill = list(total_DRR = 0))

reshaped_df$Total_DRR<-rowSums(reshaped_df[,3:19])

# Print the new data frame
write.csv(reshaped_df,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\FRU\\FRU_MSTN_3Jul_zonal_v5.csv",row.names = TRUE,fileEncoding = "UTF-8")


###CLUSTER########################################################################################

# repeated_df_summary_cluster<-repeated_df %>%
#   group_by(fsn,Cluster,bu,super_category,active_inactive,Zone,brand,City,Flag_DI,Flag_SH_Brand) %>%
#   summarize(ATP_units = sum(ATP_units),Sch_units=sum(Sch_units),PO_units=sum(PO_units),IWIT_units=sum(IWIT_units),DRR=sum(DRR))
# 
# repeated_df_summary_cluster$ATPReq_1day<-repeated_df_summary_cluster$DRR*1
# repeated_df_summary_cluster$ATPavailable_1day<-pmin(repeated_df_summary_cluster$ATPReq_1day,repeated_df_summary_cluster$ATP_units)
# 
# repeated_df_summary_cluster$ATPReq_7day<-repeated_df_summary_cluster$DRR*7
# repeated_df_summary_cluster$ATPavailable_7day<-pmin(repeated_df_summary_cluster$ATPReq_7day,repeated_df_summary_cluster$ATP_units)
# 
# repeated_df_summary_cluster$ATPReq_14day<-repeated_df_summary_cluster$DRR*14
# repeated_df_summary_cluster$ATPavailable_14day<-pmin(repeated_df_summary_cluster$ATPReq_14day,repeated_df_summary_cluster$ATP_units+repeated_df_summary_cluster$Sch_units+repeated_df_summary_cluster$IWIT_units)
# 
# repeated_df_summary_cluster$ATPReq_21day<-repeated_df_summary_cluster$DRR*21
# repeated_df_summary_cluster$ATPavailable_21day<-pmin(repeated_df_summary_cluster$ATPReq_21day,repeated_df_summary_cluster$ATP_units+repeated_df_summary_cluster$PO_units+repeated_df_summary_cluster$IWIT_units)
# 
# repeated_df_summary_cluster$Excess_Sch<-pmin(repeated_df_summary_cluster$Sch_units,pmax(0,repeated_df_summary_cluster$ATP_units+repeated_df_summary_cluster$Sch_units-(repeated_df_summary_cluster$DRR*30)))
# repeated_df_summary_cluster$Excess_PO<-pmin(repeated_df_summary_cluster$PO_units,pmax(0,repeated_df_summary_cluster$ATP_units+repeated_df_summary_cluster$PO_units-(repeated_df_summary_cluster$DRR*45)))
# 
# 
# write.csv(repeated_df_summary_cluster,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\MSTN_1Jul_cluster_v1.csv",row.names = FALSE)


###FSN########################################################################################

#write.csv(repeated_df_fsn_summary,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\MSTN_29th_fsn_v2.csv",row.names = FALSE)

###Checks
# repeated_df_check<-repeated_df[repeated_df$super_category=='Grooming',]
# write.csv(repeated_df_check,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\FSN_Check_29th.csv",row.names = TRUE)

# fc_list_summary<-ATP %>% group_by(inventory_item_warehouse_id) %>% summarize(ATP_units_atp=sum(ATP$inventory_item_atp,na.rm=T),ATP_units_qty=sum(ATP$inventory_item_quantity,na.rm=T))
# fc_list_summary <- ATP %>%
#   group_by(inventory_item_warehouse_id) %>%
#   summarize(ATP_units = sum(inventory_item_atp),ATP_unit_qty=sum(inventory_item_quantity))

# ATP_check<-ATP[ATP$product_fsn=='EDOGFYADZG4FCZX5',]

