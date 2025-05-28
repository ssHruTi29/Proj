library(stringr)
library(lubridate)
library(dplyr)
library(readxl)

###Reading input files for BU update, cluster list and cluster FC mapping
BU_update<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BU Update file\\Bu Update Input1 2024-07-22 .csv",header = T)
List_fsn_combination<- BU_update[,c("fsn","vertical","super_category","bu","Band","active_inactive","brand")]
cluster_list<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster Code_v2.xlsx",sheet="cluster",col_names = T)
cluster_fc_mapping<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster Code_v2.xlsx",sheet="cluster_fc",col_names = T)
List_fsn_combination<-cbind(List_fsn_combination)


####Filtering the SC's and Verticals not required
##Excluding SCs
exclude_sc<-c("0","BooksMedia","CoreEA","MensClothingEssentialsAndEthnic","Service","MensClothingCasualTopwear")
BU_update<-BU_update %>% filter(!(super_category %in% exclude_sc ))


###Excluding verticals
exclude_verticals<-c("PressureCooker","GasStove","Dinner Set","Cookware Set","Mop Set")
BU_update<-BU_update %>% filter(!(vertical %in% exclude_verticals ))
BU_update<-BU_update[!(BU_update$bu=="Mobile"&!grepl("^MOB",BU_update$fsn)),]

###Creating base combination - BU update(fsn) X cluster
repeated_df <- List_fsn_combination %>%
  slice(rep(1:nrow(List_fsn_combination), each = nrow(cluster_list))) %>%
  mutate(Cluster = rep(cluster_list$Cluster_list, nrow(List_fsn_combination)))  # Add id column from small_df

#Reading cluster zone mapping
Cluster_zone<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\cluster_zone.csv",header = T)
repeated_df<-merge(repeated_df,Cluster_zone,by=c("Cluster"),all.x=T)


##Cluster city mapping and DI mapping
cluster_city<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster_City.csv",header = T)
DI<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\DI List.csv",header = T)
DI<-data.frame(unique(DI[,c(1)]))
DI$Flag_DI<-1
colnames(DI)[1]<-"fsn"

repeated_df<-merge(repeated_df,cluster_city,by.x=c("Cluster"),by.y=c("Cluster_Code"),all.x=T)
repeated_df<-merge(repeated_df,DI,by.x =c("fsn"),by.y=c("fsn"),all.x=T)
repeated_df$Flag_DI[is.na(repeated_df$Flag_DI)]<-0

####ATP mapping
ATP<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\ATP\\ATP July 22.csv",header = T)
ATP<-ATP[ATP$is_first_party_seller==1,]
ATP<-merge(ATP,cluster_fc_mapping,by.x=c("inventory_item_warehouse_id"),by.y=c("FC"),all.x=T)
ATP$inventory_item_quantity<-as.numeric(ATP$inventory_item_atp)
ATP_summary <- ATP %>%
  group_by(product_fsn,Cluster) %>%
  summarize(ATP_units = sum(inventory_item_atp))

repeated_df<-merge(repeated_df,ATP_summary,by.x=c("fsn","Cluster"),by.y=c("product_fsn","Cluster"),all.x=T)


##Calculating open PO and forward scheduling

PO_and_Sch<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Sch&PO\\Total_SCH_PO\\FWD_Deep_Zone_Data_V2 2024-07-22 .csv",header = T)
PO_and_Sch$Total_Sch<- PO_and_Sch$FKI_scheduling+PO_and_Sch$RAAS_Scheduling
PO_and_Sch$Total_PO<-PO_and_Sch$FKI_open_Po+PO_and_Sch$RAAS_open_Po

PO_summary<-PO_and_Sch %>%
  group_by(fsn,Cluster_Code) %>%
  summarize(PO_units = sum(Total_PO,na.rm=T),Sch_units=sum(Total_Sch,na.rm=T))


repeated_df<-merge(repeated_df,PO_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster_Code"),all.x=T)
repeated_df$ATP_units[is.na(repeated_df$ATP_units)]<-0
repeated_df$Sch_units[is.na(repeated_df$Sch_units)]<-0
repeated_df$PO_units[is.na(repeated_df$PO_units)]<-0

#######Reading the forecast and calculating the DRR
Forecast<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Forecast\\Forecast_220724.csv",header = T)
Forecast_summary<-Forecast %>%
  group_by(fsn,fw_mapping) %>%
  summarize(Forecast_units = sum(forecast))

repeated_df<-merge(repeated_df,Forecast_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","fw_mapping"),all.x=T)
repeated_df$Forecast_units[is.na(repeated_df$Forecast_units)]<-0
no_of_days<-as.numeric(length(unique(Forecast$day)))
repeated_df$DRR<-repeated_df$Forecast_units/no_of_days

############ IWIT Calculation
IWIT<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\IWIT\\IWIT_Monthly_FSN 2024-07-22 .csv",header = T)

IWIT<-merge(IWIT,cluster_fc_mapping,by.x=c("Dest_FC"),by.y=c("FC"),all.x=T)

IWIT_summary<-IWIT %>%
  group_by(fsn,Cluster) %>%
  summarize(IWIT_units = sum(IWIT_Intransit))

repeated_df<-merge(repeated_df,IWIT_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster"),all.x=T)
repeated_df$IWIT_units[is.na(repeated_df$IWIT_units)]<-0


##########Mapping the courcing hub flag

Sourcing_hub_brands<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Rolling Plan _ SH.xlsx",sheet="FinalSHBrands",col_names = T)
Sourcing_hub_brands<-unique(Sourcing_hub_brands[,c("Brand")])
Sourcing_hub_brands$Flag_SH_Brand<-1

repeated_df<-merge(repeated_df,Sourcing_hub_brands,by.x=c("brand"),by.y=c("Brand"),all.x=T)
repeated_df$Flag_SH_Brand[is.na(repeated_df$Flag_SH_Brand)]<-0

############################### Summary at National, Zonal, Cluster Level and output #################

##NATIONAL#########################################################################

repeated_df_summary_national<-repeated_df %>%
  group_by(fsn,bu,super_category,active_inactive,brand,Flag_DI,Flag_SH_Brand) %>%
  summarize(ATP_units = sum(ATP_units),Sch_units=sum(Sch_units),PO_units=sum(PO_units),IWIT_units=sum(IWIT_units),DRR=sum(DRR))

repeated_df_summary_national<-merge(repeated_df_summary_national,SH_base,by.x=c("fsn"),by.y=c("fsn"),all.x=T)

repeated_df_summary_national$ATPReq_1day<-repeated_df_summary_national$DRR*1
repeated_df_summary_national$ATPavailable_1day<-pmin(repeated_df_summary_national$ATPReq_1day,repeated_df_summary_national$ATP_units)

repeated_df_summary_national$ATPReq_7day<-repeated_df_summary_national$DRR*7
repeated_df_summary_national$ATPavailable_7day<-pmin(repeated_df_summary_national$ATPReq_7day,repeated_df_summary_national$ATP_units)

repeated_df_summary_national$ATPReq_14day<-repeated_df_summary_national$DRR*14
repeated_df_summary_national$ATPavailable_14day<-pmin(repeated_df_summary_national$ATPReq_14day,repeated_df_summary_national$ATP_units+repeated_df_summary_national$Sch_units+repeated_df_summary_national$IWIT_units)

repeated_df_summary_national$ATPReq_21day<-repeated_df_summary_national$DRR*21
repeated_df_summary_national$ATPavailable_21day<-pmin(repeated_df_summary_national$ATPReq_21day,repeated_df_summary_national$ATP_units+repeated_df_summary_national$PO_units+repeated_df_summary_national$IWIT_units+repeated_df_summary_national$QOH_SH+repeated_df_summary_national$PO_SH+repeated_df_summary_national$IWIT_PACKING_TABLE)

repeated_df_summary_national$Excess_Sch<-pmin(repeated_df_summary_national$Sch_units,pmax(0,repeated_df_summary_national$ATP_units+repeated_df_summary_national$Sch_units-(repeated_df_summary_national$DRR*30)))
repeated_df_summary_national$Excess_PO<-pmin(repeated_df_summary_national$PO_units,pmax(0,repeated_df_summary_national$ATP_units+repeated_df_summary_national$PO_units-(repeated_df_summary_national$DRR*45)))

write.csv(repeated_df_summary_national,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\MSTN_22Jul_national_v1.csv",row.names = FALSE)


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

write.csv(repeated_df_summary_zone,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\MSTN_22Jul_zonal_v1.csv",row.names = FALSE)


###CLUSTER########################################################################################

repeated_df_summary_cluster<-repeated_df %>%
  group_by(fsn,Cluster,bu,super_category,active_inactive,Zone,brand,City,Flag_DI,Flag_SH_Brand) %>%
  summarize(ATP_units = sum(ATP_units),Sch_units=sum(Sch_units),PO_units=sum(PO_units),IWIT_units=sum(IWIT_units),DRR=sum(DRR))

repeated_df_summary_cluster$ATPReq_1day<-repeated_df_summary_cluster$DRR*1
repeated_df_summary_cluster$ATPavailable_1day<-pmin(repeated_df_summary_cluster$ATPReq_1day,repeated_df_summary_cluster$ATP_units)

repeated_df_summary_cluster$ATPReq_7day<-repeated_df_summary_cluster$DRR*7
repeated_df_summary_cluster$ATPavailable_7day<-pmin(repeated_df_summary_cluster$ATPReq_7day,repeated_df_summary_cluster$ATP_units)

repeated_df_summary_cluster$ATPReq_14day<-repeated_df_summary_cluster$DRR*14
repeated_df_summary_cluster$ATPavailable_14day<-pmin(repeated_df_summary_cluster$ATPReq_14day,repeated_df_summary_cluster$ATP_units+repeated_df_summary_cluster$Sch_units+repeated_df_summary_cluster$IWIT_units)

repeated_df_summary_cluster$ATPReq_21day<-repeated_df_summary_cluster$DRR*21
repeated_df_summary_cluster$ATPavailable_21day<-pmin(repeated_df_summary_cluster$ATPReq_21day,repeated_df_summary_cluster$ATP_units+repeated_df_summary_cluster$PO_units+repeated_df_summary_cluster$IWIT_units)

repeated_df_summary_cluster$Excess_Sch<-pmin(repeated_df_summary_cluster$Sch_units,pmax(0,repeated_df_summary_cluster$ATP_units+repeated_df_summary_cluster$Sch_units-(repeated_df_summary_cluster$DRR*30)))
repeated_df_summary_cluster$Excess_PO<-pmin(repeated_df_summary_cluster$PO_units,pmax(0,repeated_df_summary_cluster$ATP_units+repeated_df_summary_cluster$PO_units-(repeated_df_summary_cluster$DRR*45)))


write.csv(repeated_df_summary_cluster,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\MSTN_22Jul_cluster_v1.csv",row.names = FALSE)

