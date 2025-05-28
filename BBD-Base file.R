library(stringr)
library(lubridate)
library(dplyr)
library(readxl)

BU_update1<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BU Update file\\Bu Update Input1 2024-07-24 .csv",header = T)

BU_update<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BBD\\EE_Base.csv",header = T)
List_fsn_combination<- BU_update[,c("fsn","vertical","super_category","bu","Band","active_inactive","brand")]
cluster_list<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster Code_v4.xlsx",sheet="cluster",col_names = T)
cluster_fc_mapping<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster Code_v4.xlsx",sheet="cluster_fc",col_names = T)
List_fsn_combination<-cbind(List_fsn_combination)

##Filtering
##Excluding SCs
exclude_sc<-c("0","BooksMedia","CoreEA","MensClothingEssentialsAndEthnic","Service","MensClothingCasualTopwear")
BU_update<-BU_update %>% filter(!(super_category %in% exclude_sc ))


###Excluding verticals
exclude_verticals<-c("PressureCooker","GasStove","Dinner Set","Cookware Set","Mop Set")
BU_update<-BU_update %>% filter(!(vertical %in% exclude_verticals ))
BU_update<-BU_update[!(BU_update$bu=="Mobile"&!grepl("^MOB",BU_update$fsn)),]

##Creating the base - fsn X cluster
repeated_df <- List_fsn_combination %>%
  slice(rep(1:nrow(List_fsn_combination), each = nrow(cluster_list))) %>%
  mutate(Cluster = rep(cluster_list$Cluster_list, nrow(List_fsn_combination))) 

Cluster_zone<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\cluster_zone.csv",header = T)
repeated_df<-merge(repeated_df,Cluster_zone,by=c("Cluster"),all.x=T)


###ATP
ATP<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\ATP\\ATP July 24.csv",header = T)
ATP<-ATP[ATP$is_first_party_seller==1,]
ATP<-merge(ATP,cluster_fc_mapping,by.x=c("inventory_item_warehouse_id"),by.y=c("FC"),all.x=T)
ATP$inventory_item_quantity<-as.numeric(ATP$inventory_item_atp)
ATP_summary <- ATP %>%
  group_by(product_fsn,Cluster) %>%
  summarize(ATP_units = sum(inventory_item_atp))
#ATP_summary <-merge(ATP_summary,cluster_fc_mapping,by.x=c("inventory_item_warehouse_id"),by.y=c("FC"),all.x=T)
#ATP_summary<-ATP_summary[,-1]

repeated_df<-merge(repeated_df,ATP_summary,by.x=c("fsn","Cluster"),by.y=c("product_fsn","Cluster"),all.x=T)

##Open PO and total scheduling

PO_and_Sch<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Sch&PO\\Total_SCH_PO\\FWD_Deep_Zone_Data_V2 2024-07-24 .csv",header = T)
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


##7 Day total scheduling

Sch_7days<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BBD\\Consignment N7D 24 July 2024.csv",header = T)
Sch_7_summary<-Sch_7days %>%
  group_by(fsn,Cluster.Code) %>%
  summarize(day_7_sch_units = sum(sch_qty))

repeated_df<-merge(repeated_df,Sch_7_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster.Code"),all.x=T)
#repeated_df<-merge(repeated_df,PO_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster"),all.x=T)
repeated_df$day_7_sch_units[is.na(repeated_df$day_7_sch_units)]<-0


##########IWIT_IN
IWIT<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\IWIT\\IWIT_Monthly_FSN 2024-07-24 .csv",header = T)
Cluster_FC_mapping_iwit<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BBD\\Cluster_FC_mapping.xlsx")

FC_to_filter<-Cluster_FC_mapping_iwit$FC

IWIT1<-IWIT %>% filter(Dest_FC %in% FC_to_filter&Src_FC %in%FC_to_filter )

IWIT_in<-merge(IWIT1,Cluster_FC_mapping_iwit,by.x=c("Dest_FC"),by.y=c("FC"),all.x=T)
IWIT_summary_in<-IWIT_in %>%
  group_by(fsn,Cluster) %>%
  summarize(IWIT_units_in = sum(IWIT_Intransit))

repeated_df<-merge(repeated_df,IWIT_summary_in,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster"),all.x=T)
repeated_df$IWIT_units_in[is.na(repeated_df$IWIT_units_in)]<-0

##########IWIT_OUT
###############FKI+Fwd

#Cluster_FC_mapping_iwit<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BBD\\Cluster_FC_mapping.xlsx")

#FC_to_filter<-Cluster_FC_mapping$FC

IWIT2<-IWIT %>% filter(Dest_FC %in% FC_to_filter&Src_FC %in%FC_to_filter )

IWIT_out<-merge(IWIT2,Cluster_FC_mapping_iwit,by.x=c("Src_FC"),by.y=c("FC"),all.x=T)
IWIT_summary_out<-IWIT_out %>%
  group_by(fsn,Cluster) %>%
  summarize(IWIT_units_out = sum(IWIT_Intransit))

repeated_df<-merge(repeated_df,IWIT_summary_out,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster"),all.x=T)
repeated_df$IWIT_units_out[is.na(repeated_df$IWIT_units_out)]<-0


####DRR_Sale_Data
Sale<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BBD\\Sale_last_month_180724.csv",header = T)
Sale_summary<-Sale %>%
  group_by(fsn,cluster_code) %>%
  summarize(Sale_units = sum(units))

##cluster_split
Sale_clustersplit<-Sale_summary[Sale_summary$cluster_code!="null",]

summary <- aggregate(Sale_units ~ fsn, Sale_clustersplit, FUN = sum)
Sale_clustersplit$fsn_units <- summary[match(Sale_clustersplit$fsn, summary$fsn), "Sale_units"]
Sale_clustersplit$Cluster_split<-Sale_clustersplit$Sale_units/Sale_clustersplit$fsn_units
#write.csv(Sale_clustersplit,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BBD\\cluster_split.csv")
Sale_clustersplit<-Sale_clustersplit[,c("fsn","cluster_code","Cluster_split")]

repeated_df<-merge(repeated_df,Sale_summary,by.x=c("fsn","Cluster"),by.y=c("fsn","cluster_code"),all.x=T)
repeated_df$Sale_units[is.na(repeated_df$Sale_units)]<-0
no_of_days<-30
repeated_df$DRR<-repeated_df$Sale_units/no_of_days

####Sourcing hub data
##QOH and Sch
SH_data<-BU_update1[,c("fsn","QOH_SH","ATP_SH","PO_SH","SCH_SH")]
repeated_df<-merge(repeated_df,Sale_clustersplit,by.x=c("fsn","Cluster"),by.y=c("fsn","cluster_code"),all.x=T)

repeated_df$Cluster_split[is.na(repeated_df$Cluster_split)]<-0
SH_temp<-repeated_df[,c("fsn","Cluster","Cluster_split")]
SH_temp<-merge(SH_temp,SH_data,by="fsn",all.x=T)
SH_temp$SH_PO<-SH_temp$PO_SH*SH_temp$Cluster_split
SH_temp$SH_Sch<-SH_temp$SCH_SH*SH_temp$Cluster_split
SH_temp$SH_QOH<-SH_temp$QOH_SH*SH_temp$Cluster_split
SH_temp1<-SH_temp[,c("fsn","Cluster","SH_QOH","SH_Sch","SH_PO")]

repeated_df<-repeated_df<-merge(repeated_df,SH_temp1,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster"),all.x=T)

##############IWIT SH
IWIT_SH<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BBD\\SH STNs - All Sourcing Hubs - STN FSN Level_240724.csv",header = T)
IWIT_SH<-merge(IWIT_SH,cluster_fc_mapping,by.x=c("Dest_FC"),by.y=c("FC"),all.x=T)

IWIT_SH_summary_out<-IWIT_SH %>%
  group_by(fsn,Cluster) %>%
  summarize(IWIT_units_SH = sum(IWIT_Intransit))

repeated_df<-merge(repeated_df,IWIT_SH_summary_out,by.x=c("fsn","Cluster"),by.y=c("fsn","Cluster"),all.x=T)
repeated_df$IWIT_units_SH[is.na(repeated_df$IWIT_units_SH)]<-0

repeated_df$SH_QOH[is.na(repeated_df$SH_QOH)]<-0
repeated_df$SH_Sch[is.na(repeated_df$SH_Sch)]<-0
repeated_df$SH_PO[is.na(repeated_df$SH_PO)]<-0

###BBD opening and SP

OpeningandSP<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BBD\\MSTN'24 Dashboard Req - Target and SP format.csv",header = T)
OpeningandSP<-OpeningandSP[,c("fsn","Opening_Inv","SP")]
repeated_df<-merge(repeated_df,OpeningandSP,by=c("fsn"),all.x=T)
repeated_df$BBD_opening<-repeated_df$Opening_Inv*repeated_df$Cluster_split

repeated_df$unsch<-pmax(repeated_df$PO_units-repeated_df$Sch_units,0)
###########Calculated_columns
##BBD startdate- today
BBD_days<-9 

##7days
repeated_df$Invposition_7<-pmax(repeated_df$ATP_units+repeated_df$day_7_sch_units-(repeated_df$DRR*7),0)
repeated_df$Final_Invposition<-pmax(repeated_df$ATP_units+repeated_df$unsch+repeated_df$Sch_units-(repeated_df$DRR*BBD_days),0)
repeated_df$ATP_Target_min<-pmin(repeated_df$ATP_units,repeated_df$BBD_opening)
repeated_df$ATP_target_loss<-pmax(0,repeated_df$BBD_opening-repeated_df$ATP_Target_min)
repeated_df$Invpos7_Target_min<-pmin(repeated_df$BBD_opening,repeated_df$Invposition_7)
repeated_df$Invpos7_Target_loss<-pmax(0,repeated_df$BBD_opening-repeated_df$Invpos7_Target_min)
repeated_df$Final_Invpos_Target_min<-pmin(repeated_df$BBD_opening,repeated_df$Final_Invposition)
repeated_df$Final_Invpos_Target_loss<-pmax(0,repeated_df$BBD_opening-repeated_df$Final_Invpos_Target_min)
repeated_df$day_14drr_pos_Target<-pmin(repeated_df$BBD_opening,pmax(repeated_df$ATP_units+repeated_df$Sch_units-(repeated_df$DRR*14),0))
repeated_df$day_14drr_pos_Target_loss<-pmax(0,repeated_df$BBD_opening-repeated_df$day_14drr_pos_Target)

#14days
repeated_df$Invposition_14<-pmax(repeated_df$ATP_units+repeated_df$Sch_units-(repeated_df$DRR*14),0)
repeated_df$min_ATP_sch_sale_target<-pmin(pmax(repeated_df$ATP_units+repeated_df$Sch_units-(repeated_df$DRR*BBD_days),0),repeated_df$BBD_opening)
repeated_df$min_ATP_sch_sale_target_loss<-pmax(0,repeated_df$BBD_opening-repeated_df$min_ATP_sch_sale_target)
repeated_df$inv_pos_sch<-pmax(repeated_df$ATP_units+repeated_df$Sch_units-(repeated_df$DRR*BBD_days),0)
#repeated_df$inv_pos_sch<-pmax(repeated_df$ATP_units+repeated_df$Sch_units-(repeated_df$DRR*BBD_days),0)
repeated_df$SH_min_ATP_sch_sale_target<-pmin(repeated_df$BBD_opening,pmax(repeated_df$ATP_units+repeated_df$Sch_units+repeated_df$PO_units-(repeated_df$DRR*BBD_days)+repeated_df$SH_QOH,0))
repeated_df$SH_min_ATP_sch_sale_target_po<-pmin(repeated_df$BBD_opening,pmax(repeated_df$ATP_units+repeated_df$Sch_units+repeated_df$PO_units-(repeated_df$DRR*BBD_days)+repeated_df$SH_QOH+repeated_df$SH_PO,0))

colnames(repeated_df)<-c("fsn","Cluster","vertical","super_category","bu","Band","active_inactive","brand",
                         "Zone","ATP_units","PO_units","Sch_units","day_7_sch_units","IWIT_units_in","IWIT_units_out",
                         "Sale_units","DRR","Cluster_split","SH_QOH","SH_Sch","SH_PO","IWIT_units_SH","Opening_Inv","SP","BBD_opening","unsch",
                         "InvPosition_7","Final_InvPosition","Min (ATP,Target)","Loss to Target(ATP)","Min (ATP+Sch7-Sales7,Target)",
                         "Loss to Target(ATP+Sch7)","Min (ATP+Sch+Unsch-Sales,Target)","Loss to Target(ATP+Sch+Unsch)",
                         "Min (ATP+Sch14-Sales14,Target)","Loss to Target(ATP+Sch14)","Inventory Position_14",
                         "Min (ATP+Sch-Sales,Target)","Loss to Target(ATP+Sch)","InvPosition_Sch_SCH","Min (ATP+Sch+Unsch-Sales + SH QOH,Target)",
                         "Min (ATP+Sch+Unsch-Sales + SH QOH + SH PO,Target)"
                         )

##National
write.csv(repeated_df,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BBD\\test_output_v5.csv")











































