library(stringr)
library(lubridate)
library(dplyr)
library(readxl)

##inputfiles
BU_update<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\BU Update file\\Bu Update Input1 2024-07-29 .csv",header = T)
cluster_fc_mapping<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster Code_v2.xlsx",sheet="cluster_fc",col_names = T)
Cluster_zone<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\cluster_zone.csv",header = T)

##Excluding SCs
exclude_sc<-c("0","BooksMedia","CoreEA","MensClothingEssentialsAndEthnic","Service","MensClothingCasualTopwear")
BU_update<-BU_update %>% filter(!(super_category %in% exclude_sc ))

###Excluding verticals
exclude_verticals<-c("PressureCooker","GasStove","Dinner Set","Cookware Set","Mop Set")
BU_update<-BU_update %>% filter(!(vertical %in% exclude_verticals ))
BU_update<-BU_update[!(BU_update$bu=="Mobile"&!grepl("^MOB",BU_update$fsn)),]
List_fsn_combination<- BU_update[,c("fsn","vertical","super_category","bu","Band","active_inactive","brand")]

######## Zones
Zones<-data.frame(c("North","South","West","East"))
colnames(Zones)[1]<-"Zones"

####Creating the set
repeated_df <- List_fsn_combination %>%
  slice(rep(1:nrow(List_fsn_combination), each = nrow(Zones))) %>%
  mutate(Zones = rep(Zones$Zones, nrow(List_fsn_combination)))  # Add id column from small_df

#####Calculating the DRR
Forecast<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Forecast\\Forecast_290724.csv",header = T)
Forecast_summary<-Forecast %>%
  group_by(fsn,fw_mapping) %>%
  summarize(Forecast_units = sum(forecast))
Forecast_summary<-merge(Forecast_summary,Cluster_zone,by.x=c("fw_mapping"),by.y=c("Cluster"),all.x=T)

Forecast_summary<-Forecast_summary %>%
  group_by(fsn,Zone) %>%
  summarize(Forecast_units = sum(Forecast_units))

repeated_df<-merge(repeated_df,Forecast_summary,by.x=c("fsn","Zones"),by.y=c("fsn","Zone"),all.x=T)
repeated_df$Forecast_units[is.na(repeated_df$Forecast_units)]<-0
no_of_days<-as.numeric(length(unique(Forecast$day)))

repeated_df$DRR<-repeated_df$Forecast_units/no_of_days

############ATP

ATP<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\ATP\\ATP July 29.csv",header = T)
ATP<-ATP[ATP$is_first_party_seller==1,]
ATP<-merge(ATP,cluster_fc_mapping,by.x=c("inventory_item_warehouse_id"),by.y=c("FC"),all.x=T)
ATP$inventory_item_quantity<-as.numeric(ATP$inventory_item_atp)
ATP_summary <- ATP %>%
  group_by(product_fsn,Zone) %>%
  summarize(ATP_units = sum(inventory_item_atp))

repeated_df<-merge(repeated_df,ATP_summary,by.x=c("fsn","Zones"),by.y=c("product_fsn","Zone"),all.x=T)
repeated_df<-repeated_df[!is.na(repeated_df$fsn),]
repeated_df$ATP_units[is.na(repeated_df$ATP_units)]<-0

#################

repeated_df$MSTN_Req<-repeated_df$DRR*7
repeated_df$Loss<-pmax(repeated_df$MSTN_Req-repeated_df$ATP_units,0)

###Forecast W-1
Forecast_Lweek<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Waterfall\\Forecast W-1\\Forecast(W-1)_290724.csv",header = T)
Forecast_Lweek<-merge(Forecast_Lweek,Cluster_zone,by.x=c("fw_mapping"),by.y=c("Cluster"),all.x=T)

Forecast_Lweek_summary<-Forecast_Lweek %>%
  group_by(fsn,Zone) %>%
  summarize(Forecast_Wminus1 = sum(forecast))

repeated_df<-merge(repeated_df,Forecast_Lweek_summary,by.x=c("fsn","Zones"),by.y=c("fsn","Zone"),all.x=T)
repeated_df$Forecast_Wminus1[is.na(repeated_df$Forecast_Wminus1)]<-0

###Sale W-1
Sale_Lweek<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Waterfall\\Sale W-1\\Sales(W-1)_290724.csv",header = T)
Sale_Lweek<-merge(Sale_Lweek,Cluster_zone,by.x=c("cluster_code"),by.y=c("Cluster"),all.x=T)

Sale_Lweek_summary<-Sale_Lweek %>%
  group_by(fsn,Zone) %>%
  summarize(Sale_Wminus1 = sum(units))

repeated_df<-merge(repeated_df,Sale_Lweek_summary,by.x=c("fsn","Zones"),by.y=c("fsn","Zone"),all.x=T)
repeated_df$Sale_Wminus1[is.na(repeated_df$Sale_Wminus1)]<-0

repeated_df$Oversell<-pmax(0,repeated_df$Sale_Wminus1-(repeated_df$Forecast_Wminus1*3))

repeated_df_temp<-repeated_df
###PO W-21 to W-7

###################Received quantity for last 21 days , eg if today is 22 July, it takes the recieved from 1st to 21st July
##FKI PO and sch data
PO_L2week<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Waterfall\\PO Qty\\BGMHE 8th July to 28th July.csv",header = T)
PO_L2week$order_date<-as.Date(PO_L2week$order_date)
PO_L2week<-merge(PO_L2week,cluster_fc_mapping,by.x=c("origin_warehouse_id"),by.y=c("FC"),all.x=T)

###RAAS PO and sch data
RAAS_PO<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Waterfall\\PO Qty\\RAAS 8th July to July 28 with created date.xlsx")
RAAS_PO$`Created Date`<-as.Date(RAAS_PO$`Created Date`)
RAAS_PO<-merge(RAAS_PO,cluster_fc_mapping,by.x=c("Warehouse"),by.y=c("FC"),all.x=T)

##Summary FKI and RAAS for recieved qty
PO_L2week_summary_rec<-PO_L2week %>%
  group_by(fsn,Zone) %>%
  summarize(Recieved_qty_FKI=sum(received_quantity))

RAAS_PO_summary_rec<-RAAS_PO %>%
  group_by(`Product Id`,Zone) %>%
  summarize(Recieved_qty_RAAS=sum(`Received Units`))

repeated_df<-merge(repeated_df,PO_L2week_summary_rec,by.x=c("fsn","Zones"),by.y=c("fsn","Zone"),all.x=T)
repeated_df<-merge(repeated_df,RAAS_PO_summary_rec,by.x=c("fsn","Zones"),by.y=c("Product Id","Zone"),all.x=T)
repeated_df$Recieved_qty_FKI[is.na(repeated_df$Recieved_qty_FKI)]<-0
repeated_df$Recieved_qty_RAAS[is.na(repeated_df$Recieved_qty_RAAS)]<-0

repeated_df$Recieved_qty<-repeated_df$Recieved_qty_RAAS+repeated_df$Recieved_qty_FKI

###################ordered quantity for W-3 & W-2,eg. if today is 22 July, it takes the quantity from 1st to 14th July, inclusive

###FKI order qty 
PO_L2week_FKI<-PO_L2week[PO_L2week$order_date<"2024-07-22",]
PO_L2week_summary_FKI<-PO_L2week_FKI %>%
  group_by(fsn,Zone) %>%
  summarize(PO_minus7to21_FKI = sum(quantity))

####RAAS order qty
RAAS_PO_orderqty<-RAAS_PO[RAAS_PO$`Created Date`<"2024-07-22",]
RAAS_PO_orderqty_summary<-RAAS_PO_orderqty %>%
  group_by(`Product Id`,Zone) %>%
  summarize(PO_minus7to21_RAAS = sum(`Ordered Quantity`))

repeated_df<-merge(repeated_df,PO_L2week_summary_FKI,by.x=c("fsn","Zones"),by.y=c("fsn","Zone"),all.x=T)
repeated_df<-merge(repeated_df,RAAS_PO_orderqty_summary,by.x=c("fsn","Zones"),by.y=c("Product Id","Zone"),all.x=T)

repeated_df$PO_minus7to21_FKI[is.na(repeated_df$PO_minus7to21_FKI)]<-0
repeated_df$PO_minus7to21_RAAS[is.na(repeated_df$PO_minus7to21_RAAS)]<-0

repeated_df$PO_minus7to21<-repeated_df$PO_minus7to21_FKI+repeated_df$PO_minus7to21_RAAS

#####Creating the required columns

repeated_df$FR<-repeated_df$PO_minus7to21-repeated_df$Recieved_qty
repeated_df$Oversell_Loss<-pmax(pmin(repeated_df$Loss,repeated_df$Oversell),0)
repeated_df$Fill_rate_loss<-pmax(pmin((repeated_df$Loss-repeated_df$Oversell_Loss),repeated_df$FR),0)
repeated_df$Planning_loss<-ifelse(repeated_df$PO_minus7to21==0,ifelse((repeated_df$Loss-repeated_df$Oversell-repeated_df$Fill_rate_loss)<0,0,(repeated_df$Loss-repeated_df$Oversell-repeated_df$Fill_rate_loss)),0)
repeated_df$Other_Loss<-pmax(0,repeated_df$Loss-repeated_df$Fill_rate_loss-repeated_df$Oversell_Loss-repeated_df$Planning_loss)
repeated_df$req_7day<-repeated_df$DRR*7
repeated_df$avl_7day<-pmin(repeated_df$ATP_units,repeated_df$req_7day)

repeated_df$Day <-"29-07-24"


write.csv(repeated_df,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Waterfall\\output_29jul_v1.csv")
