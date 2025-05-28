library(stringr)
library(lubridate)
library(dplyr)
library(readxl)

####Cluster list and cluster FC mapping
cluster_list<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster Code_v2.xlsx",sheet="cluster",col_names = T)
cluster_fc_mapping<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Cluster Code_v2.xlsx",sheet="cluster_fc",col_names = T)

###ATP
ATP<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\ATP\\ATP July 22.csv",header = T)
ATP<-ATP[ATP$is_first_party_seller==0,]
ATP<-merge(ATP,cluster_fc_mapping,by.x=c("inventory_item_warehouse_id"),by.y=c("FC"),all.x=T)
ATP$inventory_item_quantity<-as.numeric(ATP$inventory_item_atp)

ATP_summary <- ATP %>%
  group_by(product_listing_id,Cluster) %>%
  summarize(ATP_units = sum(inventory_item_atp))
#ATP_summary <-merge(ATP_summary,cluster_fc_mapping,by.x=c("inventory_item_warehouse_id"),by.y=c("FC"),all.x=T)
#ATP_summary<-ATP_summary[,-1]
ATP_combination<-ATP[,c("seller_id","product_listing_id","product_fsn","supercategory","brand","Cluster")]
new_names <- c("Seller_id","Listing_id","fsn","Super_category","Brand","Cluster")
names(ATP_combination)<-new_names

ATP_combination<-unique(ATP_combination)
ATP_summary<-ATP %>% group_by(product_listing_id,Cluster) %>% summarise(ATP=sum(inventory_item_atp))


###seller list to be change whenever addition or reduction
# P_seller_list<-data.frame(c('d98f0b7bc7b345b1','cac314cb704244f7','08ce93ba905a40a7','46d837b40bc84e15','64eedf60dc774c0d','ca1db1d1cea2485a','e7fl8bf6f4e9i68t','pj2qynwv83mclisk','dcd2f5488fee4f01','69cfa840bde14d80','2f9340771bab475e','0cadf2825c824485','dcb3b526e1d34a21','28708c1677084657','0d2597b415034de3','a2624c6b63734d12','da3261e627ae4862','5f4d3c86de8844f2','c37c12a599304bf0','d7833c8b4d674b24','6346872f92e140fd','80a51a8ef877486b','6fbbdd5a9a884aed','c24144870360486c','bb3e6fa53fe84d16','cfb1de049b7c49d0','vug5iwypdqe0cp1v','4183d0581f1c4676','df999e0f5dd44a03','162ff61fbc33444f','1b2a89de5d8a4979','d53a3fe8d9b6451c','863a60156ae541b7','96d9a8b8c6814f63','542890c6f08e4a40','554055b447b54c75','3ece50c4be88405e','0d13575c56dd4ea5','7bc1107f6f76469e','fafcb8f97b9f4534','b20ac5b11a1e4096','c7f16b312f244143','01a25adaacaa4f34','0ece4286cec24ed1','69beccafb3d6485b','fd07370d18324d9b','fa76f16df8ab4007','69f5c0ffe26f440e','b7d652a13f834f27','c138974f4c524f57','2f11d64b04694648','8f28101a753f49e2','7168d32d0bcf45d0','2825dbe9174f4c2d','345576a971e14fd5','a1ecba1139a44702','30d6f4809289418e','6858b9b539b94586','7da4198b80f64988','af97a671364c4c78','202272d818394efc','5c65a3598d0a473b','c0579915702a4ddb','c3305269e7124052','a43e653337dd4543','f9cead7ad114493e','cd931453adcd4813','7e60184bfebd4d73','lflru9kjx3r5w038','bd6e61d4de8f4d40','0cf880bb39a84ea4','74378cda04504ec3','844529c587e84a60','84ef6c63b9e74236','963a470540f640ce','235ec73333394d52','d67c699c2cd14aaa','073531e4b6ba4dff','e2cf65a808e54507','7195472ce3c04f87','9233f1146bc44c26','6850efa73ae441d8','ab23fd813d0741cc','1e732180db3d42e9','c7340913b6264f59','a7f1337807234774','62e8870e9dce4e04','aaa0087f4a444d42','0503d503b27e4d51','c0d8c18d2bc142fb','9bff017eea08412f','66bd0b545ba3468e','1e6832d1b78b4115','44bae3e5cd2f4998','aa5f2f7f69e34f9c','b9fd8f69f8a44872','1746cc365b764e9a'))
# colnames(P_seller_list)[1]<-"Seller_id"jhhjjjj
# P_seller_list$P_seller<-1
P_seller_list<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\MPFBF\\Seller__mapping_MP.xlsx",col_names = T)



###Sales_data

Sales<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\MPFBF\\Sales\\Sales_MP_150724.csv",header = T)
#no_of_days<-as.numeric(length(unique(Sales$dates)))
Sales_combination<-Sales[,c("seller_id","listing_id","product_id","analytic_super_category","brand","cluster_code")]
Sales_combination<-unique(Sales_combination)

###unique_LIDs
names(Sales_combination)<-new_names
Total_key<-rbind(ATP_combination,Sales_combination)
Total_key<-unique(Total_key)

# Total_key<-merge(Total_key,P_seller_list,by=c("Seller_id"),all.x=T)
# Total_key$P_seller[is.na(Total_key$P_seller)]<-0

BU_SC<-Sales[,c("analytic_super_category","analytic_business_unit")]
BU_SC<-unique(BU_SC)
BU_SC<-BU_SC[BU_SC$analytic_business_unit %in% c("BGM","CoreElectronics","EmergingElectronics","Home","Mobile","Lifestyle"),]

#write.csv(BU_SC,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\MPFBF\\BU_SC.csv")

Total_key<-merge(Total_key,BU_SC,by.x=c("Super_category"),by.y=c("analytic_super_category"),all.x=T)
Total_key<-Total_key[!is.na(Total_key$analytic_business_unit),]

Cluster_zone<-read.csv("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\cluster_zone.csv",header = T)
Total_key<-merge(Total_key,Cluster_zone,by=c("Cluster"),all.x=T)


#####################Sales_summary
Sales$units<-as.numeric(Sales$units)
Sales_summary<-Sales %>% group_by(listing_id,cluster_code) %>% summarise(Sales_T=sum(units,na.rm=T))
Sales_summary$DRR<-Sales_summary$Sales_T/30

Total_key<-merge(Total_key,Sales_summary,by.x=c("Listing_id","Cluster"),by.y=c("listing_id","cluster_code"),all.x=T)
Total_key<-merge(Total_key,ATP_summary,by.x=c("Listing_id","Cluster"),by.y=c("product_listing_id","Cluster"),all.x=T)

Totakl_key<-Total_key[!is.na(Total_key$Zone),]
#write.csv(Total_key,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\MPFBF\\key_check.csv")


#####################Scheduling

Sch<-read_excel("C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\MPFBF\\Scheduling_Dump_15_July'24.xlsx")
Sch<-merge(Sch,cluster_fc_mapping,by.x=c("WarehouseId"),by.y=c("FC"),all.x=T)
date <- as.Date("2024-07-15")
Sch<-Sch[Sch$Slotdate>date,]
Sch_summary<-Sch %>% group_by(listingId,Cluster) %>% summarise(Sch_qty=sum(Promised_qty))
Total_key<-merge(Total_key,Sch_summary,by.x=c("Listing_id","Cluster"),by.y=c("listingId","Cluster"),all.x=T)


######################## Recommendation data ###########################

# List all CSV files in the folder
csv_files <- list.files(path = "C:\\Users\\shruti.shahi\\Desktop\\MPFBF\\Reco_Adhearance\\Reco W29", pattern = "\\.csv$", full.names = TRUE)
#FC_list<-c("malur_bts","bhi_vas_wh_nl_01nl","ulub_bts","gur_san_wh_nl_01nl","jai_san_wh_nl_01nl","ahm_khe_wh_nl_01nl")

file_s<-0
date_list<-as.Date("")
date_pattern <- "\\b\\d{4}-\\d{2}-\\d{2}\\b"

# Loop through each CSV file and read it
for (csv_file in csv_files) {
  data <- read.csv(csv_file)
  file_size<- file.info(csv_file)
  file_size<-file_size$size/1048
  file_size<-file_size/1048
  file_s<-rbind(file_s,file_size)
  date_match <- str_match(csv_file, "(\\d{4}-\\d{2}-\\d{2})")
  # Check if a match was found
  if (!is.na(date_match[1, 2])) {
    date <- as.Date(date_match[1, 2])
  } 
  date_list<-rbind(date_list,date)
  print("Done1")
}

#Creating the data frame for the files
date_list <- as.Date(date_list, origin = "1970-01-01")
date_list<-date_list[complete.cases(date_list)]
days<-weekdays(date_list)
weeknum <- as.numeric(format(date_list, "%V"))
file_s<-file_s[-1]
Files<-data.frame(CSV_File=csv_files,Dates=date_list,Days=days,Weeknum=weeknum,File_size=file_s)

all_weeks<-unique(Files$Weeknum)

Reco_final <- data.frame(listing_id=character(0),BusinessUnit=character(0),SuperCategory=character(0),listing_band=numeric(0),Comment=character(0),seller_id=character(0),reco_units=numeric(0),week=numeric(0))

for (i in all_weeks)
{
  print(i)
  subset1=Files[Files$Weeknum==i,]
  base_files<-subset1[subset1$File_size>53,]
  addn_files<-subset1[subset1$File_size<53,]
  base_file_csv<-read.csv(base_files$CSV_File)
  #base_file_csv<-base_file_csv[base_file_csv$location_id %in% FC_list,]
  base_file_csv1<-base_file_csv[,c("fsn","seller_id","location_id","SuperCategory","BusinessUnit","recommended_qty","SupplyChainType","listing_id","service_profile","PanIndiaDOHCOmment","RecommendationComment","Comment","listing_band")]
  base_file_csv1$key<-paste(base_file_csv1$location_id,base_file_csv1$listing_id)
  addn_filenames<-addn_files$CSV_File
  add_files_data<-data.frame()
  for(j in addn_filenames)
  {
    print(j)
    addn_csv<-read.csv(j,header=T)
    #addn_csv<-addn_csv[addn_csv$location_id %in% FC_list,]
    colnames(addn_csv)[1] <- "fsn"
    addn_csv<-addn_csv[,c("fsn","seller_id","location_id","SuperCategory","BusinessUnit","recommended_qty","SupplyChainType","listing_id","service_profile","PanIndiaDOHCOmment","RecommendationComment","Comment","listing_band")]
    addn_csv$key<-paste(addn_csv$location_id,addn_csv$listing_id)
    keyinadd<-unique(addn_csv$key)
    base_file_csv1<- base_file_csv1[!(base_file_csv1$key %in% keyinadd), ]
    base_file_csv1<-rbind(base_file_csv1,addn_csv)
  }
  
  base_file_csv1$recommended_qty<-as.numeric(base_file_csv1$recommended_qty)
}

Reco<-merge(base_file_csv1,cluster_fc_mapping,by.x=c("location_id"),by.y=c("FC"),all.x=T)
Reco_summary<-Reco %>% group_by(listing_id,Cluster) %>% summarise(Reco=sum(recommended_qty))
Total_key<-merge(Total_key,Reco_summary,by.x=c("Listing_id","Cluster"),by.y=c("listing_id","Cluster"),all.x=T)

###Cleaning_Data
Total_key<-Total_key[Total_key$Listing_id!="null",]
Total_key<- Total_key %>% filter(!all(is.na(.)))

Total_key$Sales_T[is.na(Total_key$Sales_T)]<-0
Total_key$DRR[is.na(Total_key$DRR)]<-0
Total_key$ATP[is.na(Total_key$ATP)]<-0
Total_key$Sch_qty[is.na(Total_key$Sch_qty)]<-0
Total_key$Reco[is.na(Total_key$Reco)]<-0

Total_key<-Total_key[!is.na(Total_key$Zone),]

##NATIONAL#########################################################################

Total_key_summary_national<-Total_key %>%
  group_by(Listing_id,fsn,analytic_business_unit,Super_category,Seller_id,Brand) %>%
  summarize(ATP_units = sum(ATP),Sch_units=sum(Sch_qty),Reco_units=sum(Reco),DRR=sum(DRR))

Total_key_summary_national<-merge(Total_key_summary_national,P_seller_list,by.x=c("Seller_id"),by.y=c("Seller id"),all.x=T)
Total_key_summary_national$P_seller[is.na(Total_key_summary_national$P_seller)]<-"Others"

#Total_key_summary_national<-merge(Total_key_summary_national,SH_base,by.x=c("fsn"),by.y=c("fsn"),all.x=T)

Total_key_summary_national$ATPReq_1day<-Total_key_summary_national$DRR*1
Total_key_summary_national$ATPavailable_1day<-pmin(Total_key_summary_national$ATPReq_1day,Total_key_summary_national$ATP_units)

Total_key_summary_national$ATPReq_7day<-Total_key_summary_national$DRR*7
Total_key_summary_national$ATPavailable_7day<-pmin(Total_key_summary_national$ATPReq_7day,Total_key_summary_national$ATP_units)

Total_key_summary_national$ATPReq_14day<-Total_key_summary_national$DRR*14
Total_key_summary_national$ATPavailable_14day<-pmin(Total_key_summary_national$ATPReq_14day,Total_key_summary_national$ATP_units+Total_key_summary_national$Sch_units)

Total_key_summary_national$ATPReq_21day<-Total_key_summary_national$DRR*21
Total_key_summary_national$ATPavailable_21day<-pmin(Total_key_summary_national$ATPReq_21day,Total_key_summary_national$ATP_units+Total_key_summary_national$Sch_units+Total_key_summary_national$Reco_units)

Total_key_summary_national$Excess_Sch<-pmin(Total_key_summary_national$Sch_units,pmax(0,Total_key_summary_national$ATP_units+Total_key_summary_national$Sch_units-(Total_key_summary_national$DRR*30)))
#Total_key_summary_national$Excess_PO<-pmin(Total_key_summary_national$PO_units,pmax(0,Total_key_summary_national$ATP_units+Total_key_summary_national$PO_units-(Total_key_summary_national$DRR*45)))

write.csv(Total_key_summary_national,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\MP-MSTN_15Jul_national_v2.csv",row.names = FALSE)


##Zonal#####################################################################################

##############3fsn level instock
###############################################################################################3

Total_key_summary_zone_fsn<-Total_key %>%
  group_by(fsn,analytic_business_unit,Super_category,Seller_id,Brand,Zone) %>%
  summarize(ATP_units = sum(ATP),Sch_units=sum(Sch_qty),Reco_units=sum(Reco),DRR=sum(DRR))
Total_key_summary_zone_fsn$key<-paste(Total_key_summary_zone_fsn$fsn,Total_key_summary_zone_fsn$Seller_id)

Total_key_summary_zone_fsn<-merge(Total_key_summary_zone_fsn,P_seller_list,by.x=c("Seller_id"),by.y=c("Seller id"),all.x=T)
Total_key_summary_zone_fsn$P_seller[is.na(Total_key_summary_zone_fsn$P_seller)]<-"Others"


Total_key_summary_zone_fsn$ATPReq_1day<-Total_key_summary_zone_fsn$DRR*1
Total_key_summary_zone_fsn$ATPavailable_1day<-pmin(Total_key_summary_zone_fsn$ATPReq_1day,Total_key_summary_zone_fsn$ATP_units)

Total_key_summary_zone_fsn$instock<-ifelse(Total_key_summary_zone_fsn$ATPReq_1day==0,ifelse(Total_key_summary_zone_fsn$ATP_units!=0,1,0),(Total_key_summary_zone_fsn$ATPavailable_1day/Total_key_summary_zone_fsn$ATPReq_1day))
Total_key_summary_zone_fsn$instockflag<-ifelse(Total_key_summary_zone_fsn$instock>0.8,1,0)
Total_key_summary_zone_fsn$fsn_zone <- ave(Total_key_summary_zone_fsn$instockflag, Total_key_summary_zone_fsn$key, FUN = sum)

FSNcount_flag<-Total_key_summary_zone_fsn[,c("Seller_id","fsn","fsn_zone")]
FSNcount_flag<-unique(FSNcount_flag)
############################################################
#############################################################3

Total_key_summary_zone<-Total_key %>%
  group_by(Listing_id,fsn,analytic_business_unit,Super_category,Seller_id,Brand,Zone) %>%
  summarize(ATP_units = sum(ATP),Sch_units=sum(Sch_qty),Reco_units=sum(Reco),DRR=sum(DRR))

Total_key_summary_zone<-merge(Total_key_summary_zone,P_seller_list,by.x=c("Seller_id"),by.y=c("Seller id"),all.x=T)
Total_key_summary_zone$P_seller[is.na(Total_key_summary_zone$P_seller)]<-"Others"

Total_key_summary_zone$ATPReq_1day<-Total_key_summary_zone$DRR*1
Total_key_summary_zone$ATPavailable_1day<-pmin(Total_key_summary_zone$ATPReq_1day,Total_key_summary_zone$ATP_units)

Total_key_summary_zone$ATPReq_7day<-Total_key_summary_zone$DRR*7
Total_key_summary_zone$ATPavailable_7day<-pmin(Total_key_summary_zone$ATPReq_7day,Total_key_summary_zone$ATP_units)

Total_key_summary_zone$ATPReq_14day<-Total_key_summary_zone$DRR*14
Total_key_summary_zone$ATPavailable_14day<-pmin(Total_key_summary_zone$ATPReq_14day,Total_key_summary_zone$ATP_units+Total_key_summary_zone$Sch_units)

Total_key_summary_zone$ATPReq_21day<-Total_key_summary_zone$DRR*21
Total_key_summary_zone$ATPavailable_21day<-pmin(Total_key_summary_zone$ATPReq_21day,Total_key_summary_zone$ATP_units+Total_key_summary_zone$Sch_units+Total_key_summary_zone$Reco_units)

Total_key_summary_zone$Excess_Sch<-pmin(Total_key_summary_zone$Sch_units,pmax(0,Total_key_summary_zone$ATP_units+Total_key_summary_zone$Sch_units-(Total_key_summary_zone$DRR*30)))
#Total_key_summary_zone$Excess_PO<-pmin(Total_key_summary_zone$PO_units,pmax(0,Total_key_summary_zone$ATP_units+Total_key_summary_zone$PO_units-(Total_key_summary_zone$DRR*45)))

###Speed fsns

# Total_key_summary_zone$instock<-ifelse(Total_key_summary_zone$ATPReq_1day==0,ifelse(Total_key_summary_zone$ATP_units!=0,1,0),(Total_key_summary_zone$ATPavailable_1day/Total_key_summary_zone$ATPReq_1day))
# Total_key_summary_zone$instockflag<-ifelse(Total_key_summary_zone$instock>0.8,1,0)
# Total_key_summary_zone$fsn_zone <- ave(Total_key_summary_zone$instockflag, Total_key_summary_zone$fsn, FUN = sum)
T1<-Total_key_summary_zone

Total_key_summary_zone<-merge(Total_key_summary_zone,FSNcount_flag,by=c("Seller_id","fsn"),all.x=T)


write.csv(Total_key_summary_zone,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\MP-MSTN_15Jul_zonal_v2.csv",row.names = FALSE)


###CLUSTER########################################################################################

# Total_key_summary_cluster<-Total_key %>%
#   group_by(Listing_id,analytic_business_unit,Super_category,Seller_id,Brand,Vertical,Zone,Cluster) %>%
#   summarize(ATP_units = sum(ATP),Sch_units=sum(Sch_qty),Reco_units=sum(Reco),DRR=sum(DRR))
# 
# Total_key_summary_cluster<-merge(Total_key_summary_cluster,P_seller_list,by.x=c("Seller_id"),by.y=c("Seller_id"),all.x=T)
# Total_key_summary_cluster$P_seller[is.na(Total_key_summary_cluster$P_seller)]<-0
# 
# Total_key_summary_cluster$ATPReq_1day<-Total_key_summary_cluster$DRR*1
# Total_key_summary_cluster$ATPavailable_1day<-pmin(Total_key_summary_cluster$ATPReq_1day,Total_key_summary_cluster$ATP_units)
# 
# Total_key_summary_cluster$ATPReq_7day<-Total_key_summary_cluster$DRR*7
# Total_key_summary_cluster$ATPavailable_7day<-pmin(Total_key_summary_cluster$ATPReq_7day,Total_key_summary_cluster$ATP_units)
# 
# Total_key_summary_cluster$ATPReq_14day<-Total_key_summary_cluster$DRR*14
# Total_key_summary_cluster$ATPavailable_14day<-pmin(Total_key_summary_cluster$ATPReq_14day,Total_key_summary_cluster$ATP_units+Total_key_summary_cluster$Sch_units)
# 
# Total_key_summary_cluster$ATPReq_21day<-Total_key_summary_cluster$DRR*21
# Total_key_summary_cluster$ATPavailable_21day<-pmin(Total_key_summary_cluster$ATPReq_21day,Total_key_summary_cluster$ATP_units+Total_key_summary_cluster$Sch_units+Total_key_summary_cluster$Reco_units)
# 
# Total_key_summary_cluster$Excess_Sch<-pmin(Total_key_summary_cluster$Sch_units,pmax(0,Total_key_summary_cluster$ATP_units+Total_key_summary_cluster$Sch_units-(Total_key_summary_cluster$DRR*30)))
# #Total_key_summary_cluster$Excess_PO<-pmin(Total_key_summary_cluster$PO_units,pmax(0,Total_key_summary_cluster$ATP_units+Total_key_summary_cluster$PO_units-(Total_key_summary_cluster$DRR*45)))
# 
# 
# write.csv(Total_key_summary_cluster,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\MP-MSTN_2Jul_cluster_v3.csv",row.names = FALSE)




