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
Master<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/Selection_Master.csv", bucket = gcs_bucket2)
Master[sapply(Master, is.numeric)] <- lapply(Master[sapply(Master, is.numeric)], function(x) {
  x[is.na(x)] <- 0
  return(x)
})
Master <- Master[order(-Master$City_DRR), ]

trim_spaces <- function(df) {
     df[] <- lapply(df, function(x) {
       if (is.character(x)) {
             # Trim leading, trailing, and multiple spaces
            gsub("^\\s+|\\s+$", "", gsub("\\s+", " ", x))
         } else {
             x
         }
     })
     return(df)
 }
 
Master <- trim_spaces(Master)

Master <- Master %>% distinct(City, FSN, .keep_all = TRUE)

##reading City level DRR and averaging out for new stores and cities
DRR<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/Instock_CorrectedDRR.csv",bucket = gcs_bucket)

DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)

DRR<-merge(DRR,DS,by.x = c('business_zone'),by.y = c('DS'))
DRR1 <- DRR %>%
  select(City, business_zone, fsn, DRR) %>%
  group_by(City) %>%
  mutate(unique_business_zones = n_distinct(business_zone)) %>%
  group_by(City, fsn) %>%
  summarise(DRR = sum(DRR) / first(unique_business_zones), .groups = "drop")

DRR<- DRR %>% select(City,fsn,DRR) %>% group_by(City,fsn) %>% summarise(DRR=sum(DRR))
Delhi<- DRR1 %>% filter(City == "Delhi")
Kolkata<- DRR1 %>% filter(City == "Kolkata")
Mumbai<- DRR1 %>% filter(City == "Mumbai")
##Pune<-DRR1 %>% filter(City == "Mumbai")

Lucknow<-Delhi %>% mutate(City = "Lucknow")
Jaipur<-Delhi %>% mutate(City = "Jaipur")
Guwahati<-Kolkata %>% mutate(City = "Guwahati")
Ahmedabad<-Mumbai %>% mutate(City = "Ahmedabad")


DRR2<-rbind(Lucknow,Jaipur,Guwahati,Ahmedabad)
DS_count<-DS %>% select(City,DS) %>% group_by(City) %>% summarise(DS_Count=n())
DRR2<-merge(DRR2,DS_count,by='City')
DRR2$DRR<-DRR2$DRR*DRR2$DS_Count
DRR2<-DRR2 %>% select(City,fsn,DRR)

DRR<-rbind(DRR,DRR2)

print("2")
##DRR <- DRR[order(-DRR$DRR), ]

if (!inherits(DRR, "data.frame")) {
  DRR <- as.data.frame(DRR)
}

DRR <- DRR %>% distinct(City, fsn, .keep_all = TRUE)
print("3")
Master<-merge(Master,DRR,by.x = c('City','FSN'),by.y = c('City','fsn'),all.x = TRUE)
Master$City_DRR=Master$DRR
Master <- Master[, !colnames(Master) %in% "DRR"]
Master$City_DRR[is.na(Master$City_DRR)]<-0.1

##reading SellOverrides from category
SellOverride<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/Sell_Override.csv", bucket = gcs_bucket2)

SellOverride$Start_date <- as.Date(SellOverride$Start_date, format = "%d-%m-%Y")
SellOverride$End_date <- as.Date(SellOverride$End_date, format = "%d-%m-%Y") 
SellOverride$Current_date<-Sys.Date()
SellOverride$Current_date <- as.Date(SellOverride$Current_date, format = "%Y-%m-%d") 
SellOverride$check <- ifelse(SellOverride$Current_date >= SellOverride$Start_date & SellOverride$Current_date < SellOverride$End_date, 1, 0)
SellOverride<-subset(SellOverride,check>0)
SellOverride<-SellOverride %>% select(City,FSN,Override_DRR)

Master<-merge(Master,SellOverride,by = c('City','FSN'),all.x = TRUE)
Master[is.na(Master)]<-0
Master$Final_DRR<-pmax(Master$City_DRR,Master$Override_DRR)

##taking DS level targets for MLE&LS

DS_level<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_level_Master.csv", bucket = gcs_bucket2)

DS_level$Store_DRR<-0.01

DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)

DS_level<-merge(DS_level,DS,by= c("DS","City"),all.x = TRUE)


DRR<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/Instock_CorrectedDRR.csv",bucket = gcs_bucket)

DRR<-DRR %>% select(fsn,business_zone,DRR)
DRR[is.na(DRR)]<-0
DS_count<-DS %>% select(City,DS) %>% group_by(City) %>% summarise(DS_Count=n())
Master<-merge(Master,DS_count,by='City')

Master <- Master[!is.na(Master$Final_DRR) & !is.na(Master$DS_Count), ]
Master$Final_DRR <- as.numeric(Master$Final_DRR)
Master$DS_Count <- as.numeric(Master$DS_Count)

print("oknew")
Master$Store_DRR=Master$Final_DRR/Master$DS_Count
Master<-Master %>% distinct(City,FSN,.keep_all = TRUE)

##making P1P2P3 store specific master
DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)

P1<-subset(Master,P1P2P3 %in% c('P1'))
P2<-subset(Master,P1P2P3 %in% c('P1','P2'))
P3<-subset(Master,P1P2P3 %in% c('P1','P2','P3'))

P1$P1P2P3<-"P1"
P2$P1P2P3<-"P2"
P3$P1P2P3<-"P3"

P1<-merge(P1,DS,by.x = c('City','P1P2P3'),by.y = c('City','Store Type'))
P2<-merge(P2,DS,by.x = c('City','P1P2P3'),by.y = c('City','Store Type'))
P3<-merge(P3,DS,by.x = c('City','P1P2P3'),by.y = c('City','Store Type'))

Master<-rbind(P1,P2,P3)

##Master<-merge(Master,DS,by= 'City')
##Master <-subset(Master,!(DS %in% c('bom_045_wh_hl_01')))
print("4")
Master<-distinct(Master)

##binding both DSlevel master and overall master
missing_columns <- setdiff(names(Master), names(DS_level))
for (col in missing_columns) {
  DS_level[[col]] <- NA
}

# Step 2: Align the column order of DS_level to match Master
DS_level_aligned <- DS_level[, names(Master), drop = FALSE]

# Step 3: Append rows of DS_level to Master
Master <- rbind(Master, DS_level_aligned)

Master<-merge(Master,DRR,by.x = c('DS','FSN'),by.y = c('business_zone','fsn'),all.x = TRUE)
Master[is.na(Master)]<-0

Master$DRR<-ifelse(Master$DRR==0,Master$Store_DRR,Master$DRR)

Master<-Master %>% distinct(FSN,DS,.keep_all = TRUE)

##prorating DRR to OPD
OPD<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/OPD.csv", bucket = gcs_bucket2)

date<- Sys.Date() + days(2)
week_number <- isoweek(date)
week_number<- paste0("WK", week_number)
OPD <- OPD %>% select(DS, all_of(week_number))
colnames(OPD)<-c('DS','OPD')
OPD$OPD<-ifelse(OPD$OPD<600,600,OPD$OPD)
OPD$U2O<-2.4
OPD$OPDxU20<-OPD$OPD*OPD$U2O
OPD<- OPD %>% select(DS,OPDxU20)
Master<-merge(Master,OPD,by = "DS",all.x = TRUE)
Master[is.na(Master)]<-0

Tot_DRR<-Master %>% select(DS,DRR) %>% group_by(DS) %>% summarise(Total_Store_DRR=sum(DRR))
Master<-merge(Master,Tot_DRR,by="DS",all.x = TRUE)

#####
Master$OPDxU20<-ifelse(Master$OPDxU20==0,400*3,Master$OPDxU20)

##
Master$Repl_DRR<-ifelse(Master$OPDxU20==0,Master$DRR,(Master$DRR/Master$Total_Store_DRR)*Master$OPDxU20)
Master$Repl_DRR<-ifelse(Master$Override_DRR>0,pmax(Master$Store_DRR,Master$Repl_DRR),Master$Repl_DRR)

Master$'Store Type'<-Master$P1P2P3
Master<-Master %>% select(City,DS,'DS Name',Cluster,Live,FSN,BU,SC,Analytical_Vertical,brand,Title,NOOS,P1P2P3,Reporting,City_DRR,Override_DRR,Final_DRR,DS_Count,Store_DRR,DRR,OPDxU20,Repl_DRR,'Store Type')
colnames(Master)<-c('City','DS','DS_Name','Cluster','Live','FSN','BU','SC','AV','Brand','Title','NOOS','P1P2P3','Reporting','City_DRR','Override_DRR','Final_DRR','DS_Count','Store_Fcst_DRR','Instock_DRR','OPDxU2O','OPD_Adjusted_DRR','Store_Type')

##modifying norms for nonselling FSNs
NonSelling<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/No_Sale_FSNs.csv",bucket = gcs_bucket)

NonSelling<-NonSelling %>% select(fc,fsn) 
NonSelling<-distinct(NonSelling)
NonSelling$No_Sale<-1

Master<-merge(Master,NonSelling,by.x = c('DS','FSN'),by.y = c('fc','fsn'),all.x = TRUE)
Master[is.na(Master)]<-0

Master$DRR_remark<-ifelse(Master$OPD_Adjusted_DRR<0.1,"<0.1",ifelse(Master$OPD_Adjusted_DRR<0.5,"0.1 to 0.5",ifelse(Master$OPD_Adjusted_DRR<1.000001,"0.5 to 1",">1")))

Master$DS_Units<-ifelse(Master$DRR_remark=="<0.1",1,ifelse(Master$DRR_remark=="0.1 to 0.5",2,ifelse(Master$DRR_remark=="0.5 to 1",3,0)))

##Master$DS_Units<-ifelse(Master$DRR_remark=="<0.1",2,ifelse(Master$DRR_remark=="0.1 to 0.5",4,ifelse(Master$DRR_remark=="0.5 to 1",6,0)))
##Master$DOH<-ifelse(Master$Store_Type=="Tiny" & Master$DRR_remark==">1",3,ifelse(Master$DRR_remark==">1",4,0))

Master$DOH <- ifelse(Master$City %in% c('Guwahati', 'Lucknow', 'Jaipur','Pune','Ahmedabad') & Master$DRR_remark == ">1",5,ifelse(Master$DRR_remark == ">1", 4, 0))
Master<-subset(Master,City %in% c('Bangalore','Delhi','Mumbai','Kolkata','Pune','Lucknow','Guwahati','Jaipur','Ahmedabad'))

###########Delhi downrevise
Master$DOH <- ifelse(Master$City %in% c('Lucknow','Delhi') & Master$DRR_remark == ">1",2,Master$DOH)

Master$Target_Units=round(Master$DS_Units+Master$DOH*Master$OPD_Adjusted_DRR,digits = 0)

##Master$Target_Units <- ifelse((Master$NOOS == 1 | Master$NOOS == "Y"),ifelse(Master$BU %in% c('BGM', 'Grocery'),pmax(6, Master$Target_Units),Master$Target_Units),Master$Target_Units)

SalesBand<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/City_FSN_30D_SalesBand.csv", bucket = gcs_bucket)

SalesBand<-SalesBand %>% select(City,FSN,Sales_band)
Master<-merge(Master,SalesBand,by = c('City','FSN'),all.x = TRUE)
##Master$Target_Units<-ifelse(Master$Sales_band=='Band 5',round(Master$Target_Units*1.5,0),ifelse(Master$Sales_band=='Band 4',round(Master$Target_Units*1.2,0),ifelse(Master$Sales_band=='Band 1',1,(round(Master$Target_Units,0)))))
##Master$Target_Units<-ifelse(Master$AV %in% c('ArtificialSweetener','PicklesChutneys','OtherCookingOil','SunflowerOil','Ghee','EdibleSeeds','SpicesMasala','SavouriesNamkeens','FloursSooji','GrainsMillets','FlourSooji','Condiments','Jaggery','NutsDryFruits','PastesPurees','OtherPulses','ToorDal','Rice','Salt','Sugar','SoyaProducts','Blended Oil','Olive Oil','Ground Nut Oil','Wheat Atta','Almond','Dates & Raisins','Other Nuts','Cashew','Moong','Channa','Basmati Rice','Whole Spices'),pmax(Master$Target_Units,2),Master$Target_Units)


top200<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/top200.csv", bucket = gcs_bucket2)

Master<-merge(Master,top200,by.x=c('City','FSN'),by.y=c('City','FSN'),all.x=TRUE)

Master$top200[is.na(Master$top200)]<-"N"
Master$Target_Units<-ifelse(Master$OPD_Adjusted_DRR>90,round(Master$OPD_Adjusted_DRR*3,digits=0),Master$Target_Units)
Master$Target_Units<-ifelse(Master$top200=="Y",round(Master$Target_Units*1.1,digits=0),Master$Target_Units)


Target_Override<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/Min_Target_Override.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv", overwrite = TRUE)
Target_Override<-read.csv("temp_file.csv")

Master<-merge(Master,Target_Override,by='FSN',all.x=TRUE)
Master[is.na(Master)]<-0

###################

Master <- Master %>%
  mutate(Target_Units = case_when(
    Store_Type == "P1" ~ pmax(Target_Units, P1),
    Store_Type == "P2" ~ pmax(Target_Units, P2),
    Store_Type == "P3" ~ pmax(Target_Units, P3),
    TRUE ~ Target_Units
  ))
Master <- Master[, !(names(Master) %in% c("P1", "P2","P3"))]

Master$Target_Units<-ifelse(Master$No_Sale==1,pmin(Master$Target_Units,1),Master$Target_Units)
Master$Target_Units<-ifelse(Master$top200=="Y",pmax(Master$Target_Units,10),Master$Target_Units)

Master$Target_Units <- ifelse(Master$BU %in% c('Mobile', 'CoreElectronics', 'LargeAppliances', 'EmergingElectronics','Lifestyle') & 
                                pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) <= 0.1, 1,
                              ifelse(Master$BU %in% c('Mobile', 'CoreElectronics', 'LargeAppliances', 'EmergingElectronics','Lifestyle') & 
                                       pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) <= 0.5, 3,
                                     ifelse(Master$BU %in% c('Mobile', 'CoreElectronics', 'LargeAppliances', 'EmergingElectronics','Lifestyle') & 
                                              pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) > 0.5, 
                                            ceiling(pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) * 4), 
                                            Master$Target_Units)))

Master$Target_Units <- ifelse(Master$BU %in% c('Mobile', 'CoreElectronics', 'EmergingElectronics') & Master$Target_Units == 1,2,Master$Target_Units)

# Master$Target_Units <- ifelse(Master$BU %in% c('Lifestyle') & 
#                                 pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) <= 0.1, 1,
#                               ifelse(Master$BU %in% c('Lifestyle') & 
#                                        pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) <= 0.5, 3,
#                                      ifelse(Master$BU %in% c('Lifestyle') & 
#                                               pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) > 0.5, 
#                                             ceiling(pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) * 4), 
#                                             Master$Target_Units)))

Master$Target_Units <- ifelse(Master$SC %in% c('WomenWesternGrowth','MensEssentialsEthnicBranded','MensEssentialsEthnicUnbranded'),
                              pmax(Master$Target_Units, 2),
                              Master$Target_Units)  # Ensure you're referring to the correct column


#####Overriding topical norms
#manual_norm = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1R8vApW-d_uonyLw3N6azE6mKwD51qvPglY9iyKX5hLw/edit?gid=0#gid=0',sheet='LS'))
manual_norm= data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/Target_override - LS.csv", bucket = gcs_bucket2))
manual_norm<-manual_norm[,-c("City")]
manual_norm$New_norm<-as.numeric(manual_norm$New_norm)
#temp_master<-Master
Master <- Master %>%
  left_join(manual_norm, by = c("FSN", "DS")) %>%   # Merge based on FSN & DS
  mutate(Target_Units = ifelse(!is.na(New_norm), New_norm, Target_Units)) %>%  # Replace Target if match found
  select(-New_norm)


##DS_level_excl<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_level_exclusion.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv", overwrite = TRUE)
##DS_level_excl<-read.csv("temp_file.csv")
##Master<-merge(Master,DS_level_excl,by.x=c('City','FSN','DS'),by.y=c('City','FSN','DS'),all.x=TRUE)
##Master$Remove[is.na(Master$Remove)]<-0
##Master<-subset(Master,Remove<1)
##Master <- Master[ , !(names(Master) %in% "Remove")]


##reading casepack and writing case pack target to new ouput

DS_Casepack<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_FSN_Casepack.csv", bucket = gcs_bucket2)
Master<-merge(Master,DS_Casepack,by.x = c('City','FSN'),by.y = c('City','FSN'),all.x = TRUE)
Master$Case_Pack[is.na(Master$Case_Pack)]<-1

Master$Target_Units<-pmax(round(Master$Target_Units/Master$Case_Pack,0)*Master$Case_Pack,Master$Case_Pack)

print("5")
Master<-Master %>% distinct(FSN,DS,.keep_all = TRUE)

Master <- Master %>%
  mutate(across(where(is.list), unlist))



gcs_upload(Master, name = "ipc/Nav/Hyperlocal_IPC/DS_Target_Master.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")



