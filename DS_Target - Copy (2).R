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
Master<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/Selection_Master.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
Master<-read.csv("temp_file.csv")
rm(temp_file.csv)

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
DRR<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/Instock_CorrectedDRR.csv",bucket = gcs_bucket,saveToDisk = "temp_file.csv",overwrite = TRUE)
DRR<-read.csv("temp_file.csv")
rm(temp_file.csv)

DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
DS<-read.csv("temp_file.csv")
rm(temp_file.csv)

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
Bangalore<-DRR1 %>% filter(City == "Bangalore")


##Override DRR for new City new Stores###########################
##Pune<-DRR1 %>% filter(City == "Mumbai")
##Lucknow<-Delhi %>% mutate(City = "Lucknow")
##Jaipur<-Delhi %>% mutate(City = "Jaipur")
##Guwahati<-Kolkata %>% mutate(City = "Guwahati")
##Ahmedabad<-Mumbai %>% mutate(City = "Ahmedabad")
Chennai<-Bangalore %>% mutate(City = "Chennai")
Patna<-Kolkata %>% mutate(City = "Patna")

DRR2<-Chennai
DS_count<-DS %>% select(City,DS) %>% group_by(City) %>% summarise(DS_Count=n())
DRR2<-merge(DRR2,DS_count,by='City')
DRR2$DRR<-DRR2$DRR*DRR2$DS_Count
DRR2<-DRR2 %>% select(City,fsn,DRR)

DRR3<-Patna
DRR3<-merge(DRR3,DS_count,by='City')
DRR3$DRR<-DRR3$DRR*DRR3$DS_Count
DRR3<-DRR3 %>% select(City,fsn,DRR)
DRR<-rbind(DRR,DRR2,DRR3)

##DRR <- DRR[order(-DRR$DRR), ]

if (!inherits(DRR, "data.frame")) {
  DRR <- as.data.frame(DRR)
}

DRR <- DRR %>% distinct(City, fsn, .keep_all = TRUE)

###################################################################
Master<-merge(Master,DRR,by.x = c('City','FSN'),by.y = c('City','fsn'),all.x = TRUE)
Master$City_DRR=Master$DRR
Master <- Master[, !colnames(Master) %in% "DRR"]
Master$City_DRR[is.na(Master$City_DRR)]<-0.01

rm(DRR1)
##rm(DRR2)
rm(DRR)
##reading SellOverrides from category##############################################
SellOverride<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/Sell_Override.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
SellOverride<-read.csv("temp_file.csv")
rm(temp_file.csv)

SellOverride$Start_date <- as.Date(SellOverride$Start_date, format = "%d-%m-%Y")
SellOverride$End_date <- as.Date(SellOverride$End_date, format = "%d-%m-%Y") 
SellOverride$Current_date<-Sys.Date()
SellOverride$Current_date <- as.Date(SellOverride$Current_date, format = "%Y-%m-%d") 
SellOverride$check <- ifelse(SellOverride$Current_date >= SellOverride$Start_date & SellOverride$Current_date < SellOverride$End_date, 1, 0)
SellOverride<-subset(SellOverride,check>0)
SellOverride<-SellOverride %>% select(City,FSN,Override_DRR)

Master<-merge(Master,SellOverride,by = c('City','FSN'),all.x = TRUE)
Master[is.na(Master)]<-0
rm(SellOverride)
Master$Final_DRR<-pmax(Master$City_DRR,Master$Override_DRR)
###################################################################################

##taking DS level targets for MLE&LS#####

DS_level<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_level_Master.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
DS_level<-read.csv("temp_file.csv")
rm(temp_file.csv)
DS_level$Store_DRR<-0.01
DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
DS<-read.csv("temp_file.csv")
rm(temp_file.csv)
DS_level<-merge(DS_level,DS,by= c("DS","City"),all.x = TRUE)
DRR<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/Instock_CorrectedDRR.csv",bucket = gcs_bucket,saveToDisk = "temp_file.csv",overwrite = TRUE)
DRR<-read.csv("temp_file.csv")
rm(temp_file.csv)
DRR<-DRR %>% select(fsn,business_zone,DRR)
DRR[is.na(DRR)]<-0
DS_count<-DS %>% select(City,DS) %>% group_by(City) %>% summarise(DS_Count=n())
Master<-merge(Master,DS_count,by='City')

Master <- Master[!is.na(Master$Final_DRR) & !is.na(Master$DS_Count), ]
Master$Final_DRR <- as.numeric(Master$Final_DRR)
Master$DS_Count <- as.numeric(Master$DS_Count)
Master$Store_DRR=Master$Final_DRR/Master$DS_Count
Master<-Master %>% distinct(City,FSN,.keep_all = TRUE)
######################################################################################
##making P1P2P3 store specific master###
DS<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
DS<-read.csv("temp_file.csv")
rm(temp_file.csv)

P1<-subset(Master,P1P2P3 %in% c('P1'))
P2<-subset(Master,P1P2P3 %in% c('P1','P2'))
P3<-subset(Master,P1P2P3 %in% c('P1','P2','P3'))
Premium<-subset(Master,P1P2P3 %in% c('Premium'))

P1$P1P2P3<-"P1"
P2$P1P2P3<-"P2"
P3$P1P2P3<-"P3"
Premium$P1P2P3<-"Premium"
print("ok")
P1<-merge(P1,DS,by.x = c('City','P1P2P3'),by.y = c('City','Store.Type'))
P2<-merge(P2,DS,by.x = c('City','P1P2P3'),by.y = c('City','Store.Type'))
P3<-merge(P3,DS,by.x = c('City','P1P2P3'),by.y = c('City','Store.Type'))
Premium<-merge(Premium,DS,by.x = c('City','P1P2P3'),by.y = c('City','Premium'))

common_cols <- Reduce(intersect, list(colnames(P1), colnames(P2), colnames(P3), colnames(Premium)))
P1 <- P1[, common_cols, drop = FALSE]
P2 <- P2[, common_cols, drop = FALSE]
P3 <- P3[, common_cols, drop = FALSE]
Premium <- Premium[, common_cols, drop = FALSE]
Master <- rbind(P1, P2, P3, Premium)

Master<-distinct(Master)
###########################################################################################
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
rm(DRR)
Master$DRR<-ifelse(Master$DRR==0,Master$Store_DRR,Master$DRR)

Master<-Master %>% distinct(FSN,DS,.keep_all = TRUE)

##prorating DRR to OPD#################################################
OPD<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/OPD.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
OPD<-read.csv("temp_file.csv")
rm(temp_file.csv)
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

Master$OPDxU20<-ifelse(Master$OPDxU20==0,400*3,Master$OPDxU20)
Master$Repl_DRR<-ifelse(Master$OPDxU20==0,Master$DRR,(Master$DRR/Master$Total_Store_DRR)*Master$OPDxU20)
Master$Repl_DRR<-ifelse(Master$Override_DRR>0,pmax(Master$Store_DRR,Master$Repl_DRR),Master$Repl_DRR)
Master$'Store Type'<-Master$P1P2P3
##########################################################################

##remove insecticide vertical & Cigrettes#########################
Inclusion<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/HL_FSN_Inclusion_Insecticide.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
Inclusion<-read.csv("temp_file.csv")
Master<-merge(Master,Inclusion,by.x=c('FSN'),by.y=c('FSN'),all.x=TRUE)
Master$Include[is.na(Master$Include)]<-"N"
Master <- Master %>%
  filter(!(Analytical_Vertical %in% c("InsectRepellent", "MosquitoVaporiser", "MosquitoCoil", "MosquitoVaporiserRefill","MedicineTreatment","Cigarettes") &
           Insecticide_License == "N" & Include != "Y"))


##removing non licensed verticals for pharma##########################
Master <- Master %>%
  filter(!(Analytical_Vertical %in% c('Allopathy','RxMedicine','MedicalEquipmentAndAccessories','PregnancyKit','FertilityKit','HealthCareApplianceCombo','GlucometerStrips','HearingAid','RespiratoryExerciser','DigitalThermometer','BloodPressureMonitor','GlucometerLancet','Glucometer','Nebulizer','ContactLenses','MedicineTreatment','HealthCareDevice','PulseOximeter','HealthCareAccessory','Sterilisation&InfectionPrevention','HealthTestKit','LensSolution','Vaporizer','HeatingPad') & Include != "Y"))



########################################################################################################################################################

Master<-Master %>% select(City,DS,'DS.Name',Cluster,Live,FSN,BU,SC,Analytical_Vertical,brand,Title,NOOS,P1P2P3,Reporting,City_DRR,Override_DRR,Final_DRR,DS_Count,Store_DRR,DRR,OPDxU20,Repl_DRR,'Store Type')
colnames(Master)<-c('City','DS','DS_Name','Cluster','Live','FSN','BU','SC','AV','Brand','Title','NOOS','P1P2P3','Reporting','City_DRR','Override_DRR','Final_DRR','DS_Count','Store_Fcst_DRR','Instock_DRR','OPDxU2O','OPD_Adjusted_DRR','Store_Type')

##modifying norms for nonselling FSNs###############################################
NonSelling<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/No_Sale_FSNs.csv",bucket = gcs_bucket,saveToDisk = "temp_file.csv",overwrite = TRUE)
NonSelling<-read.csv("temp_file.csv")
rm(temp_file.csv)
NonSelling<-NonSelling %>% select(fc,fsn) 
NonSelling<-distinct(NonSelling)
NonSelling$No_Sale<-1
Master<-merge(Master,NonSelling,by.x = c('DS','FSN'),by.y = c('fc','fsn'),all.x = TRUE)
Master[is.na(Master)]<-0
rm(NonSelling)
#####################################################################################

##Calculating Target Units for each DSxFSN############################################
Master$DRR_remark<-ifelse(Master$OPD_Adjusted_DRR<0.1,"<0.1",ifelse(Master$OPD_Adjusted_DRR<0.5,"0.1 to 0.5",ifelse(Master$OPD_Adjusted_DRR<1.000001,"0.5 to 1",">1")))
Master$DS_Units<-ifelse(Master$DRR_remark=="<0.1",1,ifelse(Master$DRR_remark=="0.1 to 0.5",2,ifelse(Master$DRR_remark=="0.5 to 1",3,0)))

##Master$DS_Units<-ifelse(Master$DRR_remark=="<0.1",2,ifelse(Master$DRR_remark=="0.1 to 0.5",4,ifelse(Master$DRR_remark=="0.5 to 1",6,0)))
##Master$DOH<-ifelse(Master$Store_Type=="Tiny" & Master$DRR_remark==">1",3,ifelse(Master$DRR_remark==">1",4,0))

###DOH Logics for each city ##################
Master$DOH <- ifelse(Master$City %in% c('Lucknow', 'Jaipur','Pune','Ahmedabad') & Master$DRR_remark == ">1",5,ifelse(Master$DRR_remark == ">1", 4, 0))
Master$DOH <- ifelse(Master$City %in% c('Guwahati') & Master$DRR_remark == ">1",8,Master$DOH)


###########change below on adding new cities##
Master<-subset(Master,City %in% c('Bangalore','Delhi','Mumbai','Kolkata','Pune','Lucknow','Guwahati','Jaipur','Ahmedabad','Chennai','Patna'))
Master$Target_Units=round(Master$DS_Units+Master$DOH*Master$OPD_Adjusted_DRR,digits = 0)
#######################################################################################


##Reading Sales band###############################################
SalesBand<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/City_FSN_30D_SalesBand.csv", bucket = gcs_bucket,saveToDisk = "temp_file.csv",overwrite = TRUE)
SalesBand<-read.csv("temp_file.csv")
rm(temp_file.csv)
SalesBand<-SalesBand %>% select(City,FSN,Sales_band)
Master<-merge(Master,SalesBand,by = c('City','FSN'),all.x = TRUE)
##Master$Target_Units<-ifelse(Master$Sales_band=='Band 5',round(Master$Target_Units*1.5,0),ifelse(Master$Sales_band=='Band 4',round(Master$Target_Units*1.2,0),ifelse(Master$Sales_band=='Band 1',1,(round(Master$Target_Units,0)))))
##Master$Target_Units<-ifelse(Master$AV %in% c('ArtificialSweetener','PicklesChutneys','OtherCookingOil','FloursSooji','SunflowerOil','Ghee','EdibleSeeds','SpicesMasala','SavouriesNamkeens','GrainsMillets','Condiments','Jaggery','NutsDryFruits''PastesPurees','OtherPulses','ToorDal','Rice','Salt','Sugar','SoyaProducts','Blended Oil','Olive Oil','Ground Nut Oil','Wheat Atta','Almond','Dates & Raisins','Other Nuts','Cashew','Moong','Channa','Basmati Rice','Whole Spices'),pmax(Master$Target_Units,2),Master$Target_Units)
####################################################################

top200<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/top200.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
top200<-read.csv("temp_file.csv")
rm(temp_file.csv)

Master<-merge(Master,top200,by.x=c('City','FSN'),by.y=c('City','FSN'),all.x=TRUE)
Master$top200[is.na(Master$top200)]<-"N"
Master$Target_Units<-ifelse(Master$OPD_Adjusted_DRR>100,round(Master$OPD_Adjusted_DRR*3,digits=0),Master$Target_Units)

####Min Target Override by Sell team#################################
Target_Override<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/Min_Target_Override.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv", overwrite = TRUE)
Target_Override<-read.csv("temp_file.csv")
Master<-merge(Master,Target_Override,by='FSN',all.x=TRUE)
Master[is.na(Master)]<-0
Master <- Master %>%
  mutate(Target_Units = case_when(
    Store_Type == "P1" ~ pmax(Target_Units, P1),
    Store_Type == "P2" ~ pmax(Target_Units, P2),
    Store_Type == "P3" ~ pmax(Target_Units, P3),
    TRUE ~ Target_Units
  ))
Master <- Master[, !(names(Master) %in% c("P1", "P2","P3"))]

####Reducing Target for non selling FSNs for the store##################
##Master$Target_Units <- ifelse(Master$No_Sale == 1 & Master$Instock_DRR < 0.07,pmin(Master$Target_Units, 1),Master$Target_Units)
rm(Target_Override)
#########################################################################

##MLE & LS Target Units Calculations#####################################
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
Master_1=Master
Master=Master_1

###############April MLE_event_change#####################
# 
# MLE_Event<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/Apr OMG MLE Sale FSN Inventory Spike.csv", bucket = gcs_bucket2)
# MLE_Event$`Planned Spike`=as.numeric(MLE_Event$`Planned Spike`)
# MLE_Event$`Norm (Only to consume if no historic DRR)`=as.numeric(MLE_Event$`Norm (Only to consume if no historic DRR)`)
# MLE_Event=MLE_Event[,-c(1,5)]
# MLE_Event=unique(MLE_Event)
# Master=merge(Master,MLE_Event,on="FSN",all.x=T)
# 
# Master$OPD_Adjusted_DRR <- ifelse(
#   !is.na(Master$`Planned Spike`),
#   pmax(Master$Instock_DRR, Master$OPD_Adjusted_DRR) * Master$`Planned Spike`,
#   Master$OPD_Adjusted_DRR
# )
# 
# Master$Target_Units <- ifelse(Master$BU %in% c('Mobile', 'CoreElectronics','EmergingElectronics','LargeAppliances') & 
#                                 pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) <= 0.1, 1,
#                               ifelse(Master$BU %in% c('Mobile', 'CoreElectronics','EmergingElectronics','LargeAppliances') & 
#                                        pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) <= 0.5, 3,
#                                      ifelse(Master$BU %in% c('Mobile', 'CoreElectronics','EmergingElectronics','LargeAppliances') & 
#                                               pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) > 0.5, 
#                                             ceiling(pmax(Master$OPD_Adjusted_DRR, Master$Instock_DRR) * 4), 
#                                             Master$Target_Units)))
# 
# Master$Target_Units <- ifelse(
#   !is.na(Master$`Planned Spike`) & Master$No_Sale == 1,
#   Master$`Norm (Only to consume if no historic DRR)`,
#   Master$Target_Units
# )
# 
# Master$Target_Units <- ifelse(Master$BU %in% c('Mobile', 'CoreElectronics', 'EmergingElectronics') & Master$Target_Units == 1,2,Master$Target_Units)
# 
# 
# setDT(Master)
# setDT(Master_1)
# summary1 <- Master[, .(total_target = sum(Target_Units, na.rm = TRUE)), by = BU]
# summary2 <- Master_1[, .(total_target = sum(Target_Units, na.rm = TRUE)), by = BU]
# 
# Master=Master[,-c("Planned Spike","Norm (Only to consume if no historic DRR)")]

#####Overriding MLE&LS topical norms####################################

#manual_norm = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1R8vApW-d_uonyLw3N6azE6mKwD51qvPglY9iyKX5hLw/edit?gid=0#gid=0',sheet='LS'))
manual_norm= data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/Target_override - LS.csv", bucket = gcs_bucket2))
manual_norm <- manual_norm %>%
  select(-City)  # Removes the "City" column
manual_norm$New_norm<-as.numeric(manual_norm$New_norm)
#temp_master<-Master
Master <- Master %>%
  left_join(manual_norm, by = c("FSN", "DS")) %>%   # Merge based on FSN & DS
  mutate(Target_Units = ifelse(!is.na(New_norm), New_norm, Target_Units)) %>%  # Replace Target if match found
  select(-New_norm)
###################################################################

###Code to have any exclusion at store level#######################

##DS_level_excl<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_level_exclusion.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv", overwrite = TRUE)
##DS_level_excl<-read.csv("temp_file.csv")
##Master<-merge(Master,DS_level_excl,by.x=c('City','FSN','DS'),by.y=c('City','FSN','DS'),all.x=TRUE)
##Master$Remove[is.na(Master$Remove)]<-0
##Master<-subset(Master,Remove<1)
##Master <- Master[ , !(names(Master) %in% "Remove")]
###################################################################



##Revised Target for top2500#########################################
toplist<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/top_2500_master_daily.csv", saveToDisk = "temp_file.csv", overwrite = TRUE)
toplist<-read.csv("temp_file.csv")
toplist<-toplist %>% select(ds_fkint_cp_santa_hl_core_selection_1_0.city,ds_fkint_cp_santa_hl_core_selection_1_0.product_id,ds_fkint_cp_santa_hl_core_selection_1_0.rank)
toplist<-distinct(toplist)
colnames(toplist)<-c('City','FSN','Rank')
toplist$Tag<-ifelse(toplist$Rank<101,"Top 100",ifelse(toplist$Rank<501,"Top 101-500","Top 501-2500"))
toplist<-toplist %>% select(City,FSN,Tag)

Master<-merge(Master,toplist,by.x = c('City','FSN'),by.y = c('City','FSN'),all.x = TRUE)
rm(toplist)
Master$Tag[is.na(Master$Tag)]<-"NA"

Master$DOH <- ifelse(Master$Tag %in% c('Top 100','Top 101-500') & Master$DRR_remark == ">1",pmax(Master$DOH,7),Master$DOH)
Master$DOH <- ifelse(Master$Tag %in% c('Top 501 - 2500') & Master$DRR_remark == ">1",pmax(Master$DOH,6),Master$DOH)

#Additional norms for priority stores###

Master$Target_Units <- ifelse(Master$Tag %in% c('Top 100','Top 101-500','Top 501-2500') & 
                              Master$DS %in% c('ben_029_wh_hl_01','ben_120_wh_hl_01','mum_021_wh_hl_01','mum_024_wh_hl_01','mum_033_wh_hl_01','mum_063_wh_hl_01','mum_071_wh_hl_01','mum_079_wh_hl_01') & 
                              Master$DRR_remark == ">1", 8, Master$DOH)
Master$Target_Units=round(Master$DS_Units+Master$DOH*Master$OPD_Adjusted_DRR,digits = 0)
Master$Target_Units <- ifelse(Master$Tag == 'Top 100' | Master$Tag == 'Top 101-500',pmax(2, Master$Target_Units),Master$Target_Units)
Master$Target_Units <- ifelse(Master$Tag == 'Top 100' | Master$Tag == 'Top 101-500' | Master$Tag =='Top 501-2500',pmax(3, Master$Target_Units),Master$Target_Units)



Master <- Master %>%
  select(-Tag)  # Removes the 'Tag' column

Master$Target_Units<-pmin(350,Master$Target_Units)
##Master$Target_Units <- ifelse(Master$DRR_remark == ">1",pmax(Master$Target_Units, round(Master$DOH * Master$Instock_DRR, 0)),Master$Target_Units)

rm(toplist)

######################################################################
####Patch for Norm 99
norm_Override<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/norm_override.csv", bucket = gcs_bucket2,saveToDisk = "norm_override.csv", overwrite = TRUE)
norm_Override<-read.csv("norm_override.csv")
setDT(norm_Override)

Master = norm_Override[Master, on=c('FSN','DS')]
Master = Master[,`:=`(Target_Units = ifelse(!is.na(norm_99),pmax(norm_99,Target_Units),Target_Units))]

Master[,`:=`(norm_99 = NULL)]


######################################################################

##reading casepack and writing case pack target to new ouput###########
####Splitting master into mle and rest - casepack not applicable to mle

###MLE
Master_mle=Master[Master$BU %in% c("CoreElectronics","EmergingElectronics","Mobile","LargeAppliances","Lifestyle"),]
Master_nonmle=Master[!Master$BU %in% c("CoreElectronics","EmergingElectronics","Mobile","LargeAppliances","Lifestyle"),]
Master_mle$Case_Pack=1

###Non MLE
DS_Casepack<-gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_FSN_Casepack.csv", bucket = gcs_bucket2)
Master_nonmle<-merge(Master_nonmle,DS_Casepack,by.x = c('City','FSN'),by.y = c('City','FSN'),all.x = TRUE)
Master_nonmle$Case_Pack[is.na(Master_nonmle$Case_Pack)]<-1
Master_nonmle$Target_Units<-pmax(round(Master_nonmle$Target_Units/Master_nonmle$Case_Pack,0)*Master_nonmle$Case_Pack,Master_nonmle$Case_Pack)
listtocase <- c("Mumbai", "Bangalore", "Ahmedabad", "Pune","Kolkata","Delhi","Lucknow","Jaipur","Guwahati","Chennai","Patna")
Master_nonmle <- Master_nonmle %>%
  mutate(Target_Units = ifelse(City %in% listtocase,
                               # Formula 2
                               ifelse(Case_Pack > 1,round(ifelse(Case_Pack == Target_Units,ifelse(Target_Units / OPD_Adjusted_DRR < DOH * 2,round(Target_Units + (0.5 * Case_Pack),0),Case_Pack),Target_Units), 0),Target_Units),Target_Units))
Master_nonmle <- Master_nonmle %>%
  mutate(across(where(is.list), unlist))
rm(DS_Casepack)

####Creating master by joining non mle(case pack logic) and mle
Master=rbind(Master_nonmle,Master_mle)

Master<-Master %>% distinct(FSN,DS,.keep_all = TRUE)




print('casepack ho gaya')
rm(Master_1)
rm(Delhi)
rm(Patna)
rm(Bangalore)

rm(Chennai)
rm(Kolkata)
rm(DS_level)
rm(DS_level_aligned)
rm(Master_mle)
rm(Master_nonmle)
rm(norm_Override)
rm(SalesBand)


#######################################################################

##summary2 <- Master[, .(total_target = sum(Target_Units, na.rm = TRUE)), by = BU]

# summary1 <- Master_2[, .(total_target = sum(Target_Units, na.rm = TRUE)), by = BU]
##Master_old=gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_Target_Master.csv", bucket = gcs_bucket)
##setDT(Master_old)
##summary1 <- Master_old[, .(total_target = sum(Target_Units, na.rm = TRUE)), by = BU]
setDT(Master)
print('uploading call')
Master<-Master %>% distinct(FSN,DS,.keep_all = TRUE)

rm(P1,P2,P3,manual_norm,DRR2,DS,DS_count,Mumbai,Premium,top200,Tot_DRR,OPD)
Master <- Master %>%
  mutate(across(where(is.list), unlist))




##Uploading output to gcp##############################################
gcs_upload(Master, name = "ipc/Nav/Hyperlocal_IPC/DS_Target_Master.csv", object_function = gcs_save_csv,predefinedAcl = "bucketLevel")
gc()

########################################################################
