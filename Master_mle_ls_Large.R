###DS master

ds_master= data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2))
ds_master_summary=ds_master %>% group_by(City) %>% mutate(ds_count=n())

####Reading product categorization table
prod_cat = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1mIxIfmVOJ1whZO4bDKD51Wlcqud2l2rzuOgbFt4ges4/edit?gid=782554625#gid=782554625',sheet ='product_categorization'))
setnames(prod_cat,old=c("product_id","title","analytic_business_unit","analytic_super_category","analytic_vertical"),new=c("FSN","Title","BU","SC","Analytical_Vertical"))
prod_cat=prod_cat[,c("FSN","Title","BU","SC","Analytical_Vertical","brand")]
prod_cat=unique(prod_cat)

############################################ Electronics ###################################

EE<-data.table(read_sheet("https://docs.google.com/spreadsheets/d/14TufYGLxTfv1vYSQYGcsdYzRKzz1HjcdKaS2WIIQSwA/edit?pli=1&gid=1589894909#gid=1589894909",sheet="Electronics"))
EE<-unique(EE)
EE_com=EE[,c("BU","FSN","City","DS","Store Type","Premium","Comment","date")]
EE=EE[,c("BU","FSN","City","DS","Store Type","Premium")]

# Step 1: Expand cities
expanded_EE <- EE %>%
  mutate(City = ifelse(City == "all", list(unique(ds_master$City)), City)) %>%
  unnest(City)  
expanded_EE<-unique(expanded_EE)

expanded_EE$Key<-paste0(expanded_EE$FSN,expanded_EE$City)
expanded_EE_allDS=expanded_EE[expanded_EE$DS=="all",]
expanded_EE_not_all_DS=expanded_EE[expanded_EE$DS!="all",]
expanded_EE_not_all_DS1 <- expanded_EE_not_all_DS[!(expanded_EE_not_all_DS$Key %in% expanded_EE_allDS$Key), ]

expanded_EE=rbind(expanded_EE_allDS,expanded_EE_not_all_DS1)


# Step 2: Expand ds
expanded_EE1 <- expanded_EE %>%
  left_join(ds_master %>% select(City, DS) %>% distinct(), by = "City") %>% 
  mutate(DS = ifelse(DS.x == "all", DS.y, DS.x)) %>%
  select(FSN, City, DS, `Store Type`,Premium)# Keep original if not "all", else take all from df1

expanded_EE1<-unique(expanded_EE1)

expanded_EE2 <- expanded_EE1 %>%
  left_join(
    ds_master %>%
      select(DS, `Store Type`) %>%
      distinct(),
    by = "DS"
  ) %>%
  filter(`Store Type.x` == "all" | `Store Type.x` == `Store Type.y`)  # Remove duplicates

expanded_EE3 <- expanded_EE2 %>%
  left_join(
    ds_master %>%
      select(DS, Premium) %>%
      distinct(),
    by = "DS"
  ) %>%
  filter(Premium.x == "all" | Premium.x == Premium.y)  # Remove duplicates

expanded_EE3=expanded_EE3[,c("FSN","City","DS","Store Type.y","Premium.y")]
setnames(expanded_EE3,old=c("FSN","City","DS","Store Type.y","Premium.y"),new=c("FSN","City","DS","P1P2P3","Reporting"))
expanded_EE3=left_join(expanded_EE3,prod_cat,by=("FSN"))
expanded_EE3=merge(expanded_EE3,prod_cat,on=("FSN"),all.x=T)
expanded_EE3=unique(expanded_EE3)

#### Finding the missing DS view

missingEE<-data.frame(fsn=unique(expanded_EE3[expanded_EE3$BU=="null",]$FSN))
expanded_EE3 <- expanded_EE3 %>%
  mutate(BU = ifelse(is.na(BU), "EmergingElectronics", 
                     ifelse(BU == "LargeAppliances", "CoreElectronics", BU)))

########### New_Launch but not in master
# 
# EE_NL<-data.table(read_sheet("https://docs.google.com/spreadsheets/d/14TufYGLxTfv1vYSQYGcsdYzRKzz1HjcdKaS2WIIQSwA/edit?pli=1&gid=1589894909#gid=1589894909",sheet="New_launch_electronics"))
# EE_NL<-unique(EE_NL)
# EE_NL_com=EE_NL
# EE_NL=EE_NL[,c("BU","FSN","City","DS","Store Type","Premium")]
# 
# # Step 1: Expand cities
# expanded_EE_NL <- EE_NL %>%
#   mutate(City = ifelse(City == "all", list(unique(ds_master$City)), City)) %>%
#   unnest(City)  
# expanded_EE_NL<-unique(expanded_EE_NL)
# 
# expanded_EE_NL$Key<-paste0(expanded_EE_NL$FSN,expanded_EE_NL$City)
# expanded_EE_NL_allDS=expanded_EE_NL[expanded_EE_NL$DS=="all",]
# expanded_EE_NL_not_all_DS=expanded_EE_NL[expanded_EE_NL$DS!="all",]
# expanded_EE_NL_not_all_DS1 <- expanded_EE_NL_not_all_DS[!(expanded_EE_NL_not_all_DS$Key %in% expanded_EE_NL_allDS$Key), ]
# 
# expanded_EE_NL=rbind(expanded_EE_NL_allDS,expanded_EE_NL_not_all_DS1)
# 
# 
# # Step 2: Expand ds
# expanded_EE_NL1 <- expanded_EE_NL %>%
#   left_join(ds_master %>% select(City, DS) %>% distinct(), by = "City") %>% 
#   mutate(DS = ifelse(DS.x == "all", DS.y, DS.x)) %>%
#   select(FSN, City, DS, `Store Type`,Premium)# KEE_NLp original if not "all", else take all from df1
# 
# expanded_EE_NL1<-unique(expanded_EE_NL1)
# 
# expanded_EE_NL2 <- expanded_EE_NL1 %>%
#   left_join(
#     ds_master %>%
#       select(DS, `Store Type`) %>%
#       distinct(),
#     by = "DS"
#   ) %>%
#   filter(`Store Type.x` == "all" | `Store Type.x` == `Store Type.y`)  # Remove duplicates
# 
# expanded_EE_NL3 <- expanded_EE_NL2 %>%
#   left_join(
#     ds_master %>%
#       select(DS, Premium) %>%
#       distinct(),
#     by = "DS"
#   ) %>%
#   filter(Premium.x == "all" | Premium.x == Premium.y)  # Remove duplicates
# 
# expanded_EE_NL3=expanded_EE_NL3[,c("FSN","City","DS","Store Type.y","Premium.y")]
# setnames(expanded_EE_NL3,old=c("FSN","City","DS","Store Type.y","Premium.y"),new=c("FSN","City","DS","P1P2P3","Reporting"))
# expanded_EE_NL3=left_join(expanded_EE_NL3,prod_cat,by=("FSN"))
# expanded_EE_NL3=merge(expanded_EE_NL3,prod_cat,on=("FSN"),all.x=T)
# 
# expanded_EE_NL3=unique(expanded_EE_NL3)
# 
# missing<-data.frame(missing=unique(expanded_EE_NL3[expanded_EE_NL3$BU=="null",]$FSN))
# 
# expanded_EE_NL3 <- expanded_EE_NL3 %>%
#   mutate(BU = ifelse(is.na(BU), "EmergingElectronics", 
#                      ifelse(BU == "LargeAppliances", "CoreElectronics", BU)))


############################################ Large Appliances #########################################################

LA<-data.table(read_sheet("https://docs.google.com/spreadsheets/d/14TufYGLxTfv1vYSQYGcsdYzRKzz1HjcdKaS2WIIQSwA/edit?pli=1&gid=1589894909#gid=1589894909",sheet="Large_Appliances"))
LA<-unique(LA)
LA=LA[,c("BU","FSN","City","DS","Store Type","Premium")]

# Step 1: Expand cities
expanded_LA <- LA %>%
  mutate(City = ifelse(City == "all", list(unique(ds_master$City)), City)) %>%
  unnest(City)  
expanded_LA<-unique(expanded_LA)

expanded_LA$Key<-paste0(expanded_LA$FSN,expanded_LA$City)
expanded_LA_allDS=expanded_LA[expanded_LA$DS=="all",]
expanded_LA_not_all_DS=expanded_LA[expanded_LA$DS!="all",]
expanded_LA_not_all_DS1 <- expanded_LA_not_all_DS[!(expanded_LA_not_all_DS$Key %in% expanded_LA_allDS$Key), ]

expanded_LA=rbind(expanded_LA_allDS,expanded_LA_not_all_DS1)


# Step 2: Expand ds
expanded_LA1 <- expanded_LA %>%
  left_join(ds_master %>% select(City, DS) %>% distinct(), by = "City") %>% 
  mutate(DS = ifelse(DS.x == "all", DS.y, DS.x)) %>%
  select(FSN, City, DS, `Store Type`,Premium)# KLAp original if not "all", else take all from df1

expanded_LA1<-unique(expanded_LA1)

expanded_LA2 <- expanded_LA1 %>%
  left_join(
    ds_master %>%
      select(DS, `Store Type`) %>%
      distinct(),
    by = "DS"
  ) %>%
  filter(`Store Type.x` == "all" | `Store Type.x` == `Store Type.y`)  # Remove duplicates

expanded_LA3 <- expanded_LA2 %>%
  left_join(
    ds_master %>%
      select(DS, Premium) %>%
      distinct(),
    by = "DS"
  ) %>%
  filter(Premium.x == "all" | Premium.x == Premium.y)  # Remove duplicates


expanded_LA3=expanded_LA3[,c("FSN","City","DS","Store Type.y","Premium.y")]
setnames(expanded_LA3,old=c("FSN","City","DS","Store Type.y","Premium.y"),new=c("FSN","City","DS","P1P2P3","Reporting"))
expanded_LA3=left_join(expanded_LA3,prod_cat,by=("FSN"))
expanded_LA3=merge(expanded_LA3,prod_cat,on=("FSN"),all.x=T)
expanded_LA3=unique(expanded_LA3)

#missing<-rbind(missing,data.frame(missing=unique(expanded_LA3[expanded_LA3$BU=="null",]$FSN)))

expanded_LA3 <- expanded_LA3 %>%
  mutate(BU = ifelse(is.na(BU), "LargeAppliances",BU))

##############################################Mobile#################################################3

Mobile<-data.table(read_sheet("https://docs.google.com/spreadsheets/d/14TufYGLxTfv1vYSQYGcsdYzRKzz1HjcdKaS2WIIQSwA/edit?pli=1&gid=1589894909#gid=1589894909",sheet="Mobile"))
Mobile<-unique(Mobile)
Mobile=Mobile[,c("BU","FSN","City","DS","Store Type","Premium")]
# Step 1: Expand cities
expanded_Mobile <- Mobile %>%
  mutate(City = ifelse(City == "all", list(unique(ds_master$City)), City)) %>%
  unnest(City)  
expanded_Mobile<-unique(expanded_Mobile)

expanded_Mobile$Key<-paste0(expanded_Mobile$FSN,expanded_Mobile$City)
expanded_Mobile_allDS=expanded_Mobile[expanded_Mobile$DS=="all",]
expanded_Mobile_not_all_DS=expanded_Mobile[expanded_Mobile$DS!="all",]
expanded_Mobile_not_all_DS1 <- expanded_Mobile_not_all_DS[!(expanded_Mobile_not_all_DS$Key %in% expanded_Mobile_allDS$Key), ]

expanded_Mobile=rbind(expanded_Mobile_allDS,expanded_Mobile_not_all_DS1)


# Step 2: Expand ds
expanded_Mobile1 <- expanded_Mobile %>%
  left_join(ds_master %>% select(City, DS) %>% distinct(), by = "City") %>% 
  mutate(DS = ifelse(DS.x == "all", DS.y, DS.x)) %>%
  select(FSN, City, DS, `Store Type`,Premium)# KMobilep original if not "all", else take all from df1

expanded_Mobile1<-unique(expanded_Mobile1)

expanded_Mobile2 <- expanded_Mobile1 %>%
  left_join(
    ds_master %>%
      select(DS, `Store Type`) %>%
      distinct(),
    by = "DS"
  ) %>%
  filter(`Store Type.x` == "all" | `Store Type.x` == `Store Type.y`)  # Remove duplicates

expanded_Mobile3 <- expanded_Mobile2 %>%
  left_join(
    ds_master %>%
      select(DS, Premium) %>%
      distinct(),
    by = "DS"
  ) %>%
  filter(Premium.x == "all" | Premium.x == Premium.y)  # Remove duplicates

expanded_Mobile3=expanded_Mobile3[,c("FSN","City","DS","Store Type.y","Premium.y")]
setnames(expanded_Mobile3,old=c("FSN","City","DS","Store Type.y","Premium.y"),new=c("FSN","City","DS","P1P2P3","Reporting"))
expanded_Mobile3=left_join(expanded_Mobile3,prod_cat,by=("FSN"))
expanded_Mobile3=merge(expanded_Mobile3,prod_cat,on=("FSN"),all.x=T)
expanded_Mobile3=unique(expanded_Mobile3)

#missing<-rbind(missing,data.frame(missing=unique(expanded_Mobile3[expanded_Mobile3$BU=="null",]$FSN)))

expanded_Mobile3 <- expanded_Mobile3 %>%
  mutate(BU = ifelse(is.na(BU), "Mobile",BU))

#write_sheet(missing,'https://docs.google.com/spreadsheets/d/18sInnYW1KV75x3EYfmbdbidUM87KT8LodpktOC4jT-k/edit?gid=0#gid=0',sheet='missing')


#write.csv(expanded_Mobile3,"D:\\shruti.shahi\\Outputs\\Mobile_master1.csv")

##MLE Master Original
mle_master=rbind(expanded_EE3,expanded_Mobile3,expanded_LA3)

setDT(mle_master)
mle_master[, `:=` (NOOS = 0, 
                   City_DRR = 0.1, 
                   Buy_POC = "None", 
                   date = as.Date(NA))]
mle_master=mle_master[,c("DS","FSN","City","BU","SC","Analytical_Vertical","brand","Title","NOOS","City_DRR","P1P2P3","Reporting","Buy_POC","date")]        

###MLE_new_addn
# 
# New_addn<-data.table(read_sheet("https://docs.google.com/spreadsheets/d/14TufYGLxTfv1vYSQYGcsdYzRKzz1HjcdKaS2WIIQSwA/edit?pli=1&gid=1589894909#gid=1589894909",sheet="New_Addn"))
# New_addn$date <- dmy(unlist(New_addn$date)) 
# New_addn=New_addn[,c("BU","FSN","City","DS","Store Type","Premium","date")]
# New_addn$date=as.Date(New_addn$date,format="%d/%m/%Y")
# New_addn<-unique(New_addn)
# # Step 1: Expand cities
# expanded_New_addn <- New_addn %>%
#   mutate(City = ifelse(City == "all", list(unique(ds_master$City)), City)) %>%
#   unnest(City)  
# expanded_New_addn<-unique(expanded_New_addn)
# 
# expanded_New_addn$Key<-paste0(expanded_New_addn$FSN,expanded_New_addn$City)
# expanded_New_addn_allDS=expanded_New_addn[expanded_New_addn$DS=="all",]
# expanded_New_addn_not_all_DS=expanded_New_addn[expanded_New_addn$DS!="all",]
# expanded_New_addn_not_all_DS1 <- expanded_New_addn_not_all_DS[!(expanded_New_addn_not_all_DS$Key %in% expanded_New_addn_allDS$Key), ]
# 
# expanded_New_addn=rbind(expanded_New_addn_allDS,expanded_New_addn_not_all_DS1)
# 

# Step 2: Expand ds
# expanded_New_addn1 <- expanded_New_addn %>%
#   left_join(ds_master %>% select(City, DS) %>% distinct(), by = "City") %>% 
#   mutate(DS = ifelse(DS.x == "all", DS.y, DS.x)) %>%
#   select(FSN, City, DS, `Store Type`,Premium,date)# KNew_addnp original if not "all", else take all from df1
# 
# expanded_New_addn1<-unique(expanded_New_addn1)
# 
# expanded_New_addn2 <- expanded_New_addn1 %>%
#   left_join(
#     ds_master %>%
#       select(DS, `Store Type`) %>%
#       distinct(),
#     by = "DS"
#   ) %>%
#   filter(`Store Type.x` == "all" | `Store Type.x` == `Store Type.y`)  # Remove duplicates
# 
# expanded_New_addn3 <- expanded_New_addn2 %>%
#   left_join(
#     ds_master %>%
#       select(DS, Premium) %>%
#       distinct(),
#     by = "DS"
#   ) %>%
#   filter(Premium.x == "all" | Premium.x == Premium.y)  # Remove duplicates
# 
# expanded_New_addn3=expanded_New_addn3[,c("FSN","City","DS","Store Type.y","Premium.y","date")]
# setnames(expanded_New_addn3,old=c("FSN","City","DS","Store Type.y","Premium.y"),new=c("FSN","City","DS","P1P2P3","Reporting"))
# expanded_New_addn3=left_join(expanded_New_addn3,prod_cat,by=("FSN"))
# expanded_New_addn3=merge(expanded_New_addn3,prod_cat,on=("FSN"),all.x=T)
# expanded_New_addn3=unique(expanded_New_addn3)
# #expanded_New_addn3$
# 
# missing<-rbind(missing,data.frame(missing=unique(expanded_New_addn3[expanded_New_addn3$BU=="null",]$FSN)))
# 
# write_sheet(missing,'https://docs.google.com/spreadsheets/d/18sInnYW1KV75x3EYfmbdbidUM87KT8LodpktOC4jT-k/edit?gid=0#gid=0',sheet='missing')
# 
# #############Needs to change
# expanded_New_addn3 <- expanded_New_addn3 %>%
#   mutate(BU = ifelse(is.na(BU), "New_Addn",BU))
# 
# setDT(expanded_New_addn3)
# expanded_New_addn3[, `:=` (NOOS = 0, 
#                    City_DRR = 0.1, 
#                    Buy_POC = "None")]
# expanded_New_addn3=expanded_New_addn3[,c("DS","FSN","City","BU","SC","Analytical_Vertical","brand","Title","NOOS","City_DRR","P1P2P3","Reporting","Buy_POC","date")]        
# 
# ##mle_marter_original +additional
# mle_master=rbind(mle_master,expanded_New_addn3)

setDT(mle_master)
# mle_master[, `:=` (NOOS = 0, 
#                    City_DRR = 0.1, 
#                    Buy_POC = "None", 
#                    date = as.Date(NA))]
mle_master=mle_master[,c("DS","FSN","City","BU","SC","Analytical_Vertical","brand","Title","NOOS","City_DRR","P1P2P3","Reporting","Buy_POC","date")]        
mle_master=unique(mle_master)
mle_master$key=paste0(mle_master$FSN,mle_master$City)

# ####Remove from master
Removals<-data.table(read_sheet("https://docs.google.com/spreadsheets/d/14TufYGLxTfv1vYSQYGcsdYzRKzz1HjcdKaS2WIIQSwA/edit?pli=1&gid=1589894909#gid=1589894909",sheet="Removals"))
Removals$key=paste0(Removals$FSN,Removals$City)
mle_master=mle_master[!mle_master$key %in% Removals$key,]

mle_master <- mle_master %>% select(-key)


fwrite(mle_master,paste0("D:\\shruti.shahi\\Outputs\\Master\\mle_master.csv"))


##########################################################
######################################################################
##################################### Lifestyle ################################

# ###updated logic
# Lifestyle<-data.table(read_sheet("https://docs.google.com/spreadsheets/d/1deookDH4ZXTnBwAed4QPJ952BHwN2dAnadb75FQFnSo/edit?gid=172412513#gid=172412513",sheet="Sheet1"))
# Lifestyle=unique(Lifestyle)
# Lifestyle1=Lifestyle
# Lifestyle=Lifestyle[,c("fsn","HL city","fc","Store Type")]
# setnames(Lifestyle,old=colnames(Lifestyle),new=c("FSN","City","DS","P1P2P3"))
# #Lifestyle=left_join(Lifestyle,prod_cat,by=("FSN"))
# Lifestyle=merge(Lifestyle,prod_cat,on=("FSN"),all.x=T)
# Lifestyle=unique(Lifestyle)
# 
# missing<-data.frame(unique(Lifestyle[is.na(Lifestyle$BU),]$FSN))
# write_sheet(missing,'https://docs.google.com/spreadsheets/d/18sInnYW1KV75x3EYfmbdbidUM87KT8LodpktOC4jT-k/edit?gid=0#gid=0',sheet='missing')
# 
# #write.csv(expanded_Lifestyle3,"C://Users//shruti.shahi//Documents//expanded_Lifestyle3.csv")
# 
# setDT(Lifestyle)
# Lifestyle[, `:=` (NOOS = 0, 
#                             City_DRR = 0.1, 
#                             Buy_POC = "None",
#                             Reporting="None",
#                             date = as.Date(NA))]
# Lifestyle=Lifestyle[,c("DS","FSN","City","BU","SC","Analytical_Vertical","brand","Title","NOOS","City_DRR","P1P2P3","Reporting","Buy_POC","date")]        
# Lifestyle$BU="Lifestyle"
# fwrite(Lifestyle,paste0("D:\\shruti.shahi\\Outputs\\lifestyle\\lifestyle_master.csv"))
# expanded_Lifestyle3=Lifestyle

#####my logic##############

Lifestyle<-data.table(read_sheet("https://docs.google.com/spreadsheets/d/14TufYGLxTfv1vYSQYGcsdYzRKzz1HjcdKaS2WIIQSwA/edit?pli=1&gid=1589894909#gid=1589894909",sheet="Lifestyle"))
Lifestyle<-unique(Lifestyle)
Lifestyle=Lifestyle[,c("BU","FSN","City","DS","Store Type","Premium")]
# Step 1: Expand cities
expanded_Lifestyle <- Lifestyle %>%
  mutate(City = ifelse(City == "all", list(unique(ds_master$City)), City)) %>%
  unnest(City)
expanded_Lifestyle<-unique(expanded_Lifestyle)

expanded_Lifestyle$Key<-paste0(expanded_Lifestyle$FSN,expanded_Lifestyle$City)
expanded_Lifestyle_allDS=expanded_Lifestyle[expanded_Lifestyle$DS=="all",]
expanded_Lifestyle_not_all_DS=expanded_Lifestyle[expanded_Lifestyle$DS!="all",]
expanded_Lifestyle_not_all_DS1 <- expanded_Lifestyle_not_all_DS[!(expanded_Lifestyle_not_all_DS$Key %in% expanded_Lifestyle_allDS$Key), ]

expanded_Lifestyle=rbind(expanded_Lifestyle_allDS,expanded_Lifestyle_not_all_DS1)


# Step 2: Expand ds
expanded_Lifestyle1 <- expanded_Lifestyle %>%
  left_join(ds_master %>% select(City, DS) %>% distinct(), by = "City") %>%
  mutate(DS = ifelse(DS.x == "all", DS.y, DS.x)) %>%
  select(FSN, City, DS, `Store Type`,Premium)# KLifestylep original if not "all", else take all from df1

expanded_Lifestyle1<-unique(expanded_Lifestyle1)

expanded_Lifestyle2 <- expanded_Lifestyle1 %>%
  left_join(
    ds_master %>%
      select(DS, `Store Type`) %>%
      distinct(),
    by = "DS"
  ) %>%
  filter(`Store Type.x` == "all" | `Store Type.x` == `Store Type.y`)  # Remove duplicates

expanded_Lifestyle3 <- expanded_Lifestyle2 %>%
  left_join(
    ds_master %>%
      select(DS, Premium) %>%
      distinct(),
    by = "DS"
  ) %>%
  filter(Premium.x == "all" | Premium.x == Premium.y)  # Remove duplicates


expanded_Lifestyle3=expanded_Lifestyle3[,c("FSN","City","DS","Store Type.y","Premium.y")]
setnames(expanded_Lifestyle3,old=c("FSN","City","DS","Store Type.y","Premium.y"),new=c("FSN","City","DS","P1P2P3","Reporting"))
expanded_Lifestyle3=left_join(expanded_Lifestyle3,prod_cat,by=("FSN"))
expanded_Lifestyle3=merge(expanded_Lifestyle3,prod_cat,on=("FSN"),all.x=T)
expanded_Lifestyle3=unique(expanded_Lifestyle3)

#missing<-rbind(missing,data.frame(missing=unique(expanded_Lifestyle3[expanded_Lifestyle3$BU=="null",]$FSN)))
#write.csv(expanded_Lifestyle3,"C://Users//shruti.shahi//Documents//expanded_Lifestyle3.csv")

setDT(expanded_Lifestyle3)
expanded_Lifestyle3[, `:=` (NOOS = 0,
                            City_DRR = 0.1,
                            Buy_POC = "None",
                            date = as.Date(NA))]
expanded_Lifestyle3=expanded_Lifestyle3[,c("DS","FSN","City","BU","SC","Analytical_Vertical","brand","Title","NOOS","City_DRR","P1P2P3","Reporting","Buy_POC","date")]
expanded_Lifestyle3$BU="Lifestyle"

fwrite(expanded_Lifestyle3,paste0("D:\\shruti.shahi\\Outputs\\lifestyle\\lifestyle_master.csv"))

############################Large#############################################

Large<-data.table(read_sheet("https://docs.google.com/spreadsheets/d/14TufYGLxTfv1vYSQYGcsdYzRKzz1HjcdKaS2WIIQSwA/edit?pli=1&gid=1589894909#gid=1589894909",sheet="Large"))
#Large<-unique(Large)
Large=Large[,c("BU","FSN","City","DS","Store Type","Premium")]
# Step 1: Expand cities
expanded_Large <- Large %>%
  mutate(City = ifelse(City == "all", list(unique(ds_master$City)), City)) %>%
  unnest(City)  
expanded_Large<-unique(expanded_Large)

expanded_Large$Key<-paste0(expanded_Large$FSN,expanded_Large$City)
expanded_Large_allDS=expanded_Large[expanded_Large$DS=="all",]
expanded_Large_not_all_DS=expanded_Large[expanded_Large$DS!="all",]
expanded_Large_not_all_DS1 <- expanded_Large_not_all_DS[!(expanded_Large_not_all_DS$Key %in% expanded_Large_allDS$Key), ]

expanded_Large=rbind(expanded_Large_allDS,expanded_Large_not_all_DS1)


# Step 2: Expand ds
expanded_Large1 <- expanded_Large %>%
  left_join(ds_master %>% select(City, DS) %>% distinct(), by = "City") %>% 
  mutate(DS = ifelse(DS.x == "all", DS.y, DS.x)) %>%
  select(FSN, City, DS, `Store Type`,Premium)# KLargep original if not "all", else take all from df1

expanded_Large1<-unique(expanded_Large1)

expanded_Large2 <- expanded_Large1 %>%
  left_join(
    ds_master %>%
      select(DS, `Store Type`) %>%
      distinct(),
    by = "DS"
  ) %>%
  filter(`Store Type.x` == "all" | `Store Type.x` == `Store Type.y`)  # Remove duplicates

expanded_Large3 <- expanded_Large2 %>%
  left_join(
    ds_master %>%
      select(DS, Premium) %>%
      distinct(),
    by = "DS"
  ) %>%
  filter(Premium.x == "all" | Premium.x == Premium.y)  # Remove duplicates


expanded_Large3=expanded_Large3[,c("FSN","City","DS","Store Type.y","Premium.y")]
setnames(expanded_Large3,old=c("FSN","City","DS","Store Type.y","Premium.y"),new=c("FSN","City","DS","P1P2P3","Reporting"))
expanded_Large3=left_join(expanded_Large3,prod_cat,by=("FSN"))
expanded_Large3=merge(expanded_Large3,prod_cat,on=("FSN"),all.x=T)
expanded_Large3=unique(expanded_Large3)
# missing<-rbind(missing,data.frame(missing=unique(expanded_Large3[expanded_Large3$BU=="null",]$FSN)))
expanded_Large3$BU="Large"

setDT(expanded_Large3)
expanded_Large3[, `:=` (NOOS = 0, 
                        City_DRR = 0.1, 
                        Buy_POC = "None", 
                        date = as.Date(NA))]
expanded_Large3=expanded_Large3[,c("DS","FSN","City","BU","SC","Analytical_Vertical","brand","Title","NOOS","City_DRR","P1P2P3","Reporting","Buy_POC","date")]        


fwrite(expanded_Large3,paste0("D:\\shruti.shahi\\Outputs\\Large\\large_master.csv"))

#####DS Target master

nav_master= gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_level_Master.csv", bucket = gcs_bucket2)
mle_ls_master=rbind(mle_master,expanded_Lifestyle3)
mle_ls_master=unique(mle_ls_master)

fwrite(mle_ls_master,paste0("D:\\shruti.shahi\\Outputs\\Master\\DS_level_Master.csv"))



#write.csv(expanded_Large3,"C://Users//shruti.shahi//Documents//expanded_Large3.csv")


