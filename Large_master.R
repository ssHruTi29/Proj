

Large = data.table(read_sheet('https://docs.google.com/spreadsheets/d/14TufYGLxTfv1vYSQYGcsdYzRKzz1HjcdKaS2WIIQSwA/edit?gid=0#gid=0',sheet='Large'))

ds_master= data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2))
setnames(ds_master, old ='DS',new='business_zone')

#### checking all

##all city combination
fsns<-Large$FSN
city_comb<-data.frame(City=c('Mumbai','Delhi','Kolkata','Bangalore','Pune','Lucknow','Ahmedabad','Guwahati')
                      ,Ds="all")
combinations <- expand.grid(City = city_comb$City, FSN = fsns)
combinations$Ds<-"all"
combinations$BU<-"Large"
Large_all<-combinations[,c("City","Ds","FSN","BU")]

##taking all P3,city list
store_master<-ds_master[ds_master$`Store Type`=="P3"]
Large_all = store_master[Large_all, on=c('City'), allow =TRUE]
Large_all$date<-"13-02-2025"
Large_all$Live=1

Large_all[, `:=`(NOOS = 0, SC = "NA", Analytical_Vertical = "NA", brand = "NA", Title = "NA", City_DRR = 0.1, P1P2P3 = 'P3', Reporting = 'Current', Buy_POC = 'Ojas')]
setnames(Large_all,old=c("business_zone"),new=c("DS"))
columns_to_keep<-colnames(lifestyle_master_ds)
Large_all <- Large_all[, ..columns_to_keep]

fwrite(Large_all,paste0("D:\\shruti.shahi\\Outputs\\Large\\large_master.csv"))

#####creating all cities X FSN view


##



all_city<-Large[City == "all"]  # Remove "All" for join
#all_city[, City := NULL]

all_city <- ds_master[all_city, on = .(City), allow.cartesian = TRUE]  # Join with all cities

# **Step 2: Expand DS based on P1/P2/P3**
expanded_dt <- expanded_dt[P1P2P3 == "All" | Store_Type == P1P2P3]  # Match Store_Type with P1P2P3

# **Step 3: Handle specific cities separately**
matched_dt <- dt2[City != "All"][dt1, on = "City", nomatch = 0]

# **Step 4: Merge All Data**
final_dt <- rbind(expanded_dt, matched_dt, fill = TRUE)

# **Step 5: Filter Correct Priority Tagging**
final_dt <- final_dt[
  !(P1P2P3 != "All" & Store_Type != P1P2P3)  # Ensure Store_Type matches P1P2P3
]

# **Print Final Data Table**
print(final_dt)





lifestyle_master_all = store_master[lifestyle_master_all, on=c('City'), allow =TRUE]
lifestyle_master_all<-lifestyle_master_all[, .(City, business_zone, FSN, BU)]
setnames(lifestyle_master_all, old = "business_zone", new = "DS")