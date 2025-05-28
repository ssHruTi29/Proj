prod_cat = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1mIxIfmVOJ1whZO4bDKD51Wlcqud2l2rzuOgbFt4ges4/edit?gid=782554625#gid=782554625',sheet ='product_categorization'))
setnames(prod_cat,old=c("product_id","title","analytic_business_unit","analytic_super_category","analytic_vertical"),new=c("FSN","Title","BU","SC","Analytical_Vertical"))
cms_analytical_vertical=prod_cat[,c("Analytical_Vertical","cms_vertical")]
prod_cat=prod_cat[,c("FSN","Title","BU","SC","Analytical_Vertical","brand")]
prod_cat=unique(prod_cat)

cms_analytical_vertical[, cms_vertical := tolower(cms_vertical)]
cms_analytical_vertical=unique(cms_analytical_vertical)

vertical_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/seller_inputs/TCR_HL_Seller_Mapping - vertical.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
vertical_master<-read.csv("temp_file.csv")
setDT(vertical_master)
vertical_master[, VERTICAL := tolower(VERTICAL)]
vertical_master=merge(vertical_master,cms_analytical_vertical,by.x="VERTICAL",by.y="cms_vertical",all.x=T)

brand_vertical=gcs_get_object("ipc/Nav/IPC_Hyperlocal/seller_inputs/TCR_HL_Seller_Mapping - brand_vertical.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
brand_vertical<-read.csv("temp_file.csv")
brand_vertical=merge(brand_vertical,cms_analytical_vertical,by.x="vertical",by.y="cms_vertical",all.x=T)

setDT(brand_vertical)
brand_vertical[, `:=`(vertical = tolower(vertical), brand = tolower(brand))]

fsn_master=gcs_get_object("ipc/Nav/IPC_Hyperlocal/seller_inputs/TCR_HL_FSN_Seller_Mapping.csv", bucket = gcs_bucket2,saveToDisk = "temp_file.csv",overwrite = TRUE)
fsn_master<-read.csv("temp_file.csv")

setDT(fsn_master)

prod_cat = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1mIxIfmVOJ1whZO4bDKD51Wlcqud2l2rzuOgbFt4ges4/edit?gid=782554625#gid=782554625',sheet ='product_categorization'))
setnames(prod_cat,old=c("product_id","title","analytic_business_unit","analytic_super_category","analytic_vertical"),new=c("FSN","Title","BU","SC","Analytical_Vertical"))
prod_cat=prod_cat[,c("FSN","Title","BU","SC","Analytical_Vertical","brand")]
prod_cat=unique(prod_cat)

STNs=read_xlsx("C:\\Users\\shruti.shahi\\Downloads\\Noise_STNs.xlsx")
setDT(STNs)
STNs[, BU := NULL]

STNs=merge(STNs,prod_cat,by="FSN",all.x=T)
STNs=STNs[!(STNs$FSN %in% fsn_master$fsn),]
STNs=STNs[!(STNs$Analytical_Vertical %in% vertical_master$Analytical_Vertical)]
STNs=STNs[!(paste0(STNs$Analytical_Vertical,STNs$brand) %in% paste0(brand_vertical$Analytical_Vertical,brand_vertical$brand))]

#write.csv(STNs,"C:\\Users\\shruti.shahi\\Downloads\\NL_to_DS_Sales_sellerremoval.csv")
write.csv(omg_fsns_prob_seller,"C:\\Users\\shruti.shahi\\Downloads\\omg_fsns_prob_seller_removal.csv")

omg_fsns = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1wTQg7VvzIBd9aKV48NwYhyBHhu4-je7JZgQu03_u9k8/edit?gid=1590178487#gid=1590178487',sheet ='Spike Calculation'))
omg_fsns_prob_seller=omg_fsns[omg_fsns$FSN %in% fsn_master$fsn,]
