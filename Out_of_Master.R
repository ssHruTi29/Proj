
#atp = data.table(fread("E:\\Vishruti\\Hyperlocal\\HL Final  inventory_snapshot_19 Nov 2024 04_36_58 .csv"))
atp = gcs_get_object("ipc/Nav/Hyperlocal_IPC/Inventory_Snapshot.csv", bucket = gcs_bucket)
#atp<-read.csv("temp_file.csv")
setDT(atp)
atp = atp[fc_area == 'store']
Inv = atp[,.SD]


ds_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
setDT(ds_master)

###atp_for_ds
setDT(atp)
setDT(ds_master)
atp_ds <- atp[fc %in% ds_master$DS]
##Mapping_the_product_categorization

prod_cat = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1mIxIfmVOJ1whZO4bDKD51Wlcqud2l2rzuOgbFt4ges4/edit?gid=782554625#gid=782554625',sheet ='product_categorization'))
setDT(prod_cat)
setnames(prod_cat, 
         old = c("product_id", "title", "analytic_super_category", "analytic_business_unit", "analytic_vertical"), 
         new = c("FSN", "Title", "SC", "BU", "Analytical_Vertical"))
prod_cat<-prod_cat[,c("FSN","Title","brand","Analytical_Vertical","SC","BU")]
setnames(prod_cat,old=c("FSN"),new=c("fsn"))

#temp<-total_lifestyle
#total_lifestyle<-total_lifestyle[,-c("Title", "SC", "BU", "Analytical_Vertical","brand")]
atp_updated<-merge(atp_ds,prod_cat,by=("fsn"),all.x=T)
atp_updated<-unique(atp_updated)
atp_Ls<-atp_updated[atp_updated$BU=="LifeStyle",]


lifestyle_master_ds = gcs_get_object("ipc/Nav/IPC_Hyperlocal/lifestyle_master_backup.csv", bucket = gcs_bucket2)

LS_out_of_master<-atp_Ls[!(atp_Ls$fsn %in% lifestyle_master_ds$FSN)]
write.csv(LS_out_of_master,"D:\\shruti.shahi\\Outputs\\lifestyle\\LS_out_of_master.csv")
