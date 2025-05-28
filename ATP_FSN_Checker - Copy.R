
####Remove_Laptop_from_DS
atp_ds=atp %>% filter(nl_code2 %in% ds_master$business_zone)
prod_cat = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1mIxIfmVOJ1whZO4bDKD51Wlcqud2l2rzuOgbFt4ges4/edit?gid=782554625#gid=782554625',sheet ='product_categorization'))
setnames(prod_cat , old ='product_id',new='FSN')
#prod_cat =prod_cat[,.(FSN,analytic_business_unit)]
atp_mapped =prod_cat[atp_ds, on=c('FSN')]
atp_mapped<-unique(atp_mapped)
atp_mapped_Laptop<-atp_mapped[analytic_vertical %in% 'Laptop', ]

write.csv(atp_mapped_Laptop,"D:\\shruti.shahi\\Outputs\\Laptop_ds_to_SH.csv")



####Remove_Laptop_from_SH
atp_sh=atp %>% filter(nl_code2 %in% sh_master$sh_code)
prod_cat = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1mIxIfmVOJ1whZO4bDKD51Wlcqud2l2rzuOgbFt4ges4/edit?gid=782554625#gid=782554625',sheet ='product_categorization'))
setnames(prod_cat , old ='product_id',new='FSN')
#prod_cat =prod_cat[,.(FSN,analytic_business_unit)]
atp_mapped =prod_cat[atp_sh, on=c('FSN')]
atp_mapped<-unique(atp_mapped)
atp_mapped_Laptop<-atp_mapped[analytic_vertical %in% 'Laptop', ]

write.csv(atp_mapped_Laptop,"D:\\shruti.shahi\\Outputs\\Laptop_SH_to_NL.csv")
#write.csv(atp_mapped_LS,"D:\\shruti.shahi\\Outputs\\Laptop_ds_to_SH.csv")


# mle_nav_master = mle_nav_master[,`:=`(BU = ifelse(is.na(BU),analytic_business_unit,BU))]
# mle_nav_master = mle_nav_master[,`:=`(analytic_business_unit = NULL)]
# final_master = rbind(mle_nav_master,nav_master)