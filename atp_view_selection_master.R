###atp_data

oriniginal_master<-gcs_get_object("ipc/Nav/Hyperlocal_IPC/selection_master_ds_mle_ls.csv", bucket = gcs_bucket)
oriniginal_master <- fread(text = rawToChar(oriniginal_master))
setDT(oriniginal_master)



master = gcs_get_object("ipc/Nav/Hyperlocal_IPC/selection_master_ds_mle_ls.csv", bucket = gcs_bucket)
master <- fread(text = rawToChar(master))
setDT(master)

master<-unique(master)
# master_wide <- dcast(master, FSN + business_zone ~ nl_code, value.var = "nl_atp")
# df_wide <- dcast(master, FSN + business_zone ~ nl_cod, value.var = "nl_atp")  # Typo fixed here

master_wide <- dcast(master, ... ~ nl_code, value.var = "nl_atp")
master_wide <- dcast(master_wide, ... ~ nl_code2, value.var = "nl2_atp")

master_wide$ban_ven_wh_nl_01nl<-master_wide$ban_ven_wh_nl_01nl/master_wide$sum_check
master_wide$bhi_pad_wh_nl_01nl<-master_wide$bhi_pad_wh_nl_01nl/master_wide$sum_check
master_wide$frk_bts<-master_wide$frk_bts/master_wide$sum_check
master_wide$nad_har_wh_kl_nl_01nl<-master_wide$nad_har_wh_kl_nl_01nl/master_wide$sum_check

master_wide$bhi_vas_wh_nl_01nl<-master_wide$bhi_vas_wh_nl_01nl/master_wide$sum_check
master_wide$gur_san_wh_nl_01nl<-master_wide$gur_san_wh_nl_01nl/master_wide$sum_check
master_wide$malur_bts<-master_wide$malur_bts/master_wide$sum_check
master_wide$ulub_bts<-master_wide$ulub_bts/master_wide$sum_check