
mle_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/mle_master.csv", bucket = gcs_bucket2)
setDT(mle_master)

lifestyle_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/lifestyle_master.csv", bucket = gcs_bucket2)
setDT(lifestyle_master)

to_be_removed<-data.table(read_sheet('https://docs.google.com/spreadsheets/d/1Hhaoh6UaTbDu9KD8UnZrfgOrQEnUiAm68wwuhD0FyTw/edit?gid=0#gid=0',sheet ='Remove'))
setDT(to_be_removed)

to_be_removed$Key<-paste0(to_be_removed$FSN,to_be_removed$DS)
mle_master$key<-paste0(mle_master$FSN,mle_master$DS)
lifestyle_master$key<-paste0(lifestyle_master$FSN,lifestyle_master$DS)

mle_master1 <- mle_master %>%
  filter(!key %in% as.character(to_be_removed$Key))

lifestyle_master1 <- lifestyle_master %>%
  filter(!key %in% as.character(to_be_removed$Key))


lifestyle_master1=lifestyle_master1[,-c("key")]
mle_master1=mle_master1[,-c("key")]
lifestyle_master1<-unique(lifestyle_master1)
mle_master1<-unique(mle_master1)

fwrite(lifestyle_master1,paste0("D:\\shruti.shahi\\Outputs\\lifestyle\\lifestyle_master.csv"))
fwrite(mle_master1,paste0("D:\\shruti.shahi\\Outputs\\Master\\mle_master.csv"))

lifestyle_master1=lifestyle_master1[,-c("date")]
mle_master1=mle_master1[,-c("date")]

nav_master= gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_level_Master.csv", bucket = gcs_bucket2)
setDT(nav_master)
nav_master = nav_master[!BU %in% c("Lifestyle","EmergingElectronics","CoreElectronics","Mobile","LargeAppliances")]

nav_master<-rbind(nav_master,lifestyle_master1,mle_master1)

fwrite(nav_master,paste0("D:\\shruti.shahi\\Outputs\\Master\\DS_level_Master.csv"))
