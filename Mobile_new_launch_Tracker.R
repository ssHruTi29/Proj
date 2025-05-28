

new_launch_mobile = data.table(read_sheet('https://docs.google.com/spreadsheets/d/14TufYGLxTfv1vYSQYGcsdYzRKzz1HjcdKaS2WIIQSwA/edit?gid=1213488504#gid=1213488504',sheet='New_launch'))
new_launch_mobile$`Sale Date`=as.character(new_launch_mobile$`Sale Date`)
new_launch_mobile=new_launch_mobile[,-c("Sale Date")]
new_launch_mobile=unique(new_launch_mobile)

##Creating new launch ds combination
ds_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
Live_store=ds_master[ds_master$Live==1,]
Live_store=Live_store[,c("City","DS")]

#new_Launch_Mater=merge(new _launch_mobile,Live_store,by=NULL)
new_Launch_Master=crossing(new_launch_mobile,Live_store)

####Adding moto launch ###################


####Adding pixel 9A launch ###################


#####ATP
atp = gcs_get_object("ipc/Nav/Hyperlocal_IPC/Inventory_Snapshot.csv", bucket = gcs_bucket)
setDT(atp)
atp = atp[fc_area == 'store']
Inv = atp[,.SD]

####NL - set 1 (main FCs)
nl_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/nl_hub.csv", bucket = gcs_bucket2)
setDT(nl_master)
setnames(nl_master, old =c('code','city'),new=c('nl_code','City'))
new_Launch_Master = nl_master[new_Launch_Master, on =c('City')]

###SH details 
sh_master = gcs_get_object("ipc/Nav/IPC_Hyperlocal/sourcing_hub.csv", bucket = gcs_bucket2)
setDT(sh_master)
sh_master=sh_master[,c("code","city")]
setnames(sh_master, old =c('code','city'),new=c('sh_code','City'))
new_Launch_Master = sh_master[new_Launch_Master, on =c('City')]

####DS ATP
setDT(atp)
atp = atp[,.(atp = sum(atp)),by =.(fsn,fc)]
setnames(atp, old =c('fsn','fc'),new=c('FSN','DS'))
new_Launch_Master = atp[new_Launch_Master , on =c('FSN','DS')]
new_Launch_Master = new_Launch_Master[,`:=`(atp = ifelse(is.na(atp),0,atp))]

###nl_atp
####Adding NL Atp
setnames(atp, old ='atp',new ='nl_atp')
setnames(atp, old ='DS',new ='nl_code')
new_Launch_Master = atp[new_Launch_Master , on =c('FSN','nl_code')]
new_Launch_Master[,`:=`(nl_atp = ifelse(is.na(nl_atp),0,nl_atp))]
new_Launch_Master$Flag=1
new_Launch_Master[,`:=`(sum_check=sum(Flag)),by=.(FSN,nl_code)]
new_Launch_Master$nl_atp_pivot=new_Launch_Master$nl_atp/new_Launch_Master$sum_check

##sh_atp
setnames(atp, old ='nl_atp',new ='sh_atp')
setnames(atp, old ='nl_code',new ='sh_code')
new_Launch_Master = atp[new_Launch_Master , on =c('FSN','sh_code')]
new_Launch_Master[,`:=`(nl_atp = ifelse(is.na(nl_atp),0,nl_atp))]
new_Launch_Master$Flag=1
new_Launch_Master[,`:=`(sum_check=sum(Flag)),by=.(FSN,sh_code)]
new_Launch_Master$sh_atp_pivot=new_Launch_Master$sh_atp/new_Launch_Master$sum_check

######IWIT Data
ds_it = gcs_get_object("ipc/Nav/Hyperlocal_IPC/DS_IT.csv", bucket = gcs_bucket)
setDT(ds_it)
ds_it_launch=ds_it[ds_it$fsn %in% c(new_Launch_Master$FSN),]
write.csv(ds_it_launch,"D:\\shruti.shahi\\Outputs\\ds_it_launch.csv")
#ds_it = ds_it[Inwarding_Status %in% c('IN_TRANSIT','INWARDING','INITIATED')]
ds_it[,`:=`(Creation_Date = as.POSIXct(Creation_Date, format="%Y-%m-%d %H:%M:%S"))]
ds_it[,`:=`(days = as.numeric(Sys.time()-Creation_Date,units = "days"))]

ds_iwit = ds_it[,.(iwit = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]

####3NL to SH Intransit
###NL to sh

nl_to_sh = data.table(gcs_get_object("ipc/Nav/Anuj/IWIT_InTransit_FSN.csv", bucket = gcs_bucket))
nl_to_sh = nl_to_sh[Src_FC %in% unique(nl_master$nl_code)]
nl_to_sh = nl_to_sh[Dest_FC %in% unique(sh_master$sh_code)]
ds_iwit_sh = nl_to_sh[,.(iwit = sum(IWIT_Intransit)), by =.(fsn,Dest_FC,Src_FC)]


###Adding Patch for NL to DS
setnames(ds_iwit,old='Src_FC',new='nl_code')
setnames(ds_iwit,old='Dest_FC',new='DS')
setnames(ds_iwit,old='fsn',new='FSN')
setnames(ds_iwit,old='iwit',new='nl_ds_iwit')
new_Launch_Master = ds_iwit[new_Launch_Master, on =c('FSN','DS','nl_code')]
new_Launch_Master[,`:=`(nl_ds_iwit = ifelse(is.na(nl_ds_iwit),0,nl_ds_iwit))]

###Adding Patch for SH to DS
setnames(ds_iwit,old='nl_code',new='sh_code')
setnames(ds_iwit,old='nl_ds_iwit',new='sh_ds_iwit')
new_Launch_Master = ds_iwit[new_Launch_Master, on =c('FSN','DS','sh_code')]
new_Launch_Master[,`:=`(sh_ds_iwit = ifelse(is.na(sh_ds_iwit),0,sh_ds_iwit))]

###Adding Patch for NL to SH
setnames(ds_iwit_sh,old='Src_FC',new='nl_code')
setnames(ds_iwit_sh,old='Dest_FC',new='sh_code')
setnames(ds_iwit_sh,old='fsn',new='FSN')
setnames(ds_iwit_sh,old='iwit',new='nl_sh_iwit')
new_Launch_Master = ds_iwit_sh[new_Launch_Master, on =c('FSN','sh_code','nl_code')]
new_Launch_Master[,`:=`(nl_sh_iwit = ifelse(is.na(nl_sh_iwit),0,nl_sh_iwit))]
new_Launch_Master$nl_sh_iwit=new_Launch_Master$nl_sh_iwit/new_Launch_Master$sum_check


#####STNs required
new_Launch_Master$Requirement = as.numeric(str_extract(new_Launch_Master$`Placement Logic`, "\\d+"))
new_Launch_Master[,`:=`(Requirement = ifelse(is.na(Requirement),2,Requirement))]
setDT(new_Launch_Master)
new_Launch_Master[,`:=`(Ds_deficit=Requirement-atp-nl_ds_iwit-nl_sh_iwit-sh_ds_iwit)]
new_Launch_Master[, STN_nl_to_ds := ifelse(Ds_deficit > 0, ifelse(nl_atp_pivot > Ds_deficit, Ds_deficit, pmax(0, nl_atp_pivot)), 0)]
new_Launch_Master[, STN_sh_to_ds := ifelse(Ds_deficit > 0, ifelse(sh_atp_pivot > Ds_deficit, Ds_deficit, pmax(0, sh_atp_pivot)), 0)]
new_Launch_Master[, STN_sh_to_ds := ifelse(Ds_deficit > 0, ifelse(sh_atp_pivot > Ds_deficit, Ds_deficit, pmax(0, sh_atp_pivot)), 0)]

new_Launch_Master[, instock := ifelse(atp > 0,1,0)]
new_Launch_Master[, instock_intransit := ifelse((atp+nl_ds_iwit+sh_ds_iwit) > 0,1,0)]

write_sheet(new_Launch_Master,'https://docs.google.com/spreadsheets/d/1efzp8KAr0FtjY2W9FTVz-49q-pztgJvFbte9O2HHCBw/edit?gid=125435566#gid=125435566',sheet='New_Launch_data')

#large_appliances = data.table(read_sheet('https://docs.google.com/spreadsheets/d/1SMTEsFl8ySSsw1G7qQ2jsajoJ5F0CQKW2Vn2lsbal9I/edit?gid=1244990083#gid=1244990083',sheet='LargeAppliances'))
