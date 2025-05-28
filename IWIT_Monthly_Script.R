options(java.parameters = "-Xmx200000m")
library(RJDBC)
library(plyr)
library(dplyr)
library(lubridate)
library(data.table)

################ GCP Write Functions #################

gcs_save_rds <- function(input, output) {
  saveRDS(input, output)
}

gcs_save_csv <- function(input, output) {
  write.csv(input, output, row.names = FALSE)
}

project_id <- "fks-ip-azkaban"
auth_file <- "/usr/share/fk-retail-ipc-azkaban-exec/resources/fks-ip-azkaban-sak.json"
gcs_bucket <- "fk-ipc-data-sandbox"
gcs_bucket2 <- "fk-ipc-adhoc-data-sandbox"
Sys.setenv("GCS_DEFAULT_BUCKET" = gcs_bucket, "GCS_AUTH_FILE" = auth_file)
library(googleCloudStorageR)
library(httr)
options(googleCloudStorageR.upload_limit = 100000000000L)

############# Summary Functions ########################

pivot_table <- function(df, var_vec, value_vector, outputs) {
  df <- df[, c(var_vec, value_vector)]
  meltdata <- df %>% group_by(across(all_of(var_vec))) # Use dplyr for grouping
  pivot <- meltdata %>% summarise(across(all_of(value_vector), funs(sum)), .groups = 'drop')
  return(as.data.frame(pivot))
}

end_date <- Sys.Date()  # Current date
start_date <- end_date - 30  # 30 days ago

Start_Date <- paste0(start_date, " ", "00:00:00")
End_Date <- paste0(end_date, " ", "11:59:59")
Cancel_Date <- Start_Date  # Using the same range for cancellation data
IStart_Date <- Sys.Date() - 30

# Getting Master_FC_List
FC_Ref <- gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2)
FC_Ref <- FC_Ref[c(2)]

Dest_FC_Ref1 <- toString(shQuote(FC_Ref$DS))

Pallet_Verticals <- gcs_get_object("ipc/Nav/Inputs/Pallet_Vertical_Reference.csv", bucket = gcs_bucket2)
Pallet_Verticals <- subset(Pallet_Verticals, select = c('SuperCategory', 'Vertical', 'Pallet_Vertical'))

# Getting IWIT Data
runQuery <- paste0("SELECT a.stn_id as STN_Id,
                  a.fsn as fsn,
                  a.sku_id as SKU,
                  a.sc_company as Seller,
                  a.src_warehouse as Src_FC,
                  a.dest_warehouse as Dest_FC,
                  a.src_warehouse_area as Src_WH_Area,
                  a.dest_warehouse_area as Dest_WH_Area,
                  a.available_qty as Reserved_Qty,
                  a.dispatched_qty as Dispatched_Qty,
                  a.accepted_qty as Accepted_Qty,
                  a.status as Dispatch_Status,
                  a.created_at as Creation_Date,
                  a.updated_at as Dispatch_Date,
                  a.user as Created_By,
                  a.error as Creation_Error,
                  b.status as Inwarding_Status,
                  b.updated_at as Inward_Date
                  FROM transfer_item as a
                  LEFT JOIN transfer_note as b
                  ON a.stn_id = b.stn_id
                  WHERE a.created_at >= '", Start_Date, "' AND
                  a.created_at <= '", End_Date, "' AND
                  a.dest_warehouse in (", Dest_FC_Ref1, ") AND
                  a.available_qty > 0 AND
                  a.src_warehouse_area = 'store' AND
		  (b.status IS NULL OR b.status != 'CANCELLED')")

runQuery_New <- paste0("SELECT c.reason,
       		  c.limiting_factor,
                  c.source_slot_start_time,
                  c.destination_slot_start_time,
		  a.stn_id as STN_Id,
                  a.fsn as fsn,
                  a.sku_id as SKU,
                  a.sc_company as Seller,
                  a.src_warehouse as Src_FC,
                  a.dest_warehouse as Dest_FC,
		  a.src_warehouse_area as Src_WH_Area,
		  a.dest_warehouse_area as Dest_WH_Area,
                  a.requested_qty as Requested_Qty,                         
                  a.created_at as Creation_Date,
                  a.user as Created_By
                  FROM entity_slot_attribution as c
                  LEFT JOIN transfer_item as a
                  ON c.entity_id=a.stn_id
                  WHERE a.created_at >='",Cancel_Date,"'AND
                  a.created_at <='",End_Date,"'AND
                  a.src_warehouse_area ='store'")

drv <- JDBC("com.mysql.cj.jdbc.Driver", "/usr/share/fk-retail-ipc-azkaban-exec/drivers/mysql/mysql-connector-java.jar")
conn <- dbConnect(drv, "jdbc:mysql://master.retl-proc-ob-retail-ob.prod.altair.fkcloud.in:3306/iwit?useSSL=false", "v-reta-7rTsuh4SK", "IsYbZhK-pA0bwW24-lsB")	
IWIT_Db <- dbGetQuery(conn, runQuery)
IWIT_Cancel_Db <- dbGetQuery(conn, runQuery_New)
on.exit(RJDBC::dbDisconnect(conn))

# Handle NA values
IWIT_Db[is.na(IWIT_Db)] <- 0

# Convert date columns to Date format
##IWIT_Db$Creation_Date <- as.Date(IWIT_Db$Creation_Date)
##IWIT_Db$Dispatch_Date <- as.Date(IWIT_Db$Dispatch_Date)
##IWIT_Db$Inward_Date <- as.Date(IWIT_Db$Inward_Date)

runQuery_New2<-paste0("select d.entity_id as STN_Id, d.value as Sch_Details from entity_attributes as d where d.key = 'slot_detail'")

drv <- JDBC("com.mysql.cj.jdbc.Driver", "/usr/share/fk-retail-ipc-azkaban-exec/drivers/mysql/mysql-connector-java.jar")
conn <- dbConnect(drv, "jdbc:mysql://master.retl-proc-ob-retail-ob.prod.altair.fkcloud.in:3306/iwit?useSSL=false", "v-reta-7rTsuh4SK", "IsYbZhK-pA0bwW24-lsB")

entity_data <- dbGetQuery(conn, runQuery_New2)
on.exit(RJDBC::dbDisconnect(conn))

entity_data<-as.data.frame(entity_data)

IWIT_data<-join(IWIT_Db,entity_data,by=c("STN_Id"),match=c("first"))

IWIT_data<-as.data.frame(IWIT_data)

IWIT_data <- IWIT_data %>% filter(!(Inwarding_Status %in% c("ERROR", "INVALID","CANCELLED")))

rm(entity_data)

# Getting FSN Attributes
Inv_Data <- gcs_get_object("ipc/Nav/Data/Inv_Data.rds", parseFunction = gcs_parse_rds)
Inv_Data <- Inv_Data %>% filter(Seller == 'ALPHA') %>% 
  select(product_fsn, product_title, BusinessUnit, SuperCategory, Category, Vertical, CMSVertical, brand) %>%
  distinct(product_fsn, .keep_all = TRUE)

# Merging IWIT data with Inventory data
IWIT_data <- merge(IWIT_data, Inv_Data, by.x = c("fsn"), by.y = c("product_fsn"), all.x = TRUE)

rm(Inv_Data)

IWIT_data <- merge(IWIT_data, Pallet_Verticals, by = c("SuperCategory", "Vertical"), all.x = TRUE)

rm(Pallet_Verticals)

# Fill NA values
IWIT_data$Pallet_Vertical[is.na(IWIT_data$Pallet_Vertical)] <- "Non Palletized"

# Merging with FC reference
names(FC_Ref) <- paste0("Src_", names(FC_Ref))


IWIT_data <- merge(IWIT_data, FC_Ref, by.x = c("Dest_FC"),by.y=c("Src_DS"), all.x = TRUE)
##names(FC_Ref) <- gsub("Src_", "Dest_", names(FC_Ref))
##IWIT_data <- merge(IWIT_data, FC_Ref, by = "Dest_FC", all.x = TRUE)

# Calculate Intransit
IWIT_data$IWIT_Intransit <- pmax(IWIT_data$Reserved_Qty - IWIT_data$Accepted_Qty, 0)


# Filter out erroneous data
#IWIT_data <- IWIT_data %>% filter(is.na(Creation_Error))


# Adding new date column DBD from Sch_Details
db1 <- ifelse(substr(IWIT_data$Sch_Details, 75, 78) == "null", substr(IWIT_data$Sch_Details, 108, 117), substr(IWIT_data$Sch_Details, 125, 134))
db2 <- as.data.frame(db1)
colnames(db2) <- c("DBD")
db2$DBD1 <- as.Date(with(db2, paste(substr(db2$DBD, 1, 4), substr(db2$DBD, 6, 7), substr(db2$DBD, 9, 10), sep = "-")), "%Y-%m-%d")
IWIT_data <- cbind(IWIT_data, db2["DBD1"])


# Get time series data and merge
##tsdb <- gcs_get_object("ipc/Nav/Anuj/IWIT_TS.csv")
##tsdb <- tsdb %>% select(stn_id, intransit_ts)
##tsdb$intransit_ts <- as.Date(tsdb$intransit_ts)
##colnames(tsdb) <- c("STN_Id", "intransit_ts")
##IWIT_data <- merge(IWIT_data, unique(tsdb), by = "STN_Id", all.x = TRUE)

# Update Dispatch_Date based on Intransit data
##IWIT_data$Dispatch_Date <- ifelse(is.na(IWIT_data$intransit_ts), IWIT_data$Dispatch_Date, IWIT_data$intransit_ts)

# Filter for valid Inwarding_Status

# Upload to GCS
gcs_upload(IWIT_data, name = "ipc/Nav/Hyperlocal_IPC/DS_IT.csv", object_function = gcs_save_csv)


