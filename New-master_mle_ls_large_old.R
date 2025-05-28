
library(dplyr)
library(tidyr)

ds_master= data.table(gcs_get_object("ipc/Nav/IPC_Hyperlocal/DS_Master.csv", bucket = gcs_bucket2))
#setnames(ds_master, old ='DS',new='business_zone')
ds_master_summary=ds_master %>% group_by(City) %>% mutate(ds_count=n())


####Electronics
EE<-data.table(read_sheet("https://docs.google.com/spreadsheets/d/14TufYGLxTfv1vYSQYGcsdYzRKzz1HjcdKaS2WIIQSwA/edit?pli=1&gid=1589894909#gid=1589894909",sheet="Electronics"))

# Step 1: Expand cities
expanded_EE <- EE %>%
  mutate(City = ifelse(City == "all", list(unique(ds_master$City)), City)) %>%
  unnest(City)  

# Step 2: Expand ds
expanded_EE <- expanded_EE %>%
  left_join(ds_master %>% select(City, DS) %>% distinct(), by = "City") %>% 
  mutate(DS = ifelse(DS.x == "all", DS.y, DS.x)) %>%  # Keep original if not "all", else take all from df1
  select(FSN, City, DS, `Store Type`)  # Clean up


# Step 3: Filter priority_type properly
expanded_EE <- expanded_EE %>%
  left_join(
    ds_master %>%
      select(DS, `Store Type`) %>%
      distinct(),
    by = "DS"
  ) %>%
  filter(`Store Type.x` == "all" | `Store Type.x` == `Store Type.y`)  # Remove duplicates

# View final expanded dataframe
print(expanded_df2)



