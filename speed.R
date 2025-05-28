library(stringr)
library(lubridate)
library(dplyr)
library(readxl)

######################## Recommendation data ###########################

# List all CSV files in the folder
csv_files <- list.files(path = "C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\Zonal", pattern = "\\.csv$", full.names = TRUE)

date_pattern <- "\\b\\d{2}-\\d{2}-\\d{2}\\b"
empty_df <- data.frame(lapply(names(data), function(x) character()))

final_data <- data.frame()
# Loop through each CSV file and read it
for (csv_file in csv_files) {
  data <- read.csv(csv_file)
  date_match <- str_match(csv_file, "(\\d{2}-\\d{2}-\\d{2})")
  # Check if a match was found
  if (!is.na(date_match[1, 2])) {
    date <- as.Date(date_match[1, 2],format = "%d-%m-%y")
  } 
  data$Date<-date
  final_data<-rbind(final_data,data)
  print("Done1")
  
}
final_data_summary <- final_data %>%
  group_by(Date) %>%
  summarise(FSN_4zone = sum(n_distinct(fsn[fsn_zone == "4"])),FSN_3zone = sum(n_distinct(fsn[fsn_zone == "3"])),FSN_2zone = sum(n_distinct(fsn[fsn_zone == "2"])),FSN_1zone = sum(n_distinct(fsn[fsn_zone == "1"])))  # Filter within summarise

write.csv(final_data_summary,"C:\\Users\\shruti.shahi\\Desktop\\BGM\\Analytics\\Output\\speed_010724.csv",row.names = FALSE)
