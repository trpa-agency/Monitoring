## Bioassessment_SDE_MissingSurveys.R ##
#Created: January 26th, 2026
#Last Updated: January 26th, 2026
#Evelyn Malamut, Tahoe Regional Planning Agency
#This script was developed to add data from Bioassessment Surveys not already in SDE and update re-calculated CSCI scores
#This R script uses R version 4.5.1

##Notes: January 26th, 2026
## SDE has all sites included in CSCI_AllYears
## All re-calculated CSCI scores are updated in SDE

## Load libraries ##

library(dplyr)
library(tidyverse)
library(devtools)
library(readxl)

## File paths ##
CSCI_AllYears_path <- "F:/Research and Analysis/Fisheries/Streams/Bioassessment/California Stream Condition Index/California Stream Condition Index/CSCI_Scores_AllSites_AllYears.xlsx"
SDE_Updated_path <- "C:/Users/emalamut/Tahoe Regional Planning Agency/Science & Data Team - Documents/Monitoring/Programs/Bioassessment/Workspace/StreamSDE_Updated.xlsx"

CSCI_AllYears <- read_excel(CSCI_AllYears_path)
SDE_Updated <- read_excel(SDE_Updated_path)

CSCI_AllYears_Stations <- CSCI_AllYears %>%
  select(
    StationCode, 
    `Sample Year`
    ) %>%
  rename(STATION_CODE = StationCode, YEAR_OF_SURVEY = `Sample Year`)

SDE_Updated_Stations <- SDE_Updated %>%
  select(
    STATION_CODE, 
    YEAR_OF_SURVEY
  )

diff_df <- setdiff(CSCI_AllYears_Stations, SDE_Updated_Stations)

reverse_df <- setdiff(SDE_Updated_Stations, CSCI_AllYears_Stations)

selected_rows_CSCI <- semi_join(
  x = CSCI_AllYears, 
  y = diff_df, 
  by = c("StationCode" = "STATION_CODE", "Sample Year" = "YEAR_OF_SURVEY")
)

#CHECK for stations with same StationCode (so that coordinates match)
matching_stations <- intersect(diff_df$STATION_CODE, SDE_Updated$STATION_CODE)
matching_df <- data.frame(matching_stations = matching_stations)

#After writing, I deleted the SAX ones with the zero vs o situation and re-formatted to match SDE format
write.csv(selected_rows_CSCI, "C:/Users/emalamut/Desktop/SelectedRowsCSCI.csv")

# Updating re-calculated CSCI scores
CSCI_updated_scores <- CSCI_AllYears[!is.na(CSCI_AllYears$New_Score), ]
CSCI_updated_scores <- CSCI_updated_scores %>%
  select(
    StationCode, 
    `Sample Year`,
    Final_Score
  )
CSCI_updated_scores <- CSCI_updated_scores[order(CSCI_updated_scores$StationCode), ]
