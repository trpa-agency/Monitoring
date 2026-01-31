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
library(writexl)
library(lubridate)

## File paths ##
CEDEN_CSCI_IPI_path <- "C:/Users/emalamut/Tahoe Regional Planning Agency/Science & Data Team - Documents/Monitoring/Programs/Bioassessment/Workspace/CEDEN_CSCI_IPI_2025_Export.xlsx"
SDE_Updated_path <- "C:/Users/emalamut/Tahoe Regional Planning Agency/Science & Data Team - Documents/Monitoring/Programs/Bioassessment/Workspace/StreamSDE_Updated.xlsx"

CEDEN_CSCI_IPI_ALL <- read_excel(CEDEN_CSCI_IPI_path)
SDE_Updated <- read_excel(SDE_Updated_path)

# Seelect 634 sites
CEDEN_CSCI_IPI <- CEDEN_CSCI_IPI_ALL[startsWith(CEDEN_CSCI_IPI_ALL$StationCode, "634"), ] %>%
  select(
    Project,
    SampleAgency,
    StationCode,
    StationName,
    TargetLatitude,
    TargetLongitude,
    SampleDate,
    Analyte,
    Result
  )

write_xlsx(CEDEN_CSCI_IPI, "C:/Users/emalamut/Desktop/CEDEN_CSCI_IPI_634.xlsx")

#Create df with stations that exist in CEDEN but not in SDE
CEDEN_diff_list <- list(
  StationCode = setdiff(unique(CEDEN_CSCI_IPI$StationCode), unique(SDE_Updated$STATION_CODE))
)

CEDEN_diff <- as.data.frame(CEDEN_diff_list)

#Create df with stations that exist in SDE but not in CEDEN
SDE_diff_list <- list(
  StationCode = setdiff(unique(SDE_Updated$STATION_CODE), unique(CEDEN_CSCI_IPI$StationCode))
)

SDE_diff <- as.data.frame(SDE_diff_list)


# Formatting CEDEN output to append non-TRPA sampled sites
CEDEN_CSCI <- CEDEN_CSCI_IPI[CEDEN_CSCI_IPI$Analyte == "CSCI", ] %>%
  mutate(
    YEAR_OF_SURVEY = year(SampleDate)
  ) %>%
  select(
    STATION_CODE = StationCode,
    STATION_NAME = StationName,
    SAMPLE_DATE = SampleDate,
    YEAR_OF_SURVEY,
    LONGITUDE = TargetLongitude,
    LATITUDE = TargetLatitude,
    AGENCY = SampleAgency,
    CSCI = Result
  )
  
CEDEN_IPI <- CEDEN_CSCI_IPI[CEDEN_CSCI_IPI$Analyte == "IPI", ] %>%
  mutate(
    YEAR_OF_SURVEY = year(SampleDate)
  ) %>%
  select(
    STATION_CODE = StationCode,
    STATION_NAME = StationName,
    SAMPLE_DATE = SampleDate,
    YEAR_OF_SURVEY,
    LONGITUDE = TargetLongitude,
    LATITUDE = TargetLatitude,
    AGENCY = SampleAgency,
    IPI = Result
  ) 

formatted_CEDEN_CSCI_IPI <- full_join(
  x = CEDEN_CSCI, 
  y = CEDEN_IPI, 
  by = c("STATION_CODE", "STATION_NAME", "SAMPLE_DATE", "YEAR_OF_SURVEY", "AGENCY", "LONGITUDE", "LATITUDE")
)

joined_df <- full_join(
  x = formatted_CEDEN_CSCI_IPI, 
  y = SDE_Updated, 
  by = c("STATION_CODE", "YEAR_OF_SURVEY")
)

test_df <- joined_df %>%
  mutate(
    AGENCY.y = coalesce(AGENCY.y, AGENCY.x),
    LONGITUDE.y = ifelse(AGENCY.y != "TRPA", LONGITUDE.x, LONGITUDE.y),
    LATITUDE.y = ifelse(AGENCY.y != "TRPA", LATITUDE.x, LATITUDE.y),
    CSCI_SCORE = if_else(AGENCY.y != "TRPA", CSCI, CSCI_SCORE),
    STREAM_NAME = if_else(AGENCY.y != "TRPA", STATION_NAME, STREAM_NAME),
    STATION_TYPE = if_else(AGENCY.y != "TRPA", "NA", STATION_TYPE),
    LTINFO = if_else(AGENCY.y != "TRPA", "NA", LTINFO),
    IPI_SCORE = IPI
  )

SDE_w_CEDEN <- test_df %>%
  select(
    STATION_CODE = STATION_CODE,
    STREAM_NAME,
    LONGITUDE = LONGITUDE.y,
    LATITUDE = LATITUDE.y,
    SAMPLE_DATE,
    YEAR_OF_SURVEY,
    CSCI_SCORE,
    IPI_SCORE,
    AGENCY = AGENCY.y,
    LTINFO
  )
