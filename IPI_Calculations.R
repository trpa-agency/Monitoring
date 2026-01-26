## Calculate IPI ##
#Source: Instructions for Calculating Bioassessment Indices
#Created: October 21st, 2025
#Last Updated: December 11th, 2025
#Evelyn Malamut, Tahoe Regional Planning Agency
#This R Markdown was developed to calculate CSCI scores with yearly bioassessment data
#This R script uses R version 4.5.1

## Initial Installation ##

#install.packages("devtools")
#install.packages("dplyr)
#install.packages("tidyverse")
#install_github("SCCWRP/PHAB")

## Load libraries ##

library(dplyr)
library(tidyverse)
library(devtools)
library(PHAB)

## USER INPUT: File paths ##
phab_path <- "C:/Users/emalamut/BMI_24_Processing/TRPA_2024_PHAB_metrics_20250428.csv"
station_path <- "C:/Users/emalamut/BMI_24_Processing/Indices_Metrics_Consolidated.csv"
output_path <- "C:/Users/emalamut/BMI_24_Processing/IPI_report_2024.csv"

#Load and format phab data
#Create SampleID column

phab_metrics <- read.csv(phab_path, stringsAsFactors = F) %>%
  select(
    StationCode, 
    SampleDate, 
    SampleAgencyCode, 
    Variable, 
    Result, 
    Count_Calc
    ) %>%
  mutate(
    PHAB_SampleID = paste(StationCode, SampleDate, sep="_") #create SampleID
  )

#Load and format station data
#Filter for the stations that have associated phab information

phab_stations <- read.csv(station_path, stringsAsFactors = F) %>%
  select(
    StationCode, 
    New_Lat, 
    New_Long, 
    SITE_ELEV, 
    MAX_ELEV, 
    ELEV_RANGE, 
    AREA_SQKM, 
    PPT_00_09, 
    KFCT_AVE, 
    MEANP_WS, 
    MINP_WS
    ) %>%
  filter(StationCode %in% unique(phab_metrics$StationCode))

#Create Report

report <- IPI(stations = phab_stations, phab = phab_metrics)
write.csv(report, output_path)
