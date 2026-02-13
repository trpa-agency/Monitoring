## Calculate IPI ##
#Source: Instructions for Calculating Bioassessment Indices
#Created: October 21st, 2025
#Last Updated: January 26th, 2026
#Evelyn Malamut, Tahoe Regional Planning Agency
#This R Markdown was developed to calculate IPI scores with yearly bioassessment data
#This R script uses R version 4.5.1

## Initial Installation ##

#install.packages("devtools")
#install.packages("dplyr")
#install.packages("tidyverse")
#install.packages("readxl")
#library(devtools) #Have to load the library before using install_github function
#install_github("SCCWRP/PHAB")

## Load libraries ##

library(dplyr)
library(tidyverse)
library(devtools)
library(PHAB)
library(readxl)

## USER INPUT: File paths ##
phab_path <- "F:/Research and Analysis/Fisheries/Streams/Bioassessment/2020/TRPA_PHAB_Metrics_2020.xlsx"
station_path <- "F:/Research and Analysis/Fisheries/Streams/Bioassessment/California Stream Condition Index/Physical Habitat Condition Index/2020 PHAB/Stations_20.csv"
output_path <- "F:/Research and Analysis/Fisheries/Streams/Bioassessment/California Stream Condition Index/Physical Habitat Condition Index/2020 IPI/2020_IPI_report.csv"

#Load and format phab data
#Create SampleID column

phab_metrics <- read_excel(phab_path) %>%
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

phab_metrics <- phab_metrics %>% filter(StationCode %in% unique(phab_stations$StationCode))

#Create Report

report <- IPI(stations = phab_stations, phab = phab_metrics)
write.csv(report, output_path)