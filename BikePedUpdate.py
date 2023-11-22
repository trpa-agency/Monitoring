"""
BikePedUpdate.py
Created: March 11th, 2022
Last Updated: March 11th, 2022
Mason Bindl, Tahoe Regional Planning Agency
Andrew McClary, Tahoe Regional Planning Agency
This python script was developed to transform data from Trafx and Ecovision

This script uses Python 3.x and was designed to be used with 
the default ArcGIS Pro python enivorment "arcgispro-py3-clone", with
no need for installing new libraries.
"""
#--------------------------------------------------------------------------------------------------------#
# import packages and modules
import arcpy
from datetime import datetime
import os
import sys
import pyodbc
import pandas as pd
from arcgis.features import FeatureSet, GeoAccessor, GeoSeriesAccessor
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# set overwrite to true
arcpy.env.overwriteOutput = True

# in memory output file path
wk_memory = "memory" + "\\"

# set workspace and sde connections 
working_folder = "C:\GIS"
workspace = "F:/gis/PROJECTS/ResearchAnalysis/Monitoring/Data/BikePed/"
arcpy.env.workspace = "F:/gis/PROJECTS/ResearchAnalysis/Monitoring/Data/BikePed/"
# network path to connection files
filePath = "C:\\GIS\\DB_CONNECT"

# database file path 
sdeBase = os.path.join(filePath, "Vector.sde")

# make sql database connection to BMP with pyodbc
sdeTabular = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=sql12;DATABASE=sde_tabular;UID=sde;PWD=staff')

# Final table to update
outTable = "sde_tabular.SDE.bike_ped_counter_tabular"

# start a timer for the entire script run
FIRSTstartTimer = datetime.now()

# Create and open log file.
complete_txt_path = os.path.join(working_folder, "BikePed_ETL_Log.txt")
print (complete_txt_path)
log = open(complete_txt_path, "w")

# Write results to txt file
log.write("Log: " + str(FIRSTstartTimer) + "\n")
log.write("\n")
log.write("Begin process:\n")
log.write("Process started at: " + str(FIRSTstartTimer) + "\n")
log.write("\n")

#---------------------------------------------------------------------------------------#
## GET DATA
#---------------------------------------------------------------------------------------#
# start timer for the get data requests
startTimer = datetime.now()
# create a dataframe from the counter feature class to use in creating the counter ids
counters = sdeBase + "\\sde.SDE.Transportation\\sde.SDE.bike_ped_counter_spatial"
sdfBike = pd.DataFrame.spatial.from_featureclass(counters)
# Get Bike Ped SQL Table
dfBikeOG = pd.read_sql("SELECT * FROM sde_tabular.SDE.bike_ped_counter_tabular", sdeTabular)
# export OG data to workspace
dfBikeOG.to_csv(os.path.join(workspace,"BikePed_Original.csv"))

# read in CSV of OG data
dfBike = pd.read_csv(os.path.join(workspace,"BikePed_Original.csv"))

#read in Trafx data download
trafx = pd.read_csv(os.path.join(workspace, "trafx_daily.csv"))

# read in Ecovision data download
eco = pd.read_csv(os.path.join(workspace, "ecovision_daily.csv"))
       
# report how long it took to get the data
endTimer = datetime.now() - startTimer
print("\nTime it took to get the data: {}".format(endTimer))   
log.write("\nTime it took to get the data: {}".format(endTimer)) 
#---------------------------------------------------------------------------------------#
## Define Functions ##
#---------------------------------------------------------------------------------------#
##--------------------------------------------------------------------------------------------------------#
## SEND EMAIL WITH LOG FILE ##
##--------------------------------------------------------------------------------------------------------#
# path to text file
fileToSend = complete_txt_path

# email parameters
subject = "Bike/Ped ETL Log File"
sender_email = "infosys@trpa.org"
# password = ''
receiver_email = "GIS@trpa.gov; bvollmer@trpa.gov"

# send mail function
def send_mail(body):
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = receiver_email

    msgText = MIMEText('%s<br><br>Cheers,<br>GIS Team' % (body), 'html')
    msg.attach(msgText)

    attachment = MIMEText(open(fileToSend).read())
    attachment.add_header("Content-Disposition", "attachment", filename = os.path.basename(fileToSend))
    msg.attach(attachment)

    try:
        with smtplib.SMTP("mail.smtp2go.com", 25) as smtpObj:
            smtpObj.ehlo()
            smtpObj.starttls()
#             smtpObj.login(sender_email, password)
            smtpObj.sendmail(sender_email, receiver_email, msg.as_string())
    except Exception as e:
        print(e)

# replaces features in outfc with exact same schema
def updateSDE(inputfc,outfc, fieldnames):
        # disconnect all users
    print("\nDisconnecting all users...")
    arcpy.DisconnectUser(sde, "ALL")

    # deletes all rows from the SDE feature class
    arcpy.TruncateTable_management(outfc)
    print ("\nDeleted all records in: {}\n".format(outfc))

    # insert rows from Temporary feature class to SDE feature class
    with arcpy.da.InsertCursor(outfc, fieldnames) as oCursor:
        count = 0
        with arcpy.da.SearchCursor(inputfc, fieldnames) as iCursor:
            for row in iCursor:
                oCursor.insertRow(row)
                count += 1
                if count % 1000 == 0:
                    print("Inserting record {0} into SDE table".format(count))

    # disconnect all users
    print("\nDisconnecting all users...")
    arcpy.DisconnectUser(sde, "ALL")
    # confirm feature class was created
    print("\nUpdated " + outfc)

try:
    #---------------------------------------------------------------------------------------#
    ## CREATE STAGING TABLE ##
    #---------------------------------------------------------------------------------------#
    # start timer for the get data requests
    startTimer = datetime.now()
    #---------------------------------------------------------------------------------------#
    
    # drop fields - this should probably be made more conditional

    # Specify the string you want to search for in column names
    string_to_find = 'SPOT'

    # Drop columns containing the specified string
    columns_to_drop = [col for col in trafx.columns if string_to_find in col]
    trafx = trafx.drop(columns=columns_to_drop)
    #trafx.drop(trafx.columns[[25,26,27,28]], axis=1, inplace=True)
    
    # get a list of the fields that will be values
    # number of columns
    num_columns = trafx.shape[1]
    dfCol = trafx.iloc[:,1:num_columns]
    trafxList = dfCol.columns.to_list()
    
    # transfrom data from wide to long format
    dfTrafx = pd.melt(trafx.reset_index(), id_vars = ['Day'], value_vars = trafxList)

    # change the format of field to Date
    dfTrafx['Day'] = pd.to_datetime(dfTrafx['Day'])
    # change the format to MM-DD-YYYY
    dfTrafx['Day_Formatted'] = dfTrafx['Day'].dt.strftime('%m-%d-%Y')
    # rename fields
    dfTrafx = dfTrafx.rename(columns={'Day_Formatted':'month_day_year',
                                      'Day':'count_date', 
                        'variable':'counter_name', 
                        'value':'count_of_bike_ped'})

    # # drop columns
    # eco.drop(eco.columns[[25]], axis=1, inplace=True)
    # get a list of the fields that will be values
    num_columns = eco.shape[1]
    dfCol = eco.iloc[:,1:num_columns]
    ecoList = dfCol.columns.to_list()
    # transfrom data from wide to long format
    dfEco = pd.melt(eco.reset_index(), id_vars = ['Time'], value_vars = ecoList)
    # change the format of field to Date
    dfEco['Time'] = pd.to_datetime(dfEco['Time'])
    # change the format to MM-DD-YYYY
    dfEco['Time_Formatted'] = dfEco['Time'].dt.strftime('%m-%d-%Y')
    # rename fields
    dfEco = dfEco.rename(columns={'Time_Formatted':'month_day_year', 
                                  'Time':'count_date',
                        'variable':'counter_name', 
                        'value':'count_of_bike_ped'})

    # combine data frames
    df = pd.concat([dfEco, dfTrafx])
    #update station names so that counterid gets populated correctly.
    #We don't have full control over the names so this is a workaround
    station_lookup = {
        'Shared-use path Sunnyside' : 'Shared-use Path - Sunnyside',
    'Shared-Use Path - Homewood - @ Madden Creek Bridge' : 'Shared-use path - Homewood',
    'Shared-Use Path - Truckee River - @ 1/4 mile past TC Lumber' : 'Shared-use path - Truckee River Trail',
    'Shared-Use Path Sunnyside' : 'Shared-use path - Sunnyside',
    'Pioneer Trail W of Larch - pneumatic tubes Formula': 'Pioneer Trail W of Larch - Pneumatic tubes'
    }
    df['counter_name']=df['counter_name'].replace(station_lookup)

    # drop NaN values
    df.dropna(subset = ['count_of_bike_ped'], inplace=True)
    # create the month field and set it's type
    df.insert(loc=3,column='month_of_year', value = pd.to_datetime(df['month_day_year']).dt.month)
    # calculate the values
    df['month_of_year'] = pd.to_datetime(df['month_day_year']).dt.month

    # set the season field values
    df.loc[df['month_of_year'].isin([12,1,2,3]),  'season_of_year'] = 'Winter' 
    df.loc[df['month_of_year'].isin([6,7,8,9]),   'season_of_year'] = 'Summer' 
    df.loc[df['month_of_year'].isin([4,5,10,11]), 'season_of_year'] = 'Off-Season' 

    # set the counter category values by comparing the list of station names created earliar
    df.loc[df['counter_name'].isin(ecoList), 'counter_category']= 'ecovision'
    df.loc[df['counter_name'].isin(trafxList), 'counter_category']= 'trafx'
    df.loc[df['counter_name'].isin(ecoList) & df['counter_name'].isin(trafxList), 'counter_category']= 'ecovision & trafx'
    
    # create dictionary of counter_name:counter_id to be used to assign counter_id to the new data
    counterDict = sdfBike.set_index('counter_name').to_dict()['counter_id']
    # set counter id based on counter_id in DF Bike using dictionary
    df["counter_id"] = df["counter_name"].apply(lambda x: counterDict.get(x))
    
    # set OBJECTID field to be the index
    df['OBJECTID'] = df.reset_index().index
    df.set_index('OBJECTID',inplace=True)

    # save to CSV
    df.to_csv(os.path.join(workspace,"BikePed_New.csv"))

    #sde connection to disconnect users
    sde = "C:\\GIS\\DB_CONNECT\\Tabular.sde"

    # Change this to the path of your input feature class
    inputfc = os.path.join(workspace,"BikePed_New.csv")

    # Change this to the path of your output FC
    outfc = os.path.join(sde,"sde_tabular.SDE.bike_ped_counter_tabular")

    # Get field objects from source FC
    dsc = arcpy.Describe(inputfc)
    fields = dsc.fields

    # # List all field names except the OID field
    fieldnames = [field.name for field in fields if field.name != "Field1"]

    updateSDE(inputfc, outfc, fieldnames)

    # report how long it took to run the script
    endTimer = datetime.now() - startTimer
    print ("\nTime it took to run this script: {}".format(endTimer))
 
    # report how long it took to run the script
    FINALendTimer = datetime.now() - FIRSTstartTimer
    print ("\nTime it took to run this script: {}".format(FINALendTimer))

    log.write("\nTime it took to run this script: {}".format(FINALendTimer))
    log.close()
    
    header = "SUCCESS - The Bike/Ped data was updated."
    # send email with header based on try/except result
    send_mail(header)
    print('Sending email...')

# catch any arcpy errors
except arcpy.ExecuteError:
    print(arcpy.GetMessages())
    log.write(arcpy.GetMessages())
    log.close()
    
    header = "ERROR - Arcpy Exception - Check Log"
    # send email with header based on try/except result
    send_mail(header)
    print('Sending email...')

# catch system errors
except Exception:
    e = sys.exc_info()[1]
    print(e.args[0])
    log.write(str(e))
    log.close()
    
    header = "ERROR - System Error - Check Log"
    # send email with header based on try/except result
    send_mail(header)
    print('Sending email...')
