import logging
import pandas as pd # pip install pandas
import mysql
import json
import azure.functions as func
import pathlib
from azure.storage.blob import BlobServiceClient
from io import StringIO, BytesIO
from pydantic import BaseModel
from decimal import *
from pathlib import Path
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime

class Kpidata(BaseModel):
    rcp_revid: int
    batchnumber: int
    recipe_name: str
    steplogname: str
    log_stepdate: datetime
    rcp_seqallid: int
    bias_arccount: float
    act_temp_1: float
    act_temp_2: float
    pirani_a2_mbar: Decimal
    penning_mbar: Decimal
    baraton_mbar: Decimal
# csvinput =  "function/Exp220919001_471v3.csv" 
# allrowss = pd.read_csv(csvinput)
#myblob = 'blob/Exp220919001_471.csv'

def createobj(dataset):
    kpidata = Kpidata(rcp_revid=dataset.RCP_RevId, batchnumber=dataset.BATCHNUM, recipe_name=dataset._2, steplogname=dataset.StepLogName, log_stepdate=datetime.strptime(dataset.LOGDATE, "%d-%m-%Y %H:%M:%S"),
                      rcp_seqallid=dataset.RCP_SEQALLID, bias_arccount=dataset.BIAS_ARCCOUNT, act_temp_1=dataset.ACT_TEMP_1,
                      act_temp_2=dataset.ACT_TEMP_2, pirani_a2_mbar=dataset.PIRANI_A2_mbar, penning_mbar=dataset.PENNING_mbar,
                      baraton_mbar=dataset.BARATRON_mbar)
    return kpidata
def importdata(blobname):
    # dest_fileee = "C:/Users/Gebruiker/Documents/Minor/code/sfeer.csv"
    # storage_account_key = "pa7JGvJjJs2KjMPaUza2/fp8gurZS9S7JJSjVNRN4W3Lx6XFCJtOeHWlIhw0b2E3E5eJLO9OiFCz+AStI7wu/Q=="
    # storage_account_name = "hauzertempblob"
    # urll= "https://hauzertempblob.blob.core.windows.net/"
    blobbie = blobname.split('/')

    connection_string = "DefaultEndpointsProtocol=https;AccountName=hauzertempblob;AccountKey=Cn3p1XQGa8m+z6Zpj6R+LxZbHisQdMvhEJObSvig4V30oM5uDwkyLGmgRtmrEypV2aOFXiVTQ528+AStB9D9Jg==;EndpointSuffix=core.windows.net"
    container_name = "blob"

    # Initialise client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    # Initialise container
    blob_container_client = blob_service_client.get_container_client(container_name)
    # Get blob
    blob_client = blob_container_client.get_blob_client(blobbie[1])

    download_stream = blob_client.download_blob()
    stream = BytesIO()
    downloader = blob_client.download_blob()

    # download the entire file in memory here
    # file can be many giga bytes! Big problem
    downloader.readinto(stream)
    stream.seek(0)
    allrowss = pd.read_csv(stream)
    logging.info(f"{allrowss}")
    return allrowss
def processdata(rawdata):

    for p in rawdata.itertuples():
        #print(p)
        kpidataobj = createobj(p)
        logging.info(kpidataobj)
        insertsql(kpidataobj)

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    rawdata = importdata(myblob.name)
    processdata(rawdata)

#importing dataset from csv example

def insertsql(formatobj):
    
    # config = {
    # 'host':'hauzermysql.mysql.database.azure.com',
    # 'user':'mysa',
    # 'password':'HauzerSQL!',
    # 'database':'mysqldb'
    # }
    try:
        conn = mysql.connector.connect(user='mysa', password='HauzerSQL!',
                              host='hauzermysql.mysql.database.azure.com',
                              database='mysqldb')
        conn.autocommit = True
        
        print("Connection established")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor = conn.cursor() 


       
     
    print(formatobj)

    cursor.execute("INSERT INTO kpidata (rcp_revid, batchnumber, recipe_name, steplogname, log_stepdate, rcp_seqallid, bias_arccount, act_temp_1, act_temp_2, pirani_a2_mbar, penning_mbar, baraton_mbar) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", (formatobj.rcp_revid, formatobj.batchnumber, formatobj.recipe_name, formatobj.steplogname, formatobj.log_stepdate.strftime("%Y/%m/%d %H:%M:%S"), formatobj.rcp_seqallid, formatobj.bias_arccount, formatobj.act_temp_1, formatobj.act_temp_2, formatobj.pirani_a2_mbar, formatobj.penning_mbar, formatobj.baraton_mbar))
    print("Inserted",cursor.rowcount,"row(s) of data.")
    # cursor.execute("INSERT INTO kpidata (rcp_revid, batchnumber, recipe_name, steplogname, log_stepdate, rcp_seqallid, biasarccount, act_temp_1, act_temp_2, pirani_a2_mbar, penning_mbar, baraton_mbar) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", (formatobj.rcp_revid, formatobj.batchnumber, formatobj.recipe_name, formatobj.steplogname, formatobj.log_stepdate, formatobj.rcp_seqallid, formatobj.bias_arccount, formatobj.act_temp_1, formatobj.act_temp_2, formatobj.pirani_a2_mbar, formatobj.baraton_mbar))
    # print("Inserted",cursor.rowcount,"row(s) of data.")
      #cursor.execute("INSERT INTO kpidata (rcp_revid, batchnumber, recipe_name, steplogname, log_stepdate, rcp_seqallid, biasarccount, act_temp_1, act_temp_2, pirani_a2_mbar, penning_mbar, baraton_mbar) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", (111, "111", "111", "111", 111, 111, 111, 111, 111, 111, 111, 111))
      #print("Inserted",cursor.rowcount,"row(s) of data.")

      # Cleanup
    conn.commit()
    cursor.close()
    conn.close()
    print("Done.")