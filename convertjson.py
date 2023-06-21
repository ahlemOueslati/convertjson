import pandas as pd
import numpy as np
import json
import os
import json
import argparse
import multiprocessing as mp
import datetime
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import lit

#Convert JSON files to CSV
def convert_json(pth, inputfile, opt):
    """
    """
    startc1 = datetime.datetime.now()
    jsonfile = pd.read_csv(pth+inputfile,sep='|', header=None)
    jsonfile.columns = ['origjson']
    sublevel1 = ['time', 'resourceId', 'ResourceGUID', 'Type', 'ClientBrowser', 'ClientCountryOrRegion', 'ClientIP', 
                 'ClientOS', 'ClientType', 'IKey', '_BilledSize', 'SDKVersion', 'SyntheticSource','Message', 'SeverityLevel', 
                 'ItemCount']
    sublevel2 = {'Properties': ['Container Id',  'Models', 'Request Id', 'Service Name', 'Workspace Name']}
    sublevel3 = {'Properties': {'Prediction': ['amount', 'approval', 'threshold', 'default_proba']}}
    sublevel4 = {'Properties': {'Input': {'input_data': ['customer_id', 'mean_volcashin_w11_w4', 'mean_volcashout_w11_w4', 
                                                'mean_volmerpay_w11_w4', 'mean_balomend_w11_w4', 'mean_volbillpay_w11_w4', 
                                                'mean_nbcontactcallin_w11_w4', 'mean_nbcall_w11_w4', 
                                                'mean_nbcontactcallout_w11_w4', 'mean_volcashin_w25_w12', 
                                                'mean_volcashout_w25_w12', 'mean_volmerpay_w25_w12', 'mean_balomend_w25_w12', 
                                                'mean_volbillpay_w25_w12', 'mean_nbcontactcallin_w25_w12', 
                                                'mean_nbcall_w25_w12', 'mean_nbcontactcallout_w25_w12', 
                                                'mean_volcashin_w25_w0', 'mean_volcashout_w25_w0', 'mean_volmerpay_w25_w0', 
                                                'mean_balomend_w25_w0', 'mean_volbillpay_w25_w0', 
                                                'mean_nbcontactcallin_w25_w0', 'mean_nbcall_w25_w0', 
                                                'mean_nbcontactcallout_w25_w0', 'std_balomend_w25_w0', 
                                                'std_nbcall_w25_w0', 'age', 'request_id', 'oba_intake', 
                                                'oba_repayment']}}}


    for vrnm in sublevel1:
        jsonfile[vrnm] = jsonfile["origjson"].apply(lambda x: json.loads(x)[vrnm])
    for vrnm in sublevel2.keys():
        for vlname in sublevel2[vrnm]:
            jsonfile[vlname] = jsonfile["origjson"].apply(lambda x: json.loads(x)[vrnm][vlname])
    for vrnm in sublevel3.keys():
        for vlname in sublevel3[vrnm].keys():
            for prdname in sublevel3[vrnm][vlname]:
                jsonfile[prdname] = jsonfile["origjson"].apply(lambda x: json.loads(json.loads(x)[vrnm][vlname])[prdname])
    for vrnm in sublevel4.keys():
        for vlname in sublevel4[vrnm].keys():
            for prdname in sublevel4[vrnm][vlname].keys():
                for iptname in sublevel4[vrnm][vlname][prdname]:
                    jsonfile[iptname] = jsonfile["origjson"].apply(lambda x: json.loads(json.loads(x)[vrnm][vlname])[prdname].get(iptname, ''))
    jsonfile = jsonfile[jsonfile.columns.drop("origjson")]
    startc2 = datetime.datetime.now()
    filename = opt+inputfile.split('.')[0]+'.csv'
    jsonfile.to_csv(filename, sep=";", index=False)
    #print("CONVERTING THIS FILE TOOK: {}".format(startc2 - startc1))
    #return ''

if __name__ == "__main__":

    #Define the start datetime
    start1 = datetime.datetime.now()
    #define the storage account name, the access key, the input and the output container names
    #using --> dbutils.widgets.get("Parameter name in ADF pipeline")
    blob_account_name = dbutils.widgets.get("BlobAccountName")
    input_blob_container_name = dbutils.widgets.get("InputContainerName")
    output_blob_container_name = dbutils.widgets.get("OutputContainerName")
    accessKey = dbutils.widgets.get("AccessKey")

    print("================== START CONVERTING JSON FILES ================")

    #jsontest local directory is replaced by a mount point ("/mnt/jsontest")
    #check if "/mnt/jsontest" is mounted, then unmount it
    if any(mount.mountPoint == "/mnt/jsontest" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount("/mnt/jsontest")

    #copy the existed files in the blob container to the mount point "/mnt/jsontest"
    #using the blob container path and configured by the access key
    dbutils.fs.mount(  
        source = f"wasbs://{input_blob_container_name}@{blob_account_name}.blob.core.windows.net/",  
        mount_point = "/mnt/jsontest",  
        extra_configs = {f"fs.azure.account.key.{blob_account_name}.blob.core.windows.net": accessKey})
    
    #define the datapath
    datapath = "/dbfs/mnt/jsontest/"
    #create the output data path
    #using the dbutils.fs.mkdirs("path") instead of os.mkdir("path")
    dbutils.fs.mkdirs("/mnt/jsonconverted")
    output = "/dbfs/mnt/jsonconverted/"

    listfiles = [onefile for onefile in os.listdir(datapath)]
    listfiles=listfiles[:3]
    results = [convert_json(datapath,onefile, output) for onefile in listfiles]

    #define the schema of the dataframe
    #pandas dataframe is replaced by spark df
    #defining the schema is mandatory because we can't concatenate two dataframes with diffrent schemas in spark
    schema = StructType([
        StructField('time', StringType(), True),
        StructField('resourceId', StringType(), True),
        StructField('ResourceGUID', StringType(), True),
        StructField('Type', StringType(), True),
        StructField('ClientBrowser', StringType(), True),
        StructField('ClientCountryOrRegion', StringType(), True),
        StructField('ClientIP', StringType(), True),
        StructField('ClientOS', StringType(), True),
        StructField('ClientType', StringType(), True),
        StructField('IKey', StringType(), True),
        StructField('_BilledSize', StringType(), True),
        StructField('SDKVersion', StringType(), True),
        StructField('SyntheticSource', StringType(), True),
        StructField('Message', StringType(), True),
        StructField('SeverityLevel', StringType(), True),
        StructField('ItemCount', StringType(), True),
        StructField('Container Id', StringType(), True),
        StructField('Models', StringType(), True),
        StructField('Request Id', StringType(), True),
        StructField('Service Name', StringType(), True),
        StructField('Workspace Name', StringType(), True),
        StructField('amount', StringType(), True),
        StructField('approval', StringType(), True),
        StructField('threshold', StringType(), True),
        StructField('default_proba', StringType(), True),
        StructField('customer_id', StringType(), True),
        StructField('mean_volcashin_w11_w4', StringType(), True),
        StructField('mean_volcashout_w11_w4', StringType(), True),
        StructField('mean_volmerpay_w11_w4', StringType(), True),
        StructField('mean_balomend_w11_w4', StringType(), True),
        StructField('mean_volbillpay_w11_w4', StringType(), True),
        StructField('mean_nbcontactcallin_w11_w4', StringType(), True),
        StructField('mean_nbcall_w11_w4', StringType(), True),
        StructField('mean_nbcontactcallout_w11_w4', StringType(), True),
        StructField('mean_volcashin_w25_w12', StringType(), True),
        StructField('mean_volcashout_w25_w12', StringType(), True),
        StructField('mean_volmerpay_w25_w12', StringType(), True),
        StructField('mean_balomend_w25_w12', StringType(), True),
        StructField('mean_volbillpay_w25_w12', StringType(), True),
        StructField('mean_nbcontactcallin_w25_w12', StringType(), True),
        StructField('mean_nbcall_w25_w12', StringType(), True),
        StructField('mean_nbcontactcallout_w25_w12', StringType(), True),
        StructField('mean_volcashin_w25_w0', StringType(), True),
        StructField('mean_volcashout_w25_w0', StringType(), True),
        StructField('mean_volmerpay_w25_w0', StringType(), True),
        StructField('mean_balomend_w25_w0', StringType(), True),
        StructField('mean_volbillpay_w25_w0', StringType(), True),
        StructField('mean_nbcontactcallin_w25_w0', StringType(), True),
        StructField('mean_nbcall_w25_w0', StringType(), True),
        StructField('mean_nbcontactcallout_w25_w0', StringType(), True),
        StructField('std_balomend_w25_w0', StringType(), True),
        StructField('std_nbcall_w25_w0', StringType(), True),
        StructField('age', StringType(), True),
        StructField('request_id', StringType(), True),
        StructField('oba_intake', StringType(), True),
        StructField('oba_repayment', StringType(), True)
        ])
    
    # Concatenate all files
    files = [m for m in os.listdir(output) if 'csv' in m]
    #create an empty dataframe with a schema
    df = spark.createDataFrame([], schema)

    for filename in files:
        print(filename)
        tmp = spark.read.option("delimiter",";").option("header","true").csv("/mnt/jsonconverted/"+filename)
        tmp = tmp.withColumn("request_id",col("request_id").cast(StringType()))
        df= df.union(tmp)

    dtnw = str(datetime.datetime.now()).split('.')[0].replace(" ", "_").replace("-", "").replace(":", "")
    cwd = '/dbfs/mnt'
    #create the 'ft' directory
    dbutils.fs.mkdirs("/mnt/ft")
    #save the csv file
    df.write.mode("overwrite").option("delimiter",";").option("index","false").format("csv").save(cwd+'/ft/EBM_logs_{}.csv'.format(dtnw))

    #define the 'cdt2' path (the output container path)
    cdt2 = f"wasbs://jsonconverted@{blob_account_name}.blob.core.windows.net/"

    #define the sas token
    #using --> dbutils.widgets.get("Parameter name in ADF pipeline")
    sas_token = dbutils.widgets.get("Token")

    #give the write access to spark using the sas token
    spark.conf.set(
    f"fs.azure.sas.{output_blob_container_name}.{blob_account_name}.blob.core.windows.net",
    sas_token
    )

    #save the file to the output blob container
    file_name = cdt2+'EBM_logs_{}.csv'.format(dtnw)
    df.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(file_name)

    end1 = datetime.datetime.now()
    print("    THE COMPLETE JOB TOOK: {}".format(end1 - start1))
    print("================== JOB RUN SUCCESSFULLY ================")