import bqlib
import json

def getConfig():
    with open('./config.json') as cf:
        conf = json.load(cf)
    return(conf)

def bqDataLoad(event, context):
    bucketName = event['bucket']
    configList = getConfig()
    bqDataset = configList["datasetName"]
    bqExternalTable = configList["externalTableName"]
    bqTargetTable = configList["targetTableName"]
    bqColumnList = configList["columnList"]
    fileLocation = "gs://" + bucketName + "/*.csv"

    # Delete External Table
    bqlib.deleteTable(bqDataset, bqExternalTable)

    # Create External Table
    bqlib.createTable(bqDataset, bqExternalTable, fileLocation)

    # Data Load from External Table
    bqlib.insertData(bqDataset, bqExternalTable, bqTargetTable, bqColumnList)
    
    print("Data loaded successfully...")
