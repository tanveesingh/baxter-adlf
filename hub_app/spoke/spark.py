from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from run import hub_parser
#from hub_app.uom_conver_factor import fn_item_uom_cnvrn
ETL_PRCS_NAME = hub_parser.parse_args().prcs_name
REGION = hub_parser.parse_args().region
APP_NAME = ETL_PRCS_NAME+'_'+REGION

def get_session(master_ip=None):
    if hub_parser.parse_args().memory_config:
        config_mode_file = hub_parser.parse_args().memory_config
    else:
        config_mode_file = 'incr1_low.txt'
    config_mode_file = '/home/hadoop/Framework/Data_Processing/SparkHub/'+config_mode_file
    file = open(config_mode_file,'r+')
    content = file.read().split('\n')
    conf_list =[]
    for value in content:
        val = value.split(',')[1]
        conf_list.append(val)
    
    spark = SparkSession.builder.config("spark.dynamicAllocation.enabled", "False") \
    .config("spark.driver.memory",conf_list[0]).config("spark.executor.memory",conf_list[1]).config("spark.executor.instances",conf_list[2]).config("spark.executor.cores",conf_list[3]).config("spark.app.name",APP_NAME).config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.sql.parquet.writeLegacyFormat", "True").config("yarn.app.mapreduce.am.resource.mb","8G").config("yarn.nodemanager.resource.memory-mb","8G").config("yarn.scheduler.maximum-allocation-mb","8G") \
    .config("spark.scheduler.mode", "FAIR").config("spark.sql.sources.partitionOverwriteMode", "dynamic").config("spark.sql.autoBroadcastJoinThreshold","-1").enableHiveSupport() \
    .getOrCreate()
    
    spark.conf.set("spark.debug.maxToStringFields", 1000)
    spark.conf.set("spark.sql.broadcastTimeout", 36000)
    spark.conf.set("spark.sql.codegen.wholeStage", False)
    spark.conf.set("spark.sql.hive.caseSensitiveInferenceMode","INFER_ONLY")
    spark.conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.sql.hive.convertMetastoreParquet","false")
    #spark.conf.set("spark.sql.catalogImplementation","HIVE")
    #config("hive.metastore.uris", "thrift://{master_ip}:9083".format(master_ip=master_ip)).
    #print(spark.sparkContext._conf.getAll())
    return spark