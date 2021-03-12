from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from hub_app.log import setup_custom_logger
from run import hub_parser
from hub_app.func_utils import get_exception, archive_files,get_current_time

ETL_PRCS_NAME = hub_parser.parse_args().prcs_name
REGION = hub_parser.parse_args().region
PART_VAL = hub_parser.parse_args().part_val
log = None
files_list = []


'''
Description : Function to perform error checks on columns
Input : Spark Dataframe,Column List
Output : Error Dataframe, Valid Dataframe
'''
def error_check(df,spark, **kwargs):
    try:
        delimiter = kwargs.get('delimiter')
        error_table_path = kwargs.get('error_table_path')
        temp_error_path = error_table_path.replace('error', 'temp/error_E1')
        df.write.format("parquet").mode("overwrite").save(temp_error_path)

        
        if 'column_list_e1_non_jde_ak' in kwargs:
            column_list_e1_non_jde_ak = kwargs.get('column_list_e1_non_jde_ak')
            column_list_e1_non_jde_ak = column_list_e1_non_jde_ak.split(delimiter)
            non_jde_ak_date_format = kwargs.get('non_jde_ak_date_format')
            non_jde_ak_date_format = non_jde_ak_date_format.split(delimiter)
        else:
            column_list_e1_non_jde_ak = []
            non_jde_ak_date_format = []

        if 'column_list_e1_non_jde_non_ak' in kwargs:
            column_list_e1_non_jde_non_ak = kwargs.get('column_list_e1_non_jde_non_ak')
            column_list_e1_non_jde_non_ak = column_list_e1_non_jde_non_ak.split(delimiter)
            non_jde_non_ak_date_format = kwargs.get('non_jde_non_ak_date_format')
            non_jde_non_ak_date_format = non_jde_non_ak_date_format.split(delimiter)
        else:
            column_list_e1_non_jde_non_ak = []
            non_jde_non_ak_date_format = []


        if 'column_list_e1_jde_ts_audit' in kwargs:
            column_list_e1_jde_ts_audit = kwargs.get('column_list_e1_jde_ts_audit')
            column_list_e1_jde_ts_audit = column_list_e1_jde_ts_audit.split(delimiter)
        else:
            column_list_e1_jde_ts_audit = []

        if 'column_list_e1_jde_ts_non_audit' in kwargs:
            column_list_e1_jde_ts_non_audit = kwargs.get('column_list_e1_jde_ts_non_audit')
            column_list_e1_jde_ts_non_audit = column_list_e1_jde_ts_non_audit.split(delimiter)
        else:
            column_list_e1_jde_ts_non_audit = []

        if 'column_list_e1_ak_ts' in kwargs:
            column_list_e1_ak_ts = kwargs.get('column_list_e1_ak_ts')
            column_list_e1_ak_ts = column_list_e1_ak_ts.split(delimiter)
        else:
            column_list_e1_ak_ts = []

        if 'column_list_e1_ak_date' in kwargs:
            column_list_e1_ak_date = kwargs.get('column_list_e1_ak_date')
            column_list_e1_ak_date = column_list_e1_ak_date.split(delimiter)
        else:
            column_list_e1_ak_date = []

        if 'column_list_e1_non_ak_date' in kwargs:
            column_list_e1_non_ak_date = kwargs.get('column_list_e1_non_ak_date')
            column_list_e1_non_ak_date = column_list_e1_non_ak_date.split(delimiter)
        else:
            column_list_e1_non_ak_date = []
        
        if 'column_list_e2' in kwargs:
            column_list_e2 = kwargs.get('column_list_e2')
            column_list_e2 = column_list_e2.split(delimiter)
        else:
            column_list_e2 = []

        if 'column_list_e2_jde_date' in kwargs:
            column_list_e2_jde_date = kwargs.get('column_list_e2_jde_date')
            column_list_e2_jde_date = column_list_e2_jde_date.split(delimiter)
        else:
            column_list_e2_jde_date = []      
        
        if 'column_list_e3_null' in kwargs:
            column_list_e3_null = kwargs.get('column_list_e3_null')
            column_list_e3_null = column_list_e3_null.split(delimiter)
        else:
            column_list_e3_null = []

        if 'column_list_e3_not_null' in kwargs:
            column_list_e3_not_null = kwargs.get('column_list_e3_not_null')
            column_list_e3_not_null = column_list_e3_not_null.split(delimiter)
        else:
            column_list_e3_not_null = []

        if 'column_list_e4' in kwargs:
            column_list_e4 = kwargs.get('column_list_e4')
            column_list_e4 = column_list_e4.split(delimiter)
        else:
            column_list_e4 = []

        if 'column_list_e4_jde_date' in kwargs:
            column_list_e4_jde_date = kwargs.get('column_list_e4_jde_date')
            column_list_e4_jde_date = column_list_e4_jde_date.split(delimiter)
        else:
            column_list_e4_jde_date = []

        if 'column_list_e5_not_null' in kwargs:
            column_list_e5_not_null = kwargs.get('column_list_e5_not_null')
            column_list_e5_not_null = column_list_e5_not_null.split(delimiter)
        else:
            column_list_e5_not_null = []

        if 'column_list_e5_null' in kwargs:
            column_list_e5_null = kwargs.get('column_list_e5_null')
            column_list_e5_null = column_list_e5_null.split(delimiter)
        else:
            column_list_e5_null = []
        if 'column_list_e6' in kwargs:
            column_list_e6 = kwargs.get('column_list_e6')
            column_list_e6 = column_list_e6.split(delimiter)
        else:
            column_list_e6 = []

        error_desc_columns =[]
        error_code_columns =[]

        
        log.info("E1 JDE TS AUDIT : " + str(column_list_e1_jde_ts_audit))
        log.info("E1 NON JDE NON AK : " + str(column_list_e1_non_jde_non_ak))
        log.info("E1 NON JDE AK : " + str(column_list_e1_non_jde_ak))
        log.info("NON JDE NON AK DATE FORMAT : " + str(non_jde_non_ak_date_format))
        log.info("NON JDE AK DATE FORMAT : " + str(non_jde_ak_date_format))
        log.info("E1 JDE TS NON AUDIT : " + str(column_list_e1_jde_ts_non_audit))
        log.info("E1 AK TS : "+str(column_list_e1_ak_ts))
        log.info("E1 AK DATE : " +str(column_list_e1_ak_date))
        log.info("E1 NON AK DATE : " + str(column_list_e1_non_ak_date))
        log.info("E2 JDE DATE : " +str(column_list_e2_jde_date))
        log.info("E2 : " +str(column_list_e2))
        log.info("E3 NULL : " + str(column_list_e3_null))
        log.info("E3 NOT NULL : " + str(column_list_e3_not_null))
        log.info("E4 JDE DATE : " + str(column_list_e4_jde_date))
        log.info("E4 : " +str(column_list_e4))
        log.info("E5 NOTNULL : " + str(column_list_e5_not_null))
        log.info("E5 NULL : "+ str(column_list_e5_null))
        log.info("E6 : " + str(column_list_e6))

        ########################################################################
        #                  E1 Error Check Validation                           #
        ########################################################################
        ######################## E1 AK Date VALIDATION #########################
        if len(column_list_e1_ak_date) > 0:
            #print("Inside E1 AK")
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)
            df =  df.withColumn("Error_Desc_E1_AK_Date",lit(''))
            df =  df.withColumn("Error_Code_E1_AK_Date",lit(0))        
            df.registerTempTable("E1_AK_Date_Table")            
            error_desc_query = []
            error_code_query= []
            error_desc_col_list = []
            error_code_col_list = [] 

            for src_col in column_list_e1_ak_date:
                error_desc_query.append("case when trim(" + src_col +") <> '' and " + src_col +" is not null and ((cast ("+ src_col +" as decimal(38,0)) not between 0 and 8099365) or (from_unixtime(unix_timestamp(cast(("+ src_col +" + 1900000) as varchar(255)), 'yyyyDDD')) is NULL)) then 'E1:"+src_col+",' end as "+ src_col + "_Error_Desc")
                error_code_query.append("case when trim(" + src_col +") <> '' and " + src_col +" is not null and ((cast ("+ src_col +" as decimal(38,0)) not between 0 and 8099365) or (from_unixtime(unix_timestamp(cast(("+ src_col +" + 1900000) as varchar(255)), 'yyyyDDD')) is NULL)) then 1 else 0 end as "+ src_col + "_Error_Code")
                error_desc_col_list.append(src_col + '_Error_Desc')
                error_code_col_list.append(src_col + '_Error_Code')      
            
            error_desc_query = ','.join(error_desc_query)
            error_code_query = ','.join(error_code_query)
            error_desc_col = ','.join(error_desc_col_list)
            error_code_col = '+'.join(error_code_col_list)
       
            if error_desc_query != '':
                error_check_query = "select "+src_cols+" , concat_ws('',"+error_desc_col+") as Error_Desc_E1_AK_Date,("+error_code_col+") as Error_Code_E1_AK_Date  from (select * , " + error_desc_query + " , " + error_code_query+" from E1_AK_Date_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E1_AK_Date')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E1_AK_Date')
            error_code_columns.append('Error_Code_E1_AK_Date')

        
        
        ######################## E1 NON AK Date VALIDATION #########################
        if len(column_list_e1_non_ak_date) > 0:
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)     
            df =  df.withColumn("Error_Desc_E1_NON_AK_Date",lit(''))
            df =  df.withColumn("Error_Code_E1_NON_AK_Date",lit(0))        
            df.registerTempTable("E1_NON_AK_Date_Table")           
            error_desc_query = []
            error_desc_col_list = [] 

            for src_col in column_list_e1_non_ak_date:
                error_desc_query.append("case when trim(" + src_col +") <> '' and " + src_col +" is not null and cast ("+ src_col +" as decimal(38,0)) not between 0 and 8099365 then 'E1:"+src_col+",' end as "+ src_col + "_Error_Desc")
                error_desc_col_list.append(src_col + '_Error_Desc')      
            
            error_desc_query = ','.join(error_desc_query)
            error_desc_col = ','.join(error_desc_col_list)
            
       
            if error_desc_query != '':
                error_check_query = "select "+src_cols+" ,Error_Code_E1_NON_AK_Date,concat_ws('',"+error_desc_col+") as Error_Desc_E1_NON_AK_Date from (select * , " + error_desc_query + "  from E1_NON_AK_Date_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E1_NON_AK_Date')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E1_NON_AK_Date')
            error_code_columns.append('Error_Code_E1_NON_AK_Date')

        ######################## E1 JDE TS NON AUDIT VALIDATION ########################
        if len(column_list_e1_jde_ts_non_audit) > 0 :
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)     
            df =  df.withColumn("Error_Desc_E1_JDE_TS_NON_AUDIT",lit(''))
            df =  df.withColumn("Error_Code_E1_JDE_TS_NON_AUDIT",lit(0))        
            df.registerTempTable("E1_JDE_TS_NON_AUDIT_Table") 
            e1_col_list =[]        
            for col in column_list_e1_jde_ts_non_audit:
                e1_col_list.append(col + '_E1_TS')        
            
            conversion_query = []
            error_desc_query = []
            error_desc_col_list = []
           

            for src_col in column_list_e1_jde_ts_non_audit:
                conversion_query.append("case when length(cast("+ src_col +" as long)) > 6 then null else lpad(cast("+ src_col +" as long),6,0) end as "+src_col +"_E1_TS")

            conversion_query = ','.join(conversion_query)
            final_conversion_query = "select * , " + conversion_query +" from E1_JDE_TS_NON_AUDIT_Table"
            conversion_df= spark.sql(final_conversion_query)
            conversion_df.registerTempTable("E1_JDE_TS_NON_AUDIT_Table") 

            for e1_col,src_col in zip(e1_col_list,column_list_e1_jde_ts_non_audit):
                error_desc_query.append("case when "+ e1_col +" is null  or substring(trim("+ e1_col +"),1,2) not between 0 and 23 or substring(trim("+ e1_col +"),3,2) not between 0 and 59 or substring(trim("+ e1_col +"),5,2) not between 0 and 59 then 'E1:"+src_col+",' end as "+ src_col + "_Error_Desc")
                error_desc_col_list.append(src_col + '_Error_Desc')
             
            
            error_desc_query = ','.join(error_desc_query)
            error_desc_col = ','.join(error_desc_col_list)
            
        
            if error_desc_query != '':
                e1_col_list = ','.join(e1_col_list)
                error_check_query = "select "+src_cols+","+e1_col_list+" ,Error_Code_E1_JDE_TS_NON_AUDIT, concat_ws('',"+error_desc_col+") as Error_Desc_E1_JDE_TS_NON_AUDIT from (select * , " + error_desc_query + " from E1_JDE_TS_NON_AUDIT_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E1_JDE_TS_NON_AUDIT')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E1_JDE_TS_NON_AUDIT')
            error_code_columns.append('Error_Code_E1_JDE_TS_NON_AUDIT')
        
        ######################## E1 AK TS VALIDATION ########################
        if len(column_list_e1_ak_ts) > 0 :
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)      
            df =  df.withColumn("Error_Desc_E1_AK_TS",lit(''))
            df =  df.withColumn("Error_Code_E1_AK_TS",lit(0))        
            df.registerTempTable("E1_AK_TS_Table")
            e1_col_list =[]        
            for col in column_list_e1_ak_ts:
                e1_col_list.append(col + '_E1_TS')        
            
            conversion_query = []
            error_desc_query = []
            error_code_query= []
            error_desc_col_list = []
            error_code_col_list = [] 
           

            for src_col in column_list_e1_ak_ts:
                conversion_query.append("case when length(cast("+ src_col +" as long)) > 6 then null else lpad(cast("+ src_col +" as long),6,0) end as "+src_col +"_E1_TS")

            conversion_query = ','.join(conversion_query)
            final_conversion_query = "select * , " + conversion_query +" from E1_AK_TS_Table"
            conversion_df= spark.sql(final_conversion_query)
            conversion_df.registerTempTable("E1_AK_TS_Table") 

            for e1_col,src_col in zip(e1_col_list,column_list_e1_ak_ts):
                error_desc_query.append("case when "+ e1_col +" is null  or substring(trim("+ e1_col +"),1,2) not between 0 and 23 or substring(trim("+ e1_col +"),3,2) not between 0 and 59 or substring(trim("+ e1_col +"),5,2) not between 0 and 59 then 'E1:"+src_col+",' end as "+ src_col + "_Error_Desc")
                error_code_query.append("case when "+ e1_col +" is null  or substring(trim("+ e1_col +"),1,2) not between 0 and 23 or substring(trim("+ e1_col +"),3,2) not between 0 and 59 or substring(trim("+ e1_col +"),5,2) not between 0 and 59 then 1 else 0 end as "+ src_col + "_Error_Code")
                error_desc_col_list.append(src_col + '_Error_Desc')
                error_code_col_list.append(src_col + '_Error_Code')
             
            
            error_desc_query = ','.join(error_desc_query)
            error_code_query = ','.join(error_code_query)
            error_desc_col = ','.join(error_desc_col_list)
            error_code_col = '+'.join(error_code_col_list)
            
        
            if error_desc_query != '':
                e1_col_list = ','.join(e1_col_list)
                error_check_query = "select  "+src_cols+","+e1_col_list+" , concat_ws('',"+error_desc_col+") as Error_Desc_E1_AK_TS,("+error_code_col+") as Error_Code_E1_AK_TS  from (select * , " + error_desc_query + " , " + error_code_query+" from E1_AK_TS_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E1_AK_TS')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E1_AK_TS')
            error_code_columns.append('Error_Code_E1_AK_TS')


        ######################## E1 NON JDE NON AK VALIDATION ########################
        if len(column_list_e1_non_jde_non_ak) > 0 :
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)      
            df =  df.withColumn("Error_Desc_E1_NON_JDE_NON_AK",lit(''))
            df =  df.withColumn("Error_Code_E1_NON_JDE_NON_AK",lit(0))
            df.registerTempTable("E1_NON_JDE_NON_AK_Table")
            e1_col_list =[]        
            for col in column_list_e1_non_jde_non_ak:
                e1_col_list.append(col + '_E1_NJDE')

            
            conversion_query = []
            error_desc_query = []
            error_desc_col_list = []
           

            for src_col, date_format in zip(column_list_e1_non_jde_non_ak, non_jde_non_ak_date_format):
                conversion_query.append("case when to_timestamp("+ src_col +",'"+date_format+"') is null then null else to_timestamp("+ src_col +",'"+date_format+"')  end as "+src_col +"_E1_NJDE")

            conversion_query = ','.join(conversion_query)
            final_conversion_query = "select * , " + conversion_query +" from E1_NON_JDE_NON_AK_Table"
            conversion_df= spark.sql(final_conversion_query)
            conversion_df.registerTempTable("E1_NON_JDE_NON_AK_Table") 

            for e1_col,src_col in zip(e1_col_list,column_list_e1_non_jde_non_ak):
                error_desc_query.append("case when "+ e1_col +" is null then 'E1:"+src_col+",' end as "+ src_col + "_Error_Desc")
                error_desc_col_list.append(src_col + '_Error_Desc')
             
            
            error_desc_query = ','.join(error_desc_query)
            error_desc_col = ','.join(error_desc_col_list)
            
        
            if error_desc_query != '':
                e1_col_list = ','.join(e1_col_list)
                error_check_query = "select "+src_cols+","+e1_col_list+" ,Error_Code_E1_NON_JDE_NON_AK, concat_ws('',"+error_desc_col+") as Error_Desc_E1_NON_JDE_NON_AK from (select * , " + error_desc_query + " from E1_NON_JDE_NON_AK_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E1_NON_JDE_NON_AK')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E1_NON_JDE_NON_AK')
            error_code_columns.append('Error_Code_E1_NON_JDE_NON_AK')

        
        ######################## E1 NON JDE AK VALIDATION ########################
        if len(column_list_e1_non_jde_ak) > 0 :
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)       
            df =  df.withColumn("Error_Desc_E1_NON_JDE_AK",lit(''))
            df =  df.withColumn("Error_Code_E1_NON_JDE_AK",lit(0))
            df.registerTempTable("E1_NON_JDE_AK_Table") 
            e1_col_list =[]        
            for col in column_list_e1_non_jde_ak:
                e1_col_list.append(col + '_E1_NJDE')        
            
            conversion_query = []
            error_desc_query = []
            error_code_query= []
            error_desc_col_list = []
            error_code_col_list = [] 
           

            for src_col, date_format in zip(column_list_e1_non_jde_ak, non_jde_ak_date_format):
                conversion_query.append("case when to_timestamp("+ src_col +",'"+date_format+"') is null then null else to_timestamp("+ src_col +",'"+date_format+"')  end as "+src_col +"_E1_NJDE")

            conversion_query = ','.join(conversion_query)
            final_conversion_query = "select * , " + conversion_query +" from E1_NON_JDE_AK_Table"
            conversion_df= spark.sql(final_conversion_query)
            conversion_df.registerTempTable("E1_NON_JDE_AK_Table") 

            for e1_col,src_col in zip(e1_col_list,column_list_e1_non_jde_ak):
                error_desc_query.append("case when "+ e1_col +" is null then 'E1:"+src_col+",' end as "+ src_col + "_Error_Desc")
                error_code_query.append("case when "+ e1_col +" is null then 1 else 0 end as "+ src_col + "_Error_Code")
                error_desc_col_list.append(src_col + '_Error_Desc')
                error_code_col_list.append(src_col + '_Error_Code')
             
            
            error_desc_query = ','.join(error_desc_query)
            error_code_query = ','.join(error_code_query)
            error_desc_col = ','.join(error_desc_col_list)
            error_code_col = '+'.join(error_code_col_list)
            
        
            if error_desc_query != '':
                e1_col_list = ','.join(e1_col_list)
                error_check_query = "select "+src_cols+","+e1_col_list+" , concat_ws('',"+error_desc_col+") as Error_Desc_E1_NON_JDE_AK,("+error_code_col+") as Error_Code_E1_NON_JDE_AK  from (select * , " + error_desc_query + " , " + error_code_query+" from E1_NON_JDE_AK_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E1_NON_JDE_AK')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E1_NON_JDE_AK')
            error_code_columns.append('Error_Code_E1_NON_JDE_AK')

        
        ######################## E1 JDE TS AUDIT VALIDATION ########################        
        if len(column_list_e1_jde_ts_audit) > 0 :
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)      
            df.registerTempTable("E1_JDE_TS_AUDIT_Table")
            e1_col_list =[]        
            for col in column_list_e1_jde_ts_audit:
                e1_col_list.append(col + '_E1_TS')        
            
            conversion_query = []
           
            for src_col in column_list_e1_jde_ts_audit:
                conversion_query.append("case when length(cast("+ src_col +" as long)) > 6 then null else lpad(cast("+ src_col +" as long),6,0) end as "+src_col +"_E1_TS")

            conversion_query = ','.join(conversion_query)
            final_conversion_query = "select * , " + conversion_query +" from E1_JDE_TS_AUDIT_Table"
            conversion_df= spark.sql(final_conversion_query)
            temp_error_path = error_table_path.replace('error', 'temp/error_E1_JDE_TS_AUDIT')
            conversion_df.write.format("parquet").mode("overwrite").save(temp_error_path)


        log.info("E1 Validation Done")

        ########################################################################
        #                  E2 Error Check Validation                           #
        ########################################################################
        ######################## E2 AK VALIDATION ##############################
        if len(column_list_e2) > 0:
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)       
            df =  df.withColumn("Error_Desc_E2_AK",lit(''))
            df =  df.withColumn("Error_Code_E2_AK",lit(0))        
            df.registerTempTable("E2_AK_Table")             
            error_desc_query = []
            error_code_query= []
            error_desc_col_list = []
            error_code_col_list = [] 

            for src_col in column_list_e2:
                error_desc_query.append("case when trim(" + src_col +") = '' or "+ src_col +" is null then 'E2:"+src_col+",' end as "+ src_col + "_Error_Desc")
                error_code_query.append("case when trim(" + src_col +") = '' or "+ src_col +" is null then 1 else 0 end as "+ src_col + "_Error_Code")
                error_desc_col_list.append(src_col + '_Error_Desc')
                error_code_col_list.append(src_col + '_Error_Code')      
            
            error_desc_query = ','.join(error_desc_query)
            error_code_query = ','.join(error_code_query)
            error_desc_col = ','.join(error_desc_col_list)
            error_code_col = '+'.join(error_code_col_list)
       
            if error_desc_query != '':
                error_check_query = "select "+src_cols+" , concat_ws('',"+error_desc_col+") as Error_Desc_E2_AK,("+error_code_col+") as Error_Code_E2_AK  from (select * , " + error_desc_query + " , " + error_code_query+" from E2_AK_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E2_AK')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E2_AK')
            error_code_columns.append('Error_Code_E2_AK')

        ######################## E2 AK JDE Validation ############################
        if len(column_list_e2_jde_date) > 0:
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)       
            df =  df.withColumn("Error_Desc_E2_JDE_AK",lit(''))
            df =  df.withColumn("Error_Code_E2_JDE_AK",lit(0))        
            df.registerTempTable("E2_JDE_AK_Table")            
            error_desc_query = []
            error_code_query= []
            error_desc_col_list = []
            error_code_col_list = [] 

            for src_col in column_list_e2_jde_date:
                error_desc_query.append("case when trim(" + src_col +") = '' or "+ src_col +" is null or "+ src_col +" = 0 then 'E2:"+src_col+",' end as "+ src_col + "_Error_Desc")
                error_code_query.append("case when trim(" + src_col +") = '' or "+ src_col +" is null or "+ src_col +" = 0 then 1 else 0 end as "+ src_col + "_Error_Code")
                error_desc_col_list.append(src_col + '_Error_Desc')
                error_code_col_list.append(src_col + '_Error_Code')      
            
            error_desc_query = ','.join(error_desc_query)
            error_code_query = ','.join(error_code_query)
            error_desc_col = ','.join(error_desc_col_list)
            error_code_col = '+'.join(error_code_col_list)
       
            if error_desc_query != '':
                error_check_query = "select "+src_cols+" , concat_ws('',"+error_desc_col+") as Error_Desc_E2_JDE_AK,("+error_code_col+") as Error_Code_E2_JDE_AK  from (select * , " + error_desc_query + " , " + error_code_query+" from E2_JDE_AK_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E2_JDE_AK')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E2_JDE_AK')
            error_code_columns.append('Error_Code_E2_JDE_AK')

        
        log.info("E2 Validation Done")

        ########################################################################
        #                  E3 Error Check Validation                           #
        ########################################################################
        ######################## E3 NULL VALIDATION ############################        
        if len(column_list_e3_null) > 0:
            df = spark.read.format("parquet").load(temp_error_path)               
            src_cols = ','.join(df.columns)       
            df =  df.withColumn("Error_Desc_E3_NULL",lit(''))
            df =  df.withColumn("Error_Code_E3_NULL",lit(0))        
            df.registerTempTable("E3_NULL_Table")      
            e3_col_list =[]        
            for col in column_list_e3_null:
                e3_col_list.append(col + '_E3')        
            error_desc_query = []
            error_desc_col_list = [] 

            for src_col, e3_col in zip(column_list_e3_null,e3_col_list):
                error_desc_query.append("case when trim("+ src_col +") is null then '' else (case when trim("+ e3_col + ") is null then 'E3:"+src_col+",' end)end as "+ src_col + "_Error_Desc")
                error_desc_col_list.append(src_col + '_Error_Desc')       
            
            error_desc_query = ','.join(error_desc_query)
            error_desc_col = ','.join(error_desc_col_list)
       
            if error_desc_query != '':
                error_check_query = "select "+src_cols+" , Error_Code_E3_NULL,concat_ws('',"+error_desc_col+") as Error_Desc_E3_NULL from (select * , " + error_desc_query + " from E3_NULL_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E3_NULL')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E3_NULL')
            error_code_columns.append('Error_Code_E3_NULL')

        ######################## E3 NOT NULL VALIDATION ############################                        
        if len(column_list_e3_not_null) > 0 :
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)
            df =  df.withColumn("Error_Desc_E3_NOT_NULL",lit(''))
            df =  df.withColumn("Error_Code_E3_NOT_NULL",lit(0))       
            df.registerTempTable("E3_NOT_NULL_Table")
            e3_col_list =[]        
            for col in column_list_e3_not_null:
                e3_col_list.append(col + '_E3')        
            error_desc_query = []
            error_code_query= []
            error_desc_col_list = []
            error_code_col_list = []

            for src_col, e3_col in zip(column_list_e3_not_null,e3_col_list):
                error_desc_query.append("case when trim("+ src_col +") is null then '' else (case when trim("+ e3_col + ") is null then 'E3:"+src_col+",' end)end as "+ src_col + "_Error_Desc")
                error_code_query.append("case when trim("+ src_col +") is null then 0 else (case when trim("+ e3_col + ") is null then 1 else 0 end)end as "+ src_col + "_Error_Code")
                error_desc_col_list.append(src_col + '_Error_Desc')
                error_code_col_list.append(src_col + '_Error_Code')
            
            error_desc_query = ','.join(error_desc_query)
            error_code_query = ','.join(error_code_query)
            error_desc_col = ','.join(error_desc_col_list)
            error_code_col = '+'.join(error_code_col_list)
        
            if error_desc_query != '':
                error_check_query = "select "+src_cols+" , concat_ws('',"+error_desc_col+") as Error_Desc_E3_NOT_NULL,("+error_code_col+") as Error_Code_E3_NOT_NULL  from (select * , " + error_desc_query + " , " + error_code_query+" from E3_NOT_NULL_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E3_NOT_NULL')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E3_NOT_NULL')
            error_code_columns.append('Error_Code_E3_NOT_NULL')

                
        log.info("E3 Validation Done")

        ########################################################################
        #                  E4 Error Check Validation                           #
        ########################################################################
        ######################## E4 NON AK VALIDATION ############################
        if len(column_list_e4) > 0:
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)       
            df =  df.withColumn("Error_Desc_E4_NON_AK",lit(''))
            df =  df.withColumn("Error_Code_E4_NON_AK",lit(0))        
            df.registerTempTable("E4_NON_AK_Table")            
            error_desc_query = []
            error_desc_col_list = [] 

            for src_col in column_list_e4:
                error_desc_query.append("case when trim(" + src_col +") = '' or trim("+ src_col +") is null then 'E4:"+src_col+",' end as "+ src_col + "_Error_Desc")
                error_desc_col_list.append(src_col + '_Error_Desc')      
            
            error_desc_query = ','.join(error_desc_query)
            error_desc_col = ','.join(error_desc_col_list)
       
            if error_desc_query != '':
                error_check_query = "select "+src_cols+" , Error_Code_E4_NON_AK,concat_ws('',"+error_desc_col+") as Error_Desc_E4_NON_AK from (select * , " + error_desc_query + " from E4_NON_AK_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E4_NON_AK')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E4_NON_AK')
            error_code_columns.append('Error_Code_E4_NON_AK')

        ######################## E4 JDE NON AK VALIDATION ############################
        if len(column_list_e4) > 0:
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)       
            df =  df.withColumn("Error_Desc_E4_JDE_NON_AK",lit(''))
            df =  df.withColumn("Error_Code_E4_JDE_NON_AK",lit(0))        
            df.registerTempTable("E4_JDE_NON_AK_Table")           
            error_desc_query = []
            error_desc_col_list = [] 

            for src_col in column_list_e4_jde_date:
                error_desc_query.append("case when trim(" + src_col +") = '' or trim("+ src_col +") is null or trim("+ src_col +") = 0 then 'E4:"+src_col+",' end as "+ src_col + "_Error_Desc")
                error_desc_col_list.append(src_col + '_Error_Desc')      
            
            error_desc_query = ','.join(error_desc_query)
            error_desc_col = ','.join(error_desc_col_list)
       
            if error_desc_query != '':
                error_check_query = "select "+src_cols+" , Error_Code_E4_JDE_NON_AK,concat_ws('',"+error_desc_col+") as Error_Desc_E4_JDE_NON_AK from (select * , " + error_desc_query + " from E4_JDE_NON_AK_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E4_JDE_NON_AK')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E4_JDE_NON_AK')
            error_code_columns.append('Error_Code_E4_JDE_NON_AK')

        log.info("E4 Validation Done")
        ########################################################################
        #                  E5 Error Check Validation                           #
        ########################################################################
        ######################## E5 NULL VALIDATION ############################
        if len(column_list_e5_null) > 0 :
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)
            df =  df.withColumn("Error_Desc_E5_NULL",lit(''))
            df =  df.withColumn("Error_Code_E5_NULL",lit(0))        
            df.registerTempTable("E5_NULL_Table")       
            final_cols = df.columns
            e3_col_list =[]
            e3_e5_col_list =[]
            e5_col_list =[]
            e3_e5_src_col_list =[]
            src_col_list =[]
            for col in column_list_e5_null:
                if (col + '_E3') in final_cols:
                    e3_col_list.append(col + '_E3')
                    e3_e5_col_list.append(col + '_E5')
                    e3_e5_src_col_list.append(col)
                else:
                    e5_col_list.append(col + '_E5')
                    src_col_list.append(col)
            
            error_desc_query = []
            error_desc_col_list = []

            for src_col, e3_col,e5_col in zip(e3_e5_src_col_list,e3_col_list,e3_e5_col_list):
                error_desc_query.append("case when trim("+ src_col +") is null then '' else (case when trim("+ e3_col + ") is null then '' else(case when trim("+e5_col+") is null then 'E5:"+src_col+",' end)end)end as "+ src_col + "_Error_Desc")
                error_desc_col_list.append(src_col + '_Error_Desc')
            for src_col, e5_col in zip(src_col_list,e5_col_list):
                error_desc_query.append("case when trim("+ src_col +") is null then '' else (case when trim("+ e5_col + ") is null then 'E5:"+src_col+",' end)end as "+ src_col + "_Error_Desc")
                error_desc_col_list.append(src_col + '_Error_Desc')
            
            error_desc_query = ','.join(error_desc_query)
            error_desc_col = ','.join(error_desc_col_list)
        
            if error_desc_query != '':
                error_check_query = "select "+src_cols+" , Error_Code_E5_NULL, concat_ws('',"+error_desc_col+") as Error_Desc_E5_NULL from (select * , " + error_desc_query + " from E5_NULL_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E5_NULL')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E5_NULL')
            error_code_columns.append('Error_Code_E5_NULL')

        ######################## E5 NOT NULL VALIDATION ############################ 
        if len(column_list_e5_not_null) > 0:
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)
            df =  df.withColumn("Error_Desc_E5_NOT_NULL",lit(''))
            df =  df.withColumn("Error_Code_E5_NOT_NULL",lit(0))
            df.registerTempTable("E5_NOT_NULL_Table")        
            final_cols = df.columns
            e3_col_list =[]
            e3_e5_col_list =[]
            e5_col_list =[]
            e3_e5_src_col_list =[]
            src_col_list =[]
            for col in column_list_e5_not_null:
                if (col + '_E3') in final_cols:
                    e3_col_list.append(col + '_E3')
                    e3_e5_col_list.append(col + '_E5')
                    e3_e5_src_col_list.append(col)
                else:
                    e5_col_list.append(col + '_E5')
                    src_col_list.append(col)
            
            error_desc_query = []
            error_code_query= []
            error_desc_col_list = []
            error_code_col_list = []

            for src_col, e3_col,e5_col in zip(e3_e5_src_col_list,e3_col_list,e3_e5_col_list):
                error_desc_query.append("case when trim("+ src_col +") is null then '' else (case when trim("+ e3_col + ") is null then '' else(case when trim("+e5_col+") is null then 'E5:"+src_col+",' end)end)end as "+ src_col + "_Error_Desc")
                error_code_query.append("case when trim("+ src_col +") is null then 1 else (case when trim("+ e3_col + ") is null then 1 else(case when trim("+e5_col+") is null then 1 else 0 end)end)end as "+ src_col + "_Error_Code")
                error_desc_col_list.append(src_col + '_Error_Desc')
                error_code_col_list.append(src_col + '_Error_Code')
            for src_col, e5_col in zip(src_col_list,e5_col_list):
                error_desc_query.append("case when trim("+ src_col +") is null then '' else (case when trim("+ e5_col + ") is null then 'E5:"+src_col+",' end)end as "+ src_col + "_Error_Desc")
                error_code_query.append("case when trim("+ src_col +") is null then 1 else (case when trim("+ e5_col + ") is null then 1 else 0 end)end as "+ src_col + "_Error_Code")
                error_desc_col_list.append(src_col + '_Error_Desc')
                error_code_col_list.append(src_col + '_Error_Code')
            
            error_desc_query = ','.join(error_desc_query)
            error_code_query = ','.join(error_code_query)
            error_desc_col = ','.join(error_desc_col_list)
            error_code_col = '+'.join(error_code_col_list)

            if error_desc_query != '':
                error_check_query = "select "+src_cols+" , concat_ws('',"+error_desc_col+") as Error_Desc_E5_NOT_NULL,("+error_code_col+") as Error_Code_E5_NOT_NULL  from (select * , " + error_desc_query + " , " + error_code_query+" from E5_NOT_NULL_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            temp_error_path = error_table_path.replace('error', 'temp/error_E5_NOT_NULL')
            result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
            error_desc_columns.append('Error_Desc_E5_NOT_NULL')
            error_code_columns.append('Error_Code_E5_NOT_NULL')

                
        log.info("E5 Validation Done")

        ########################################################################
        #                  E6 Error Check Validation                           #
        ########################################################################
        if len(column_list_e6) > 0:
            df = spark.read.format("parquet").load(temp_error_path)
            src_cols = ','.join(df.columns)
            df =  df.withColumn("Error_Desc_E6",lit(''))
            df =  df.withColumn("Error_Code_E6",lit(0))
            df.registerTempTable("E6_Table")
            e6_col_list = []
            for col in column_list_e6:
                e6_col_list.append(col + '_E6')
                           
            error_desc_query = []
            error_code_query= []
            error_desc_col_list = []
            error_code_col_list = []

            for e6_col,src_col in zip(e6_col_list,column_list_e6):
                error_desc_query.append("case when trim("+ e6_col +") is null then 'E6:"+src_col+",' end as "+ src_col + "_Error_Desc")
                error_code_query.append("case when trim("+ e6_col +") is null then 1 else 0 end as "+ src_col + "_Error_Code")
                error_desc_col_list.append(src_col + '_Error_Desc')
                error_code_col_list.append(src_col + '_Error_Code')
                        
            error_desc_query = ','.join(error_desc_query)
            error_code_query = ','.join(error_code_query)
            error_desc_col = ','.join(error_desc_col_list)
            error_code_col = '+'.join(error_code_col_list)

            if error_desc_query != '':
                error_check_query = "select "+src_cols+" , concat_ws('',"+error_desc_col+") as Error_Desc_E6,("+error_code_col+") as Error_Code_E6  from (select * , " + error_desc_query + " , " + error_code_query+" from E6_Table)"
                result_df= spark.sql(error_check_query)
            else:
                result_df = df
            error_desc_columns.append('Error_Desc_E6')
            error_code_columns.append('Error_Code_E6')

        
        temp_error_path = error_table_path.replace('error', 'temp/error_E6')
        result_df.write.format("parquet").mode("overwrite").save(temp_error_path)
        
        log.info("E6 Validation Done")
        ######################## Forming Error and Error Free Dataframe ############################
        drop_columns = error_desc_columns + error_code_columns
        error_code_columns = '+'.join(error_code_columns)
        final_df = spark.read.format("parquet").load(temp_error_path)
        column_list = final_df.columns
        final_column_list = [i for i in column_list if i not in drop_columns] + ['Error_Desc','Error_Code']
        output_df = final_df.withColumn('Error_Desc', concat_ws('',*error_desc_columns)).withColumn('Error_Code', expr(error_code_columns)).select(final_column_list)
        src_col_list = []
        for cols in output_df.columns:
            if ('_E5' in cols.strip().upper()) or ('_E3' in cols.strip().upper())  or ('E1_TS' in cols) or ('_E1_NJDE' in cols) or ('_E6' in cols.strip().upper()):
                continue
            else:
                src_col_list.append(cols)
        error_df = output_df.select(src_col_list)
        
        df_error = error_file_generation(error_df, error_table_path)

        for cols in output_df.columns:
            if '_E5' in cols or '_E3' in cols or '_E6' in cols:
                output_df = output_df.drop(cols[0:(len(cols) - 3)])
        for cols in output_df.columns:
            if '_E1_TS' in cols:
                output_df = output_df.drop(cols[0:(len(cols) - 6)])
                output_df = output_df.withColumnRenamed(cols, cols[0:(len(cols) - 6)])
        for cols in output_df.columns:
            if '_E1_NJDE' in cols:
                output_df = output_df.drop(cols[0:(len(cols) - 8)])
                output_df = output_df.withColumnRenamed(cols, cols[0:(len(cols) - 8)])

        for cols in output_df.columns:
            if '_E3' in cols:
                output_df = output_df.drop(cols)
        

        error_free_df = output_df.filter("Error_Code ==0")
        
        column_list_e1_jde_timestamp = column_list_e1_jde_ts_non_audit + column_list_e1_jde_ts_audit + column_list_e1_ak_ts
        column_list_jde_date = column_list_e1_non_ak_date + column_list_e1_ak_date
        column_list_non_jde_date = column_list_e1_non_jde_ak + column_list_e1_non_jde_non_ak
        result_df = data_conversion(error_free_df, column_list_jde_date, column_list_e4, column_list_e1_jde_timestamp,
                                    column_list_non_jde_date, column_list_e4_jde_date)

        log.info("Error Free Dataframe Count : "+str(result_df.count()))


        if result_df.rdd.isEmpty():
            if not df_error==None:
                df_error.write.format("parquet").mode("append").save(error_table_path)
                return result_df, df_error
        return result_df, df_error
    except Exception as err:
        e = get_exception()
        print(err)
        return None, None

def error_file_generation(df, error_table_path):    
    try:
        if df.rdd.isEmpty():
            log.warn("Input Dataframe is empty!")
            return None
        else:
            df_error = df.filter("ERROR_DESC !=''")
            df_error = df_error.withColumn('ETL_REC_REJCTD_IND', expr("case when Error_Code = 0 then 'N' else 'Y' end"))
            
            if df_error.rdd.isEmpty():
                log.info("Error Dataframe is empty!")
            else:
                df_error = df_error.drop('ERROR_CODE')
                log.info("Error Dataframe Count : "+str(df_error.count()))
                log.info("Error DataFrame Generated")
            return df_error
    except:
        e = get_exception()
        print(e)

def data_conversion(df_output, column_list_e1, column_list_e4, column_list_e1_jde_timestamp, column_list_non_jde_date,
                    column_list_e4_jde_date):
    try:
        for cols in column_list_e1:
            df_output = df_output.withColumn(cols,when((trim(col(cols)) == '') | (col(cols).isNull()) | ((col(cols) == 0) | 
            ~(col(cols).cast(DecimalType(38)).between(1, 8099365))),'1899-12-31 00:00:00').when(from_unixtime(unix_timestamp((col(cols).cast(DecimalType(38)) + 1900000)
            .cast(StringType()),'yyyyDDD')).isNull(),'1899-12-31 00:00:00')
            .otherwise(from_unixtime(unix_timestamp((col(cols).cast(DecimalType(38)) + 1900000).cast(StringType()),'yyyyDDD'))))

        for cols in column_list_e1_jde_timestamp:
            df_output = df_output.withColumn(cols, when(
                col(cols).isNull() | ~(substring(col(cols), 1, 2).between(0, 23)) | ~(
                    substring(col(cols), 3, 2).between(0, 59)) | ~(substring(col(cols), 5, 2).between(0, 59)),
                lit('00:00:00')).otherwise(
                concat(substring(col(cols), 1, 2), lit(':'), substring(col(cols), 3, 2), lit(':'),
                       substring(col(cols), 5, 2))))

        for cols in column_list_non_jde_date:
            df_output = df_output.withColumn(cols, when(col(cols).isNull(), unix_timestamp(lit('1899-12-31 00:00:00')).cast(TimestampType())).otherwise(col(cols)))
        for cols in column_list_e4_jde_date:
            df_output = df_output.withColumn(cols, when(col(cols).isNull(), unix_timestamp(lit('1899-12-31 00:00:00')).cast(TimestampType())).otherwise(col(cols)))

        for name, types in df_output.dtypes:
            if name in column_list_e4:
                print("Name : " + name + " _ Types : " + types)
                if ('int'.upper() in types.upper()) or ('decimal'.upper() in types.upper()) or (
                        'float'.upper() in types.upper()) or ('double'.upper() in types.upper()) or (
                        'long'.upper() in types.upper()):
                    df_output = df_output.withColumn(name,
                                                     when((col(name).isNull()) | (trim(col(name)) == ''),
                                                          0).otherwise(
                                                         col(name)))
                elif 'string'.upper() in types.upper():
                    print("String Conversion")
                    df_output = df_output.withColumn(name,
                                                     when((col(name).isNull()) | (trim(col(name)) == ''),
                                                          '~').otherwise(
                                                         col(name)))

        df_output = df_output.drop('ERROR_DESC', 'ERROR_CODE')
        return df_output
    except:
        e = get_exception()
        print(e)