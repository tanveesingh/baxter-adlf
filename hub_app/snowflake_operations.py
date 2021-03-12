import re
from hub_app.spoke.spark import get_session
import hub_config as ccs
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
import snowflake.connector
from hub_app.log import setup_custom_logger
from run import hub_parser
from hub_app.func_utils import get_exception



ETL_PRCS_NAME = hub_parser.parse_args().prcs_name
REGION = hub_parser.parse_args().region
PART_VAL = hub_parser.parse_args().part_val
log = None



def get_sf_pkb(param):
    try:
        with open(param.sf_private_key_path, "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=param.sf_passphrase.encode(),
                backend=default_backend()
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())
        return pkb
    except Exception as err:
        print(err)
        return None


def get_sf_pkb_spark(param):
    try:
        with open(param.sf_private_key_path, "rb") as key:
            pp_key = serialization.load_pem_private_key(key.read(),
                                                        password=param.sf_passphrase.encode(),
                                                        backend=default_backend()
                                                        )

        pkb = pp_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())
        pkb = pkb.decode("UTF-8")
        pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n", "", pkb).replace("\n", "")
        return pkb
    except:
        e = get_exception()
        return None

'''
Description : Function to create a new Snowflake connection
'''
def get_sf_con(param):
    try:
        pkb = get_sf_pkb(param)

        ctx = snowflake.connector.connect(
            user=param.sf_username,
            account=param.sf_account_name,
            private_key=pkb,
            warehouse=param.sf_warehouse,
            database=param.sf_database,
            schema=param.sf_schema,
            role=param.sf_role
        )
        return ctx
    except Exception as err:
        print(err)

'''
Description : Function to close a Snowflake connection
'''
def close_sf_con(conn):
    if conn is not None:
        conn.close()
        log.info("Connection is closed")

'''
Description : Function to write data into Snowflake Stage Table
Input : Spark Dataframe,Stage Table Name, Stage Schema Name
Output : TRUE
'''
def write_sf(spark, job_param, df, **kwargs):
    try:
        if df.rdd.isEmpty():
            log.warn("Dataframe is Empty")
            return True, None
        
        stage_table_name = kwargs.get('stage_table_name').strip()
        stage_schema_name = kwargs.get("stage_schema_name").strip()
        #print(f'Before: {stage_schema_name}.{stage_table_name}')
        
        use_job_param = kwargs.get('use_job_param', 'FALSE').strip().upper()
        if use_job_param == 'TRUE':
            stage_table_name = job_param.get(stage_table_name, stage_table_name)
            stage_schema_name = job_param.get(stage_schema_name, stage_schema_name)
            #print(f'After: {stage_schema_name}.{stage_table_name}')
        
        stage_table_name = stage_table_name.upper()
        stage_schema_name = stage_schema_name.upper()
        
        database_name = ccs.sf_database
        if 'truncate_flag' in kwargs:
            if kwargs.get('truncate_flag').strip().upper() == 'TRUE':
                stats = load_sf_table(df, job_param, **kwargs)
                if stats:
                    return True,None
        con = get_sf_con(ccs)
        cursor = con.cursor()
        truncate_command = "TRUNCATE TABLE " + database_name + "." + stage_schema_name + "." + stage_table_name
        log.info(truncate_command)
        cursor.execute(truncate_command)
        con.commit()
        log.info("TRUNCATE STAGE TABLE COMPLETED")
        pkb = get_sf_pkb_spark(ccs)
        sfOptions = {
            "sfURL": ccs.sf_url,
            "sfUser": ccs.sf_username,
            "sfDatabase": ccs.sf_database,
            "sfSchema": stage_schema_name,
            "sfWarehouse": ccs.sf_warehouse,
            "sfRole": ccs.sf_role,
            "pem_private_key": pkb

        }
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option("dbtable", stage_table_name) \
            .save()
        log.info("Load to Stage table completed")
        close_sf_con(con)
        status = update_sf_table(job_param, **kwargs)
        if status:
            return True, None
        else:
            return False,None
    except:
        e = get_exception()
        return False, e


'''
Description : Function to merge data into Snowflake Table
Input : Spark Dataframe,Target Table Name, Target Schema Name
Output : TRUE
'''
def update_sf_table(job_param, **kwargs):
    try:
        con = get_sf_con(ccs)
        
        target_table_name = kwargs.get('target_table_name').strip()
        target_schema_name = kwargs.get('target_schema_name').strip()
        #print(f'Before: {target_schema_name}.{target_table_name}')
        
        stage_table_name = kwargs.get('stage_table_name').strip()
        stage_schema_name = kwargs.get('stage_schema_name').strip()
        #print(f'Before: {stage_schema_name}.{stage_table_name}')
        
        use_job_param = kwargs.get('use_job_param', 'FALSE').strip().upper()
        if use_job_param == 'TRUE':
            target_table_name = job_param.get(target_table_name, target_table_name)
            target_schema_name = job_param.get(target_schema_name, target_schema_name)
            #print(f'After: {target_schema_name}.{target_table_name}')
            stage_table_name = job_param.get(stage_table_name, stage_table_name)
            stage_schema_name = job_param.get(stage_schema_name, stage_schema_name)
            #print(f'After: {stage_schema_name}.{stage_table_name}')
        
        target_table_name = target_table_name.upper()
        target_schema_name = target_schema_name.upper()
        
        stage_table_name = stage_table_name.upper()
        stage_schema_name = stage_schema_name.upper()
        
        delimiter = kwargs.get("delimiter")
        cols_list = kwargs.get("column_list")
        if 'truncate_flag' in kwargs:
            truncate_flag = kwargs.get('truncate_flag').strip().upper()
        else:
            truncate_flag = 'FALSE'
        if truncate_flag == 'TRUE':
            trnct_flg = True
        else:
            trnct_flg = False
        if trnct_flg:
            cursor = con.cursor()
            truncate_command = "TRUNCATE TABLE " + ccs.sf_database + "." + target_schema_name + "." + target_table_name
            print(truncate_command)
            cursor.execute(truncate_command)
            con.commit()
        if truncate_flag == 'FALSE':
            if 'surrogate_cols' in kwargs:
                surrogate_cols = kwargs.get('surrogate_cols')
                surrogate_cols = surrogate_cols.split(delimiter)
            else:
                surrogate_cols = []
            cols_list = cols_list.split(delimiter)
            target_table_name = str(target_table_name).strip()
            stage_tbl = "STAGE_" + target_table_name
            database_name = ccs.sf_database
            remove_columns = ['ETL_CRT_DTM']
            cursor = con.cursor()
            update_command = "MERGE INTO " + database_name + "." + target_schema_name + "." + target_table_name + " T1 USING " + database_name + "." + stage_schema_name + "." + stage_table_name + " T2 ON ("
            for items in surrogate_cols:
                if items in cols_list:
                    cols_list.remove(items)
            for items in surrogate_cols:
                update_command = update_command + " T1." + items + " = T2." + items + " AND"
            update_command = update_command[0: (len(update_command) - 3)]
            update_command = update_command + " ) WHEN NOT MATCHED THEN "
            for items in surrogate_cols:
                if items not in cols_list:
                    cols_list.append(items)
            update_command = update_command + "INSERT ("
            for items in cols_list:
                update_command = update_command + items.strip() + ","
            update_command = update_command[0:(len(update_command) - 1)]
            update_command = update_command + " ) VALUES ( "
            for items in cols_list:
                update_command = update_command + "T2." + items.strip() + ", "
            update_command = update_command[0:(len(update_command) - 2)]
            update_command = update_command + ") "
            for items in surrogate_cols:
                if items in cols_list:
                    cols_list.remove(items)
            update_command = update_command + " WHEN MATCHED THEN UPDATE SET "
            for items in cols_list:
                if items in remove_columns:
                    cols_list.remove(items)
            for items in cols_list:
                update_command = update_command + "T1." + items + " = T2." + items + ", "
            update_command = update_command[0:(len(update_command) - 2)]
            log.info(update_command)
            cursor.execute(update_command)
            con.commit()
            log.info("Merge Completed Successfully!")
        else:
            cols_list = cols_list.split(delimiter)
            insert_command = "INSERT INTO " +ccs.sf_database+ "."+ target_schema_name + "." + target_table_name + " ("
            for items in cols_list:
                insert_command = insert_command + items.strip() + ", "
            insert_command = insert_command[0:(len(insert_command) - 2)]
            insert_command = insert_command + " ) SELECT "
            for items in cols_list:
                insert_command = insert_command + items.strip() + ", "
            insert_command = insert_command[0:(len(insert_command) - 2)]
            insert_command = insert_command + " FROM " + ccs.sf_database+ "."+stage_schema_name + "." + stage_table_name
            cursor = con.cursor()
            cursor.execute(insert_command)
            log.info(insert_command)
            con.commit()
            log.info("Insert Completed Successfully!")
        close_sf_con(con)
        return True
    except:
        e = get_exception()
        return False

'''
Description : Function to read data from Snowflake Table
Input : Target Table Name, Target Schema Name
Output : Spark Dataframe
'''
def read_sf(param, job_param, spark, **kwargs):
    try:
        spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(
            spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
        schema = kwargs.get('schema_name')
        #print(f'Before: {schema}')

        use_job_param = kwargs.get('use_job_param', 'FALSE').strip().upper()
        if use_job_param == 'TRUE':
                schema = job_param.get(schema, schema)
                #print(f'After: {schema}')
        
        sfOptions = {
            "sfURL": param.sf_url,
            "sfUser": param.sf_username,
            "sfDatabase": param.sf_database,
            "sfSchema": schema,
            "sfWarehouse": param.sf_warehouse,
            "sfRole": param.sf_role,
            "pem_private_key": get_sf_pkb_spark(param)
    
        }
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        if 'query' in kwargs:
            query = kwargs.get('query')
            schema_name = kwargs.get("schema_name")
            table_name = kwargs.get("table_name")
            database_name = ccs.sf_database

    
            result_df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptions) \
                .option("query", query) \
                .load()
        elif 'query_file' in kwargs:
            query_file = kwargs.get('query_file')
            schema_name = kwargs.get("schema_name")
            database_name = ccs.sf_database
            read_file = spark.sparkContext.wholeTextFiles(query_file).collect()
            if read_file:
                rd_temp = read_file[0][1]
                if rd_temp:
                    query = rd_temp
            result_df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptions) \
                .option("query", query) \
                .load()
        else:
            schema_name = kwargs.get("schema_name")
            table_name = kwargs.get("table_name")
            #print(f'Before: {schema_name}.{table_name}')
            
            use_job_param = kwargs.get('use_job_param', 'FALSE').strip().upper()
            if use_job_param == 'TRUE':
                schema_name = job_param.get(schema_name, schema_name)
                table_name = job_param.get(table_name, table_name)
                #print(f'After: {schema_name}.{table_name}')
            
            database_name = ccs.sf_database
            full_table_name = database_name + "." + schema_name + "." + table_name
            SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
            result_df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptions) \
                .option("dbtable", full_table_name) \
                .load()
        return result_df
    except:
        e = get_exception()
        return None

'''
Description : Function to write data into Snowflake Table
Input : Spark Dataframe,Target Table Name, Target Schema Name
Output : TRUE
'''
def load_sf_table(df, job_param, **kwargs):
    try:
        table_name = kwargs.get('target_table_name')
        schema_name = kwargs.get("target_schema_name")
        schema_name = str(schema_name).strip()
        #print(f'Before: {schema_name}.{table_name}')
        
        use_job_param = kwargs.get('use_job_param', 'FALSE').strip().upper()
        if use_job_param == 'TRUE':
            table_name = job_param.get(table_name, table_name)
            schema_name = job_param.get(schema_name, schema_name)
            #print(f'After: {schema_name}.{table_name}')
        
        schema_name = schema_name.upper()
        
        database_name = ccs.sf_database
        target_table_name = database_name + "." + schema_name + "." + str(table_name)
        con = get_sf_con(ccs)
        cursor = con.cursor()
        truncate_command = "TRUNCATE TABLE " + target_table_name
        log.info(truncate_command)
        cursor.execute(truncate_command)
        con.commit()
        log.info("TRUNCATE TABLE COMPLETED")
        pkb = get_sf_pkb_spark(ccs)
        sfOptions = {
            "sfURL": ccs.sf_url,
            "sfUser": ccs.sf_username,
            "sfDatabase": ccs.sf_database,
            "sfSchema": schema_name,
            "sfWarehouse": ccs.sf_warehouse,
            "sfRole": ccs.sf_role,
            "pem_private_key": pkb

        }
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        df.write.mode("append").format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option("dbtable", target_table_name) \
            .save()
        log.info("Load to SF table load completed")
        return True, None

    except Exception as err:
        e = get_exception()
        return False, e

'''
Description : Function to execute dml queries ito Snowflake Table
Input : SQL File
Output : TRUE
'''
def exec_sf_dml(param_dict, spark, **kwargs):
    try:
        param_dict = {key.encode().decode('utf-8'): value.encode().decode('utf-8') for (key, value) in param_dict.items()}
        
        sql_text = None
        if "sql_file" in kwargs:
            sql_file = kwargs.get("sql_file")
            if 'jp_ScheduleName' in param_dict:
                sql_file = sql_file.replace('$jp_ScheduleName', param_dict['jp_ScheduleName'].upper())
            if 'jp_LoadType' in param_dict:
                sql_file = sql_file.replace('$jp_LoadType', param_dict['jp_LoadType'])
            log.info(f"Reading File: {sql_file}")
            read_file = spark.sparkContext.wholeTextFiles(sql_file).collect()
            if read_file:
                rd_temp = read_file[0][1]
                if rd_temp:
                    sql_text = rd_temp
        elif "sql_text" in kwargs:
            sq_temp = kwargs.get("sql_text")
            if sq_temp and sq_temp.strip() != '':
                    sql_text = sq_temp
        if not sql_text:
                raise Exception("Param List provided for operation is incorrect.")
        
        if 'lookup_keys' in kwargs:
            lookup_keys = kwargs.get('lookup_keys')
            lookup_keys = lookup_keys.split("|")
            for item in lookup_keys:
                if item in param_dict:
                    sql_text = sql_text.replace(item, param_dict[item])

        con = get_sf_con(ccs)
        with con.cursor() as cur:
            cur.execute(sql_text)        
        return True
    except:
        e = get_exception()


'''
Description : Function to clone Snowflake Table
Input : Source Schema, Source Table, Target Schema, Target Table
Output : TRUE
'''
def clone_sf_table(**kwargs):
    try:
        conn = get_sf_con(ccs)
        source_schema = kwargs.get('source_schema').strip().upper()
        source_table = kwargs.get('source_table').strip().upper()
        delimiter = kwargs.get('delimiter').strip()
        target_clone_schema = kwargs.get('target_clone_schema').strip().upper()
        target_clone_schema = target_clone_schema.split(delimiter)
        target_clone_table = kwargs.get('target_clone_table').strip().upper()
        target_clone_table = target_clone_table.split(delimiter)
        for schema,table in zip(target_clone_schema, target_clone_table):
            clone_statement = "CREATE OR REPLACE TABLE "+ccs.sf_database+"."+schema+"."+table+" CLONE " + \
            ccs.sf_database+"."+source_schema+"."+source_table
            cursor = conn.cursor()
            cursor.execute(clone_statement)
            conn.commit()
        close_sf_con(conn)
        log.info("Cloning Completed")
        return True
    except:
        e =get_exception()
