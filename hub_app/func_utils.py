from collections import OrderedDict
import datetime as dt
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
import hub_app.const as cc
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import hub_config as ccs
import psycopg2
import re
from pyspark.sql.window import Window
import boto3
import sys
import time
import pyarrow as pa
import pyarrow.parquet as pq
from hub_app.log import setup_custom_logger
from run import hub_parser
import subprocess
import os
import hub_app.abc_util as abc
import linecache
import pandas
import numpy as np
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os

ETL_PRCS_NAME = hub_parser.parse_args().prcs_name
REGION = hub_parser.parse_args().region
PART_VAL = hub_parser.parse_args().part_val
etl_prcs_run_id = None
prcs_id = None
etl_job_run_id = None
part_val = None
etl_job_id = None
log = None

'''
Description : Function to create a new RDS connection
'''
def get_rds_con(self):
    try:
        rds = boto3.client('rds', region_name=self.rds_region)
        password = rds.generate_db_auth_token(DBHostname=self.rds_host, Port=self.rds_port, DBUsername=self.rds_user,
                                              Region=self.rds_region)
        conn = psycopg2.connect(user=self.rds_user, password=password, host=self.rds_host, database=self.rds_database,
                                options=f'-c search_path={self.rds_schema}')
        return conn

    except Exception as err:
        log.error(err)

'''
Description : Function to generate the current time
'''
def get_current_time(op_name):
    try:
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
        log.info("Executed {} operation at {}".format(op_name, dt_string))
    except Exception as err:
        log.warn(err)
        return None

'''
Description : Function to Close RDS Connection
'''
def close_rds_con(conn):
    if conn is not None:
        conn.close()


'''
Description :Function to Handle any exception
             This will update ABC entries in PRCS and JOB RUN tables with -1 and Aborted status
'''
def handle_exception():
    if part_val == None:
        abc.update_abc_job_failure(etl_job_run_id, etl_job_id)
        abc.update_abc_failure(etl_prcs_run_id, prcs_id)
        log.error("Application run has ended with errors")
    else:
        abc.update_abc_job_failure(etl_job_run_id, etl_job_id)
        log.error("Application run has ended with errors")


'''
Description : Function to catch Exception
'''
def get_exception(debug=True):
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    err_obj = {
        'error_code': str(exc_type),
        'error_message': str(exc_obj)
    }
    if debug:
        err_obj['error_at'] = 'EXCEPTION IN ({},LINE {} "{}")'.format(filename, lineno, line.strip())
    log.error(err_obj)
    handle_exception()
    return err_obj


'''
Description : Function to Print the error code and message
'''
def errorify(error_code='', error_message='', error_at=''):
    return {
        'error_code': str(error_code),
        'error_message': str(error_message),
        'error_at': str(error_at)
    }

'''
Description : Function to remove special characters from column name
'''
def keyfy(inp_str):
    return re.sub(r'[^A-Za-z0-9_-]+', '', inp_str).upper()

'''
Description : Functions to compare input columns with the source DF columns and map the new columns with old columns
'''
def cmp_rep(df_cols, inp_cols, new_cols):
    l_df_cols = [keyfy(x) for x in df_cols]
    l_inp_cols = [keyfy(x) for x in inp_cols]
    d_out_cols = OrderedDict()
    for i, k_df_col in enumerate(l_df_cols):
        for j, k_inp_col in enumerate(l_inp_cols):
            if k_df_col == k_inp_col:
                df_col = df_cols[i]
                inp_col = inp_cols[j]
                new_col = new_cols[j]
                d_out_cols[df_col] = new_col

    return d_out_cols

'''
Description : Functions to compare input columns withsource DF columns to find out if input columns exists
'''
def cmp_intersect(df_cols, inp_cols):
    l_df_cols = [keyfy(x) for x in df_cols]
    l_inp_cols = [keyfy(x) for x in inp_cols]
    res = OrderedDict()
    for i, k_df_col in enumerate(l_df_cols):
        for j, k_inp_col in enumerate(l_inp_cols):
            if k_df_col == k_inp_col:
                df_col = df_cols[i]
                res[df_col] = ""
    return res

def cmp_duplicates(left_cols, right_cols, left_name, right_name, left_rename_cols, right_rename_cols):
    common_cols = set(left_cols) & set(right_cols)
    select_cols = [col('A.{}'.format(x)) for x in left_cols] + [col('B.{}'.format(x)) for x in right_cols]

    rename_left_cols = []
    len_lrcols = len(left_rename_cols)
    for i, left_col in enumerate(left_cols):
        lrc = left_col
        if i < len_lrcols:
            lrc = left_rename_cols[i]
        rename_left_cols.append(lrc)

    rename_right_cols = []
    len_rrcols = len(right_rename_cols)
    for j, right_col in enumerate(right_cols):
        rrc = right_col
        if j < len_rrcols:
            rrc = right_rename_cols[j]
        elif right_col in common_cols:
            rrc = '{}_{}'.format(right_name, right_col)
        rename_right_cols.append(rrc)
    rename_cols = rename_left_cols + rename_right_cols
    return select_cols, rename_cols

'''
Description : Function to select columns from a Spark Dataframe
Input : Spark Dataframe,Columns
Output : Spark Dataframe
'''
def select_columns(df, **kwargs):
    try:
        delimiter = cc.input_column_delimiter
        columns = kwargs.get('columns')
        if "delimiter" in kwargs:
            delimiter = kwargs.get('delimiter')
        if columns == '':
            return df, None
        inp_columns = columns.split(delimiter)
        select_cols = []
        exc = False
        for cols in inp_columns:
            if cols in df.columns:
                select_cols.append(cols)
            else:
                raise Exception("Column {} does not exist".format(cols))
        df = df.select(select_cols)
        return df, None
    except:
        e = get_exception()
        return None, e

'''
Description : Function to rename columns of a Spark Dataframe
Input : Spark Dataframe,Old Columns,New Columns
Output : Spark Dataframe
'''
def rename_columns(df, **kwargs):
    try:
        old_columns = kwargs.get('old_columns')
        new_columns = kwargs.get('new_columns')
        delimiter = cc.input_column_delimiter
        old_columns = old_columns.strip().split(delimiter)
        new_columns = new_columns.strip().split(delimiter)
        if not old_columns or not new_columns:
            return df
        d_out_cols = cmp_rep(df.columns, old_columns, new_columns)
        if d_out_cols:
            for old_col, new_col in d_out_cols.items():
                df = df.withColumnRenamed(old_col, new_col)
        return df, None
    except:
        e = get_exception()
        return None, e


'''
Description : Function to drop columns of a Spark Dataframe
Input : Spark Dataframe,Columns
Output : Spark Dataframe
'''
def drop_columns(df, **kwargs):
    try:
        delimiter = cc.input_column_delimiter
        drop_cols = kwargs.get("columns")
        if "delimiter" in kwargs:
            delimiter = kwargs.get("delimiter")
        if drop_cols.strip() == '':
            e = "INVALID_PARAMS", "Param columns:<{}> has been incorrectly specified".format(drop_cols)
            raise Exception(e)
        l_drop_cols = drop_cols.split(delimiter)
        d_out_cols = cmp_intersect(df.columns, l_drop_cols)
        if d_out_cols:
            revised_drop_cols = d_out_cols.keys()
            if len(l_drop_cols) != len(revised_drop_cols):
                e = "INVALID_PARAMS", "Some of the drop columns could not be found in the dataframe columns."
                raise Exception(e)
            df = df.drop(*revised_drop_cols)
        else:
            e = "INVALID_PARAMS", "Param columns:<{}> has been incorrectly specified".format(drop_cols)
            raise Exception(e)
        return df, None
    except:
        e = get_exception()
        return None, e


'''
Description : Function to trim columns of a Spark Dataframe
Input : Spark Dataframe,Columns
Output : Spark Dataframe
'''
def cleanup_columns(df, **kwargs):
    try:
        delimiter = cc.input_column_delimiter
        columns = kwargs.get("columns")
        if "delimiter" in kwargs:
            delimiter = kwargs.get("delimiter")

        l_cols = columns.split(delimiter)
        d_out_cols = cmp_intersect(df.columns, l_cols)
        if d_out_cols:
            revised_cols = d_out_cols.keys()
            for r_col in revised_cols:
                df = df.withColumn(r_col, when ((trim(col(r_col)) == ''), None).otherwise(trim(col(r_col))))
        return df,None
    except:
        e = get_exception()
        return None, e

'''
Description : Function to generate surrogate key using md5
Input : Spark Dataframe,Columns
Output : Spark Dataframe
'''
def add_sk(spark, df, **kwargs):
    try:
        if "delimiter" in kwargs:
            delimiter = kwargs.get("delimiter")
        else:
            delimiter = '|'
        if "id_columns" in kwargs:
            id_columns = kwargs.get("id_columns").strip()
        else:
            e = "INVALID_PARAMS", " <id_columns> have not been provided"
            raise Exception(e)
        if "column_name" in kwargs:
            column_name = kwargs.get("column_name").strip()
        else:
            e = "INVALID_PARAMS", " <column_name> have not been provided"
            raise Exception(e)
        cols = id_columns.split(delimiter)
        df = df.withColumn(column_name, md5(concat_ws("|", *cols)))
        return df, None
    except:
        e = get_exception()
        return None, e

'''
Description : Function to Union two Spark Dataframe
Input : Two Spark Dataframe
Output : Spark Dataframe
'''
def union_dataset(inp_df, union_df_name):
    try:
        df = inp_df.union(union_df_name.select(inp_df.columns))
        return df, None
    except:
        e = get_exception()
        return None, e


'''
Description : Function to register Spark Dataframe as a Temp Table
Input : Spark Dataframe, Table Name, Delimiter
Output : Spark Dataframe
'''
def register_temp_table(d_dfs, **kwargs):
    try:
        delimiter = cc.input_column_delimiter
        df_names = kwargs.get('dataframe_name')
        table_names = kwargs.get('table_name')
        if "delimiter" in kwargs:
            delimiter = kwargs.get("delimiter")
        l_df_names = df_names.split(delimiter)
        l_table_names = table_names.split(delimiter)
        if len(l_df_names) != len(l_table_names):
            raise Exception("INVALID_PARAMS", "Param List provided for operation is incorrect.")
        res_set = zip(l_df_names, l_table_names)
        for item in res_set:
            df_name = item[0]
            table_name = item[1]
            if df_name in d_dfs:
                df = d_dfs.get(df_name)
                df.createOrReplaceTempView(table_name)
            else:
                raise Exception("INVALID_DATAFRAME_NAME","Dataframe <{}> has not been initialised".format(df_name))
        return True, None
    except:
        e = get_exception()
        return False, e


'''
Description : Function to run custom SQL on top of Spark Temp Table
Input : Spark Dataframe, Look Up Keys
Output : Spark Dataframe
'''
def run_sql(param_dict, spark, **kwargs):
    try:
        param_dict = {key.encode().decode('utf-8'): value.encode().decode('utf-8') for (key, value) in
                      param_dict.items()}
        if 'lookup_keys' in kwargs:
            lookup_keys = kwargs.get('lookup_keys')
            lookup_keys = lookup_keys.split("|")
            sql_text = None
            result_df_name = kwargs.get("result_dataframe")
            if "sql_file" in kwargs:
                sql_file = kwargs.get("sql_file")
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
                e = "INVALID_PARAMS", "Param List provided for operation is incorrect."
                raise Exception(e)
                return None, e
            else:
                for item in lookup_keys:
                    if item in param_dict:
                        sql_text = sql_text.replace(item, param_dict[item])
                result_df = spark.sql(sql_text).alias(result_df_name)
                return result_df, None
        else:
            sql_text = None
            result_df_name = kwargs.get("result_dataframe")
            if "sql_file" in kwargs:
                sql_file = kwargs.get("sql_file")
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
                return None, errorify("INVALID_PARAMS", "Param List provided for operation is incorrect.")
            else:
                result_df = spark.sql(sql_text).alias(result_df_name)
                return result_df, None
    except:
        e = get_exception()
        return None, e


'''
Description : Function to aggregate data in Spark Dataframe
Input : Spark Dataframe, Group By Columns, Aggregate Output
Output : Spark Dataframe
'''
def aggregate_dataset(df, **kwargs):
    try:
        delimiter = cc.input_column_delimiter
        result_dataframe_name = None
        group_by_cols = kwargs.get("group_by_columns")
        agg_func_ops = kwargs.get("aggregate_output")
        if ("result_dataframe") in kwargs:
            rd = kwargs.get("result_dataframe")
            if rd and rd.strip() != '':
                result_dataframe_name = rd
        l_grp_by_cols = group_by_cols.split(delimiter)
        l_agg_func = agg_func_ops.split(delimiter)
        l_agg_func = ["pyfunc." + x for x in l_agg_func]
        l_agg_func = [eval(x) for x in l_agg_func]
        result_df = df.groupBy(l_grp_by_cols).agg(*l_agg_func)
        if result_dataframe_name:
            result_df.alias(result_dataframe_name)
        return result_df, None
    except:
        e = get_exception()
        return None, e


'''
Description : Function to format path
Input : Path
Output : Path
'''
def format_path(fpath, **kwargs):
    try:

        timestamp_format = cc.default_timestamp_format
        if 'timestamp_format' in kwargs:
            tf = kwargs.get('timestamp_format')
            if tf and tf.strip() != '':
                timestamp_format = tf

        current_dtm = datetime.now()
        yyyy = current_dtm.strftime('%Y')
        MM = current_dtm.strftime('%m')
        dd = current_dtm.strftime('%d')
        HH = current_dtm.strftime('%H')
        mm = current_dtm.strftime('%M')
        ss = current_dtm.strftime('%S')
        ts = current_dtm.strftime(timestamp_format)
        fpath = fpath.format(timestamp=ts,
                             year=yyyy,
                             yyyy=yyyy,
                             month=MM,
                             MM=MM,
                             date=dd,
                             dd=dd,
                             hour=HH,
                             HH=HH,
                             minute=mm,
                             mm=mm,
                             second=ss,
                             ss=ss
                             )
        return fpath, None
    except:
        e = get_exception()
        return None, e

'''
Description : Function to write spark dataframe in target path
Input : Spark Dataframe,File Path,Write Mode, File Format
Output : True
'''

def write_file(spark, df, **kwargs):
    try:
        df_name = kwargs.get('dataframe_name')
        file_path = kwargs.get('file_path')
        fpath, e = format_path(file_path, **kwargs)
        if e:
            return False, e
        else:
            file_path = fpath
        file_format = kwargs.get('file_format')
        write_mode = 'append'
        distinct = False
        single_file = False
        save_as_table = False
        database_name = None
        table_name = None
        if "encoding" in kwargs:
            enc = kwargs.get('encoding')
        else:
            enc = 'UTF-8'
        if 'save_as_table' in kwargs:
            sat = kwargs.get('save_as_table')
            if sat:
                if sat.strip().upper() == 'TRUE':
                    save_as_table = True

        if save_as_table:
            database_name = 'default'
            table_name = df_name
            if 'database_name' in kwargs:
                dn = kwargs.get('database_name')
                if dn and dn.strip() != '':
                    database_name = dn

            if 'table_name' in kwargs:
                tn = kwargs.get('table_name')
                if tn and tn.strip() != '':
                    table_name = tn

        if 'write_mode' in kwargs:
            wm = kwargs.get('write_mode')
            if wm in ['append', 'overwrite', 'ignore', 'error']:
                write_mode = wm
        if 'distinct' in kwargs:
            v = kwargs.get('distinct')
            if v and v.strip().upper() == 'TRUE':
                distinct = True
        if 'single_file' in kwargs:
            sf = kwargs.get('single_file')
            if sf:
                sf = sf.strip().upper()
                if sf == 'TRUE':
                    single_file = True

        if distinct:
            df = df.distinct()
        if single_file:
            df = df.coalesce(1)

        if save_as_table:
            spark.conf.set("spark.sql.parquet.writeLegacyFormat", "True")
            cht_tags = {'table_name': table_name, 'database_name': database_name, 'file_format': file_format,
                        'file_path': file_path,
                        }
            return create_hive_table(spark, df, **cht_tags)
        else:
            if file_format == 'csv':
                df.write.option('header', True).option("multiline", "True").option("delimiter", ",") \
                    .option("quote", '"').option("encoding", enc).option('escape', '\\') \
                    .mode(write_mode).csv(file_path)
            elif file_format == 'parquet':
                if 'snapshot_mode' in kwargs:
                    snapshot_mode = kwargs.get('snapshot_mode').strip().upper()
                else:
                    snapshot_mode = 'FALSE'
                if snapshot_mode == 'MONTHLY':
                    ct_dt = datetime.now()
                    ct_str = ct_dt.strftime('%Y-%m')
                    snapshot_path = "p_month="+ct_str
                    file_path = file_path+snapshot_path+"/"
                    spark.conf.set("spark.sql.parquet.writeLegacyFormat", "True")
                    df.write.format(file_format).mode(write_mode).save(file_path)
                else:
                    spark.conf.set("spark.sql.parquet.writeLegacyFormat", "True")
                    df.write.format(file_format).mode(write_mode).save(file_path)
            else:
                if file_format == 'text':
                    df.coalesce(1).write.format(file_format).option("encoding", enc).mode(write_mode).save(file_path)
        return True, None
    except:
        e = get_exception()
        return False, e

'''
Description : Function to create HIVE table
Input : File Path,File Format,Database Name, Table_Name
Output : True
'''
def create_hive_table(spark, df, **kwargs):
    try:

        file_path = kwargs.get('file_path')
        file_format = kwargs.get('file_format').strip().upper()
        db_name = kwargs.get('database_name')
        table_name = kwargs.get('table_name')

        temp_table_name = table_name + '_temp'
        q_tbl = table_name
        if db_name:
            if db_name.strip() != '':
                q_tbl = db_name + '.' + table_name

        df.createOrReplaceTempView(temp_table_name)
        demo_text = "show databases"
        drop_sql_text = 'DROP TABLE IF EXISTS ' + q_tbl
        create_sql_text = """CREATE EXTERNAL TABLE {table_name}
                 STORED AS {format}
                 LOCATION '{location}'
                 AS SELECT * FROM {temp_table_name}
              """.format(table_name=q_tbl,
                         format='PARQUET',
                         location=file_path,
                         temp_table_name=temp_table_name
                         )
        try:
            drop_res = spark.sql(drop_sql_text)
        except:
            pass
        create_res = spark.sql(create_sql_text)
        return True, None
    except:
        e = get_exception()
        return False, e


'''
Description : Function to generate Join/Partition columns
Input : Columns, Delimiter
Output : Column List
'''
def get_join_key(key_cols, delimiter):
    try:
        partition_list = key_cols.split(delimiter)
        return partition_list, None
    except:
        e = get_exception()
        return None, e


'''
Description : Function to de-duplicate Source records and identify DML Flag
Input : Spark Dataframe, File Path
Output : Spark Dataframe
'''
def capture_dml_flag(spark, df, file_path, **kwargs):
    try:
        if "delimiter" in kwargs:
            delimiter = kwargs.get('delimiter')
        else:
            delimiter = '|'
        if "key_cols" in kwargs:
            key_cols = kwargs.get("key_cols")
        else:
            e = "Partitioning Key Columns Not Provided"
            raise Exception(e)
        if "orderby_col" in kwargs:
            orderby_col = kwargs.get("orderby_col")
        else:
            e = "ordering column not provided"
            raise Exception(e)
        if "file_path" in kwargs:
            file_path = kwargs.get("file_path")
        partition_list, error1 = get_join_key(key_cols, delimiter)
        log.info(f"Partition List : {partition_list}")

        window1 = Window.partitionBy(partition_list).orderBy(orderby_col)
        window2 = Window.partitionBy(partition_list).orderBy(col(orderby_col).desc())

        if df.rdd.isEmpty() == False:
            temp_df = df.withColumn("DML_FLAG_List", collect_list(col("OPERATION$")).over(window1))
            temp_df = temp_df.withColumn("DML_FLAG_List", col("DML_FLAG_List").cast(StringType())) \
                .withColumn("row_number", row_number().over(window2)) \
                .filter(col('row_number') == 1).drop(col('row_number')) \
                .withColumn("DML_FLAG_LIST", regexp_replace(regexp_replace(col("DML_FLAG_LIST"), "\\[", ""), "\\]", ""))
            temp1_df = temp_df.withColumn("FIRST_DML_FLAG", split(col('DML_FLAG_List'), ',').getItem(0)) \
                .withColumn("LAST_DML_FLAG", trim(reverse(split(col('DML_FLAG_List'), ',')).getItem(0)))

            final_df = temp1_df \
                .withColumn('DML_FLAG', when((col('FIRST_DML_FLAG') == 'I') & (col('LAST_DML_FLAG') == 'U'), 'I') \
                            .when((col('FIRST_DML_FLAG') == 'U') & (col('LAST_DML_FLAG') == 'U'), 'U') \
                            .when((col('FIRST_DML_FLAG') == 'D') & (col('LAST_DML_FLAG') == 'U'), 'U') \
                            .when((col('FIRST_DML_FLAG') == 'U') & (col('LAST_DML_FLAG') == 'I'), 'U') \
                            .when((col('FIRST_DML_FLAG') == 'I') & (col('LAST_DML_FLAG') == 'I'), 'I') \
                            .when((col('FIRST_DML_FLAG') == 'D') & (col('LAST_DML_FLAG') == 'I'), 'U') \
                            .when((col('FIRST_DML_FLAG') == 'U') & (col('LAST_DML_FLAG') == 'D'), 'D') \
                            .when((col('FIRST_DML_FLAG') == 'D') & (col('LAST_DML_FLAG') == 'D'), 'D') \
                            .when((col('FIRST_DML_FLAG') == 'I') & (col('LAST_DML_FLAG') == 'D'), 'D0')) \
                .filter(col('DML_FLAG') != 'D0') \
                .drop('DML_FLAG_List', 'FIRST_DML_FLAG', 'LAST_DML_FLAG', 'OPERATION$')
            final_df.write.format("parquet").mode("overwrite").save(file_path)
            return final_df, None
        else:
            return None, None
    except:
        e = get_exception()
        return None, e


'''
Description : Function to add audit columns
Input : Spark Dataframe, Column List, Column Value
Output : Spark Dataframe
'''
def add_audit_column(job_param,df,col_list,col_value,delimiter):
    try:
        if not delimiter:
            delimiter = cc.input_column_delimiter
        if col_value :
            expression = col_value.split(delimiter)
        else:
            e = "INVALID_PARAMS", " <col_value> for the new column has not been defined "
            raise Exception(e)
        if col_list:
            mod_column = col_list.split(delimiter)
        else:
            e = "INVALID_PARAMS", " <col_list> has not been defined "
            raise Exception(e)
        col_dict = {}
        eval_expr = []
        job_param = {key.encode().decode('utf-8'): value.encode().decode('utf-8') for (key, value) in job_param.items()}
        if len(mod_column) == len(expression):
            for i in range(0, len(mod_column)):
                if str(expression[i]) == 'jp_EnvSrcCd':
                    col_dict[str(mod_column[i])] = str(job_param["jp_EnvSrcCd"])
                elif str(expression[i]) == 'jp_DataSrcCd':
                    col_dict[str(mod_column[i])] = str(job_param["jp_DataSrcCd"])
                elif str(expression[i]) == 'jp_etl_prcs_run_id':
                    col_dict[str(mod_column[i])] = str(job_param["jp_etl_prcs_run_id"])
                elif str(expression[i]) == 'jp_etl_btch_num':
                    col_dict[str(mod_column[i])] = str(job_param["jp_etl_schd_run_id"])
                elif str(expression[i]) == 'jp_etl_job_name':
                    col_dict[str(mod_column[i])] = str(job_param["jp_etl_job_name"])
                else:
                    col_dict[str(mod_column[i])] = str(expression[i])
        else:
            raise Exception("MISMATCH IN NUMBER OF COLUMNS AND VALUES")
        for mod_col, expression in col_dict.items():
            if ('ETL_CRT_DTM' in mod_col) or ('ETL_UPDT_DTM' in mod_col):
                eval_expr = (
                    "df.withColumn(\'{}\',to_timestamp({}, 'yyyy_MM_dd HH_mm_ss'))".format(mod_col, expression))
            else:
                eval_expr = ("df.withColumn(\'{}\',lit('{}'))".format(mod_col, expression))
            df = eval(eval_expr)
        return df, None
    except:
        e = get_exception()
        return None, e



'''
Description : Function to delete records from default partition
Input : Source Dataframe,Target Path,Default Partition
Output : True/None
'''
def delete_records(spark, inp_df, target_read_path, **kwargs):
    try:
        if "delimiter" in kwargs:
            delimiter = kwargs.get('delimiter')
        else:
            delimiter = '|'
        if "key_cols" in kwargs:
            key_cols = kwargs.get("key_cols")
        else:
            e = "Joining Key Columns Not Provided"
            raise Exception(e)
        if "partitionby" in kwargs:
            partitionby = kwargs.get("partitionby")
        else:
            e = "Partition By column  not provided"
            raise Exception(e)
        if "defaultvalue" in kwargs:
            defaultvalue = kwargs.get("defaultvalue")
        else:
            e = "Default Partition column not provided"
            raise Exception(e)
        write_path = '/'.join(target_read_path.split('/')[:-3]) + '/temp/' + '/'.join(target_read_path.split('/')[-3:-1]) + '/' + partitionby + "=" + defaultvalue + '/'
        new_target_path = '/'.join(target_read_path.split('/')[3:])
        new_target_path = new_target_path + partitionby + "=" + defaultvalue + '/'
        s3 = boto3.client('s3', region_name='us-east-2')
        bucketname = ccs.translate_bucket_name
        response = s3.list_objects_v2(Bucket=bucketname, Prefix=new_target_path)
        if "Contents" in response:
            new_target_path = 's3://' + bucketname + '/' + new_target_path
            joining_key_list, error = get_join_key(key_cols, delimiter)
            log.info(f'Joining Key List : {joining_key_list}')
            check_value = defaultvalue
            if '-' in check_value:
                inp_df = inp_df.where(~(col(partitionby).like(check_value + "%"))).alias("inp")
            else:
                inp_df = inp_df.where(~(col(partitionby) == check_value)).alias("inp")
            target_df = spark.read.parquet(new_target_path).alias("target")
            join_df = target_df.join(inp_df, joining_key_list, 'left_outer') \
                .withColumn('Delete_Flag', expr("case when inp.ETL_CRT_DTM is not null then 'Y' else 'N' end")) \
                .filter(col('Delete_Flag') != 'Y').select('target.*')
            join_df.write.format("parquet").mode("overwrite").save(write_path)
            s3_file_copy(write_path + "|" + new_target_path)
        else:
            log.info("No Default Partition Present")
        return True, None
    except:
        e = get_exception()
        return None, e

'''
Description : Function to Merge Partitions for specific paths or Merge entire target
Input : Source Dataframe,Target Path
Output : True/None
'''
def merge_dataset(spark, src_df, file_path, part_val, sql_vars, **kwargs):
    try:
        if "delimiter" in kwargs:
            delimiter = kwargs.get('delimiter')
        else:
            delimiter = '|'
        if "key_cols" in kwargs:
            key_cols = kwargs.get("key_cols")
        else:
            e = "Joining Key Columns Not Provided"
            raise Exception(e)
        if "partition_cols" in kwargs:
            partitionby = kwargs.get("partition_cols")
            params = 'TRUE'
            log.info("Partitioned Load")
        else:
            params = 'FALSE'
            log.info("Non-Partitioned Load")

        join_keys_list, error1 = get_join_key(key_cols, delimiter)
        if params == 'TRUE':
            write_path = '/'.join(file_path.split('/')[:-3]) + '/temp/' + '/'.join(file_path.split('/')[-3:-1]) + '/' + partitionby + "=" + part_val + '/'
            file_path = '/'.join(file_path.split('/')[3:])
            file_path_new = file_path + partitionby + "=" + part_val + '/'
            s3 = boto3.client('s3', region_name='us-east-2')
            bucketname = ccs.translate_bucket_name
            response = s3.list_objects_v2(Bucket=bucketname, Prefix=file_path_new)

            if "Contents" in response:
                file_path_new = 's3://' + bucketname + '/' + file_path_new
                target_df = spark.read.format('parquet').load(file_path_new).alias("target")
                src_df = src_df.where(col(partitionby) == sql_vars).alias("incr")
                column_list = target_df.columns

                
                if ("deld_in_src_ind" in column_list) or ("DELD_IN_SRC_IND" in column_list): 
                    update_df = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("DELD_IN_SRC_IND") == 'N'), join_keys_list) \
                                      .select("upd.*", "target.ETL_CRT_DTM").select(column_list)

                    update_df.write.format("parquet").mode("overwrite").save(write_path)

                    update_df1_1 = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("DELD_IN_SRC_IND") == 'Y'), join_keys_list) \
                        .withColumn("Update_Flag",expr("case when upd.DELD_IN_SRC_IND = 'N' then 'Y' else 'N' end"))\
                        .filter("Update_Flag = 'Y'").select("upd.*", "target.ETL_CRT_DTM").select(column_list)

                    update_df1_1.write.format("parquet").mode("append").save(write_path)

                    update_df1_2 = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("DELD_IN_SRC_IND") == 'Y'), join_keys_list) \
                        .withColumn("Update_Flag",expr("case when upd.DELD_IN_SRC_IND = 'N' then 'Y' else 'N' end"))\
                        .filter("Update_Flag = 'N'").select(*join_keys_list, "target.*").select(column_list)

                    update_df1_2.write.format("parquet").mode("append").save(write_path)

                    insert_df = src_df.filter(col("DELD_IN_SRC_IND") == 'N').join(target_df, join_keys_list, 'left_outer') \
                                      .withColumn("Insert_Flag", expr("case when target.ETL_CRT_DTM is null then 'Y' else (case when target.ETL_CRT_DTM is not null and target.DELD_IN_SRC_IND = 'Y' then 'Y' else 'N' end)end")) \
                                      .filter(col("Insert_Flag") == 'Y').select("incr.*").select(column_list)

                    insert_df.write.format("parquet").mode("append").save(write_path)

                    unchanged_df = target_df.join(src_df, join_keys_list, 'left_outer') \
                                            .withColumn("UnChanged_Flag", expr("case when incr.ETL_CRT_DTM is null then 'Y' else(case when incr.ETL_CRT_DTM is not null and target.DELD_IN_SRC_IND = 'Y' then 'Y' else 'N' end)end")) \
                                            .filter(col("UnChanged_Flag") == 'Y').select("target.*").select(column_list)
                    unchanged_df.write.format("parquet").mode("append").save(write_path)
                else:
                    update_df = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("SCD_CUR_IND") == 'Y'), join_keys_list) \
                                      .select("upd.*", "target.ETL_CRT_DTM").select(column_list)

                    update_df.write.format("parquet").mode("overwrite").save(write_path)

                    update_df1_1 = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("SCD_CUR_IND") == 'N'), join_keys_list) \
                                         .withColumn("Update_Flag", expr("case when upd.SCD_CUR_IND = 'Y' then 'Y' else 'N' end")) \
                                         .filter("Update_Flag = 'Y'").select("upd.*", "target.ETL_CRT_DTM").select(column_list)

                    update_df1_1.write.format("parquet").mode("append").save(write_path)

                    update_df1_2 = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("SCD_CUR_IND") == 'N'), join_keys_list) \
                                         .withColumn("Update_Flag", expr("case when upd.SCD_CUR_IND = 'Y' then 'Y' else 'N' end")) \
                                         .filter("Update_Flag = 'N'").select(*join_keys_list, "target.*").select(column_list)

                    update_df1_2.write.format("parquet").mode("append").save(write_path)

                    insert_df = src_df.filter(col("SCD_CUR_IND") == 'Y').join(target_df, join_keys_list, 'left_outer') \
                                      .withColumn("Insert_Flag", expr("case when target.ETL_CRT_DTM is null then 'Y' else (case when target.ETL_CRT_DTM is not null and target.SCD_CUR_IND = 'N' then 'Y' else 'N' end)end")) \
                                      .filter(col("Insert_Flag") == 'Y').select("incr.*").select(column_list)

                    insert_df.write.format("parquet").mode("append").save(write_path)

                    unchanged_df = target_df.join(src_df, join_keys_list, 'left_outer') \
                                            .withColumn("UnChanged_Flag", expr("case when incr.ETL_CRT_DTM is null then 'Y' else(case when incr.ETL_CRT_DTM is not null and target.SCD_CUR_IND = 'N' then 'Y' else 'N' end)end")) \
                                            .filter(col("UnChanged_Flag") == 'Y').select("target.*").select(column_list)

                    unchanged_df.write.format("parquet").mode("append").save(write_path)

                log.info("Merging Dataset")
                s3_file_copy(write_path + "|" + file_path_new)
                log.info("Merge Completed")
            else:
                file_path_new = 's3://' + bucketname + '/' + file_path_new
                if 'DML_FLAG' in src_df.columns:
                    final_df = src_df.where(col(partitionby) == sql_vars).drop(partitionby, 'DML_FLAG')
                else:
                    final_df = src_df.where(col(partitionby) == sql_vars).drop(partitionby)
                log.info("Merging Dataset")
                final_df.write.format("parquet").mode("overwrite").save(file_path_new)
                log.info("Merge Completed")
        else:
            write_path = '/'.join(file_path.split('/')[:-3]) + '/temp/' + '/'.join(file_path.split('/')[-3:-1]) + '/'
            file_path = '/'.join(file_path.split('/')[3:])
            s3 = boto3.client('s3', region_name='us-east-2')
            bucketname = ccs.translate_bucket_name
            response = s3.list_objects_v2(Bucket=bucketname, Prefix=file_path)
            if "Contents" in response:
                file_path_new = 's3://' + bucketname + '/' + file_path
                target_df = spark.read.format('parquet').load(file_path_new).alias("target")
                temp_write_path = 's3://' + bucketname + '/' + '/'.join(file_path.split('/')[:-3]) + '/temp/' + \
                                   '/'.join(file_path.split('/')[-3:-1]) + '_temp/'
                src_df.write.format("parquet").mode("overwrite").save(temp_write_path)
                src_df = spark.read.parquet(temp_write_path)
                src_df = src_df.alias("incr")
                column_list = target_df.columns

                if ("deld_in_src_ind" in column_list) or ("DELD_IN_SRC_IND" in column_list): 
                    update_df = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("DELD_IN_SRC_IND") == 'N'), join_keys_list) \
                                      .select("upd.*", "target.ETL_CRT_DTM").select(column_list)

                    update_df.write.format("parquet").mode("overwrite").save(write_path)

                    update_df1_1 = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("DELD_IN_SRC_IND") == 'Y'), join_keys_list) \
                                         .withColumn("Update_Flag", expr("case when upd.DELD_IN_SRC_IND = 'N' then 'Y' else 'N' end")) \
                                         .filter("Update_Flag = 'Y'").select("upd.*", "target.ETL_CRT_DTM").select(column_list)

                    update_df1_1.write.format("parquet").mode("append").save(write_path)

                    update_df1_2 = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("DELD_IN_SRC_IND") == 'Y'), join_keys_list) \
                                         .withColumn("Update_Flag", expr("case when upd.DELD_IN_SRC_IND = 'N' then 'Y' else 'N' end")) \
                                         .filter("Update_Flag = 'N'").select(*join_keys_list, "target.*").select(column_list)

                    update_df1_2.write.format("parquet").mode("append").save(write_path)

                    insert_df = src_df.filter(col("DELD_IN_SRC_IND") == 'N').join(target_df, join_keys_list, 'left_outer') \
                                      .withColumn("Insert_Flag", expr("case when target.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                                      .filter(col("Insert_Flag") == 'Y').select("incr.*").select(column_list)

                    insert_df.write.format("parquet").mode("append").save(write_path)

                    unchanged_df = target_df.join(src_df, join_keys_list, 'left_outer') \
                                            .withColumn("UnChanged_Flag", expr("case when incr.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                                            .filter(col("UnChanged_Flag") == 'Y').select("target.*").select(column_list)
                    unchanged_df.write.format("parquet").mode("append").save(write_path)
                else:
                    update_df = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("SCD_CUR_IND") == 'Y'), join_keys_list) \
                                      .select("upd.*", "target.ETL_CRT_DTM").select(column_list)

                    update_df.write.format("parquet").mode("overwrite").save(write_path)

                    update_df1_1 = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("SCD_CUR_IND") == 'N'), join_keys_list) \
                                         .withColumn("Update_Flag", expr("case when upd.SCD_CUR_IND = 'Y' then 'Y' else 'N' end")) \
                                         .filter("Update_Flag = 'Y'").select("upd.*", "target.ETL_CRT_DTM").select(column_list)

                    update_df1_1.write.format("parquet").mode("append").save(write_path)

                    update_df1_2 = src_df.drop('ETL_CRT_DTM').alias("upd").join(target_df.filter(col("SCD_CUR_IND") == 'N'), join_keys_list) \
                                         .withColumn("Update_Flag", expr("case when upd.SCD_CUR_IND = 'Y' then 'Y' else 'N' end")) \
                                         .filter("Update_Flag = 'N'").select(*join_keys_list, "target.*").select(column_list)

                    update_df1_2.write.format("parquet").mode("append").save(write_path)

                    insert_df = src_df.filter(col("SCD_CUR_IND") == 'Y').join(target_df, join_keys_list, 'left_outer') \
                                      .withColumn("Insert_Flag", expr("case when target.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                                      .filter(col("Insert_Flag") == 'Y').select("incr.*").select(column_list)

                    insert_df.write.format("parquet").mode("append").save(write_path)

                    unchanged_df = target_df.join(src_df, join_keys_list, 'left_outer') \
                                            .withColumn("UnChanged_Flag", expr("case when incr.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                                            .filter(col("UnChanged_Flag") == 'Y').select("target.*").select(column_list)

                    unchanged_df.write.format("parquet").mode("append").save(write_path)

                log.info("Merging Dataset")
                s3_file_copy(write_path + "|" + file_path_new)
                # if not status:
                #     raise Exception("Error during File Copy")
                log.info("Merge Completed")
            else:
                file_path = 's3://' + bucketname + '/' + file_path
                if 'DML_FLAG' in src_df.columns:
                    final_df = src_df.drop("DML_FLAG")
                else:
                    final_df = src_df
                log.info("Merging Dataset")
                final_df.write.format("parquet").mode("overwrite").save(file_path)
                log.info("Merge Completed")
        return True, None
    except:
        e = get_exception()
        return False, e



'''
Description : Function for copying files from one path to another path in s3 before removing the existing files
Input : Source Path , Target Path
Output : True(Files Copied)
'''
def s3_file_copy(locations):
    try:
        locations = locations.split("|")
        src_path = locations[0]
        log.info("Source Path : "+src_path)
        tgt_path = locations[1]
        log.info("Target Path : "+tgt_path)
        file_check_cmd = "aws s3 ls " + tgt_path + "|wc -l"
        flag = subprocess.check_output(file_check_cmd, shell=True)
        if flag == 0:
            log.info("File doesn't exist")
            s3_copy_command = "aws s3 sync " + src_path + " " + tgt_path + " --quiet --only-show-errors"
            os.system(s3_copy_command)
            log.info("Files are copied")
            status = True
        else:
            log.info("File exists, removing existing files")
            s3_rm_command = "aws s3 rm " + tgt_path + " --recursive --quiet --only-show-errors"
            os.system(s3_rm_command)
            s3_copy_command = "aws s3 sync " + src_path + " " + tgt_path + " --quiet --only-show-errors"
            os.system(s3_copy_command)
            log.info("Files are copied")
            status = True
        return True, None
    except:
        e = get_exception()
        return False, e


'''
Description : Function to Merge source data into multiple Target Partitions
Input : Source Dataframe, Target Dataframe, Target Path
Output : Spark Dataframe
'''
def merge_multiple_partitions(spark, src_df, target_base_path, **kwargs):
    try:
        if "delimiter" in kwargs:
            delimiter = kwargs.get('delimiter')
        else:
            delimiter = '|'
        if "join_keys" in kwargs:
            join_keys = kwargs.get("join_keys")
            log.info("Join Keys : "+str(join_keys))
        else:
            e = "Joining Key Columns Not Provided"
            raise Exception(e)
        if "partition_cols" in kwargs:
            partition_cols = kwargs.get("partition_cols")
            log.info("Partition Columns : "+str(partition_cols))
        else:
            e = "Partition By column  not provided"
            raise Exception(e)
      
        join_keys_list, error1 = get_join_key(join_keys, delimiter)
        partition_list, error2 = get_join_key(partition_cols, delimiter)

        check_file_path = '/'.join(target_base_path.split('/')[3:])
        s3 = boto3.client('s3', region_name='us-east-2')
        bucketname = ccs.translate_bucket_name
        response = s3.list_objects_v2(Bucket=bucketname, Prefix=check_file_path)
        if 'partn_scd_cur_ind'.upper() in partition_list:
            if "Contents" in response:
                partition_df = src_df.select(partition_list).distinct()
                partition_df = partition_df.filter("PARTN_SCD_CUR_IND = 'Y'")
                for partition in partition_list:
                    partition_df = partition_df.withColumn(partition,
                                                        concat(lit(partition), lit("='"), col(partition), lit("'")))
                if len(partition_list) != 1:
                    partition_df = partition_df.withColumn("Combination", concat_ws(' AND ', *partition_list))
                    partition_df = partition_df.withColumn("Combination", concat(lit("("), col("Combination"), lit(")")))
                else:
                    partition_df = partition_df.withColumn("Combination", concat(lit("("), col(*partition_list), lit(")")))
                    combination_list = [row["Combination"] for row in partition_df.collect()]
                    subset_query = " OR ".join(combination_list)

                df_N = src_df.filter("SCD_CUR_IND = 'N'")
                query = " select * from target_table where" + subset_query
                target_df = spark.read.parquet(target_base_path)
                target_df.registerTempTable("target_table")
    
                incremental_target_df = spark.sql(query).alias("target")
    
                columns_list = incremental_target_df.columns

                update_df = src_df.drop('ETL_CRT_DTM').alias("upd").join(incremental_target_df, join_keys_list) \
                    .select("upd.*", "target.ETL_CRT_DTM").select(columns_list)
                insert_df = src_df.alias("incr").join(incremental_target_df, join_keys_list, 'left_outer') \
                    .withColumn("Insert_Flag", expr("case when target.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                    .filter(col("Insert_Flag") == 'Y').select("incr.*").select(columns_list)
                unchanged_df = incremental_target_df.join(src_df.alias("incr"), join_keys_list, 'left_outer') \
                    .withColumn("UnChanged_Flag", expr("case when incr.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                    .filter(col("UnChanged_Flag") == 'Y').select("target.*").select(columns_list)
                final_df1 = update_df.unionAll(insert_df)
                final_df = final_df1.unionAll(unchanged_df)
                final_df = final_df.repartition(*partition_list)
                log.info("Merging into Target")
                final_df = final_df.filter("SCD_CUR_IND='Y'")
                final_df.write.partitionBy(*partition_list).format("parquet").mode("overwrite").save(target_base_path)
                df_N.repartition(*partition_list)
                df_N.write.partitionBy(*partition_list).format("parquet").mode("append").save(target_base_path)
                log.info("Merge Completed")
            else:
                df_Y = src_df.filter("SCD_CUR_IND = 'Y'")
                df_N = src_df.filter("SCD_CUR_IND = 'N'")
                if 'DML_FLAG' in src_df.columns:
                    final_df = df_Y.drop('DML_FLAG').repartition(*partition_list)
                    df_N.drop('DML_FLAG').repartition(*partition_list)
                else:
                    final_df = df_Y.repartition(*partition_list)
                    df_N.repartition(*partition_list)
    
                log.info("Merging into Target For First time")
                final_df.write.partitionBy(*partition_list).format("parquet").mode("overwrite").save(target_base_path)
                df_N.write.partitionBy(*partition_list).format("parquet").mode("append").save(target_base_path)
                log.info("Merge Completed")
            return final_df, None
        else:            
            if "Contents" in response:
                if  "partition_movement" in kwargs:
                    partition_movement=kwargs.get("partition_movement")
                    if partition_movement=='YES':    
                        delete_records_from_partitions(spark, src_df, target_base_path, **kwargs)
                partition_df = src_df.select(partition_list).distinct()
                for partition in partition_list:
                    partition_df = partition_df.withColumn(partition,
                                                        concat(lit(partition), lit("='"), col(partition), lit("'")))
                if len(partition_list) != 1:
                    partition_df = partition_df.withColumn("Combination", concat_ws(' AND ', *partition_list))
                    partition_df = partition_df.withColumn("Combination", concat(lit("("), col("Combination"), lit(")")))
                else:
                    partition_df = partition_df.withColumn("Combination", concat(lit("("), col(*partition_list), lit(")")))
                combination_list = [row["Combination"] for row in partition_df.collect()]
                subset_query = " OR ".join(combination_list)
                query = " select * from target_table where" + subset_query
                target_df = spark.read.parquet(target_base_path)
                target_df.registerTempTable("target_table")
    
                incremental_target_df = spark.sql(query).alias("target")
    
                columns_list = incremental_target_df.columns

                if ("deld_in_src_ind" in columns_list) or ("DELD_IN_SRC_IND" in columns_list):
                    update_df = src_df.drop('ETL_CRT_DTM').alias("upd").join(incremental_target_df.filter(col("DELD_IN_SRC_IND") == 'N'), join_keys_list) \
                                      .select("upd.*", "target.ETL_CRT_DTM").select(columns_list)
                    update_df1_1 = src_df.drop('ETL_CRT_DTM').alias("upd").join(incremental_target_df.filter(col("DELD_IN_SRC_IND") == 'Y'), join_keys_list) \
                                         .withColumn("Update_Flag", expr("case when upd.DELD_IN_SRC_IND = 'N' then 'Y' else 'N' end")) \
                                         .filter("Update_Flag = 'Y'").select("upd.*", "target.ETL_CRT_DTM").select(columns_list)
                    update_df1_2 = src_df.drop('ETL_CRT_DTM').alias("upd").join(incremental_target_df.filter(col("DELD_IN_SRC_IND") == 'Y'), join_keys_list) \
                                         .withColumn("Update_Flag", expr("case when upd.DELD_IN_SRC_IND = 'N' then 'Y' else 'N' end")) \
                                         .filter("Update_Flag = 'N'").select(*join_keys_list, "target.*").select(columns_list)
                    insert_df = src_df.filter(col("DELD_IN_SRC_IND") == 'N').alias("incr").join(incremental_target_df, join_keys_list, 'left_outer') \
                                      .withColumn("Insert_Flag", expr("case when target.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                                      .filter(col("Insert_Flag") == 'Y').select("incr.*").select(columns_list)
                    unchanged_df = incremental_target_df.join(src_df.alias("incr"), join_keys_list, 'left_outer') \
                                            .withColumn("UnChanged_Flag", expr("case when incr.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                                            .filter(col("UnChanged_Flag") == 'Y').select("target.*").select(columns_list)
                else:
                    update_df = src_df.drop('ETL_CRT_DTM').alias("upd").join(incremental_target_df.filter(col("SCD_CUR_IND") == 'Y'), join_keys_list) \
                                      .select("upd.*", "target.ETL_CRT_DTM").select(columns_list)


                    update_df1_1 = src_df.drop('ETL_CRT_DTM').alias("upd").join(incremental_target_df.filter(col("SCD_CUR_IND") == 'N'), join_keys_list) \
                                         .withColumn("Update_Flag",expr("case when upd.SCD_CUR_IND = 'Y' then 'Y' else 'N' end"))\
                                         .filter("Update_Flag = 'Y'").select("upd.*", "target.ETL_CRT_DTM").select(columns_list)

                    update_df1_2 = src_df.drop('ETL_CRT_DTM').alias("upd").join(incremental_target_df.filter(col("SCD_CUR_IND") == 'N'), join_keys_list) \
                                         .withColumn("Update_Flag",expr("case when upd.SCD_CUR_IND = 'Y' then 'Y' else 'N' end"))\
                                         .filter("Update_Flag = 'N'").select(*join_keys_list, "target.*").select(columns_list)

                    insert_df = src_df.filter(col("SCD_CUR_IND") == 'Y').alias("incr").join(incremental_target_df, join_keys_list, 'left_outer') \
                                      .withColumn("Insert_Flag", expr("case when target.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                                      .filter(col("Insert_Flag") == 'Y').select("incr.*").select(columns_list)

                    unchanged_df = incremental_target_df.join(src_df.alias("incr"), join_keys_list, 'left_outer') \
                                            .withColumn("UnChanged_Flag", expr("case when incr.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                                            .filter(col("UnChanged_Flag") == 'Y').select("target.*").select(columns_list)

                final_df1 = update_df.unionAll(insert_df)
                final_df1 = final_df1.unionAll(update_df1_1)
                final_df1 = final_df1.unionAll(update_df1_2)
                final_df = final_df1.unionAll(unchanged_df)

                final_df = final_df.repartition(*partition_list)
                log.info("Merging into Target")
                #print(final_df.show(truncate=False))
                final_df.write.partitionBy(*partition_list).format("parquet").mode("overwrite").save(target_base_path)
                log.info("Merge Completed")
            else:
                if 'DML_FLAG' in src_df.columns:
                    final_df = src_df.drop('DML_FLAG').repartition(*partition_list)
                else:
                    final_df = src_df.repartition(*partition_list)
                log.info("Merging into Target For First time")
                final_df.write.partitionBy(*partition_list).format("parquet").mode("overwrite").save(target_base_path)
                log.info("Merge Completed")
            return final_df, None
    except:
        e = get_exception()
        return None, e

'''
Description : Function for capturing cdc in Inactive Partition for SCD-Type 2 Tables
Input : Source Dataframe, Target Dataframe, Target Path
Output : True/None
'''
def capture_history_inactive(spark, job_param, source_df, current_df, expire_file_path, **kwargs):
    try:
        col_list = 'ETL_PRCS_RUN_ID|ETL_BTCH_NUM|ETL_JOB_NAME'
        col_value = 'jp_etl_prcs_run_id|jp_etl_btch_num|jp_etl_job_name'
        param_dict = {key.encode().decode('utf-8'): value.encode().decode('utf-8') for (key, value) in
                      job_param.items()}
        last_process_date = job_param['jp_EtlPrcsDt']

        if "delimiter" in kwargs:
            delimiter = kwargs.get("delimiter")
        else:
            delimiter = '|'
        if "key_cols" in kwargs:
            key_cols = kwargs.get("key_cols")
        else:
            e = "Key Columns Not Provided"

            raise Exception(e)
        if "scd_key" in kwargs:
            scd_key = kwargs.get("scd_key")
            param = 'TRUE'
            log.info("Using SCD HASH VAL for comparison")
        else:
            param = 'FALSE'
            log.info("SCD HASH VAL not used for comparison")
        if "partition_cols" in kwargs:
            partition_cols = kwargs.get("partition_cols")
            is_partitioned = 'TRUE'
            log.info("Partitioned Load")
        else:
            is_partitioned = 'FALSE'
            log.info("Non - Partitioned Load")

        join_keys_list, error1 = get_join_key(key_cols, delimiter)
        log.info(f'Join Keys Column List : {join_keys_list}')
        incr_df = source_df.filter(col('ETL_UPDT_DTM') > last_process_date).alias("incr")

        if incr_df.rdd.isEmpty() == False:
            if is_partitioned.upper() == 'TRUE':
                partition_list, error2 = get_join_key(partition_cols, delimiter)
                log.warn(f'Partition Column List : {partition_list}')
                partition_df = incr_df.select(partition_list).distinct()
                for partition in partition_list:
                    partition_df = partition_df.withColumn(partition,
                                                           concat(lit(partition), lit("='"), col(partition), lit("'")))
                if len(partition_list) != 1:
                    partition_df = partition_df.withColumn("Combination", concat_ws(' AND ', *partition_list))
                    partition_df = partition_df.withColumn("Combination",
                                                           concat(lit("("), col("Combination"), lit(")")))
                else:
                    partition_df = partition_df.withColumn("Combination",
                                                           concat(lit("("), col(*partition_list), lit(")")))
                combination_list = [row["Combination"] for row in partition_df.collect()]
                subset_query = " OR ".join(combination_list)
                query = " select * from target_table where" + subset_query

                current_df.registerTempTable("target_table")
                hist_curr_old = spark.sql(query).alias("hist")
            else:
                hist_curr_old = current_df.alias("hist")

            column_list = hist_curr_old.columns
            if param.upper() == 'TRUE':
                hist_expired_df = hist_curr_old.drop('SCD_CUR_IND', 'ETL_UPDT_DTM', 'SCD_EXPN_DTM').join(incr_df,
                                                                                                         join_keys_list) \
                    .withColumn('SCD_CHN_IND',
                                when(expr('incr.' + scd_key + ' != hist.' + scd_key), 'Y').otherwise('N')) \
                    .select("hist.*", "SCD_CHN_IND", "SCD_CUR_IND", "ETL_UPDT_DTM") \
                    .withColumn('Expire_Eligible',
                                when(((col('SCD_CHN_IND') == 'Y') | (col('SCD_CUR_IND') == 'N')), 'Y').otherwise('N')) \
                    .filter(col('Expire_Eligible') == 'Y').withColumn("SCD_EXPN_DTM", col('ETL_UPDT_DTM')) \
                    .withColumn("SCD_CUR_IND", lit('N')).select(column_list)
            else:
                hist_expired_df = hist_curr_old.drop('SCD_CUR_IND', 'ETL_UPDT_DTM', 'SCD_EXPN_DTM').join(incr_df,
                                                                                                         join_keys_list) \
                    .select("hist.*", "SCD_CUR_IND", "ETL_UPDT_DTM") \
                    .withColumn("SCD_EXPN_DTM", col('ETL_UPDT_DTM')) \
                    .withColumn("SCD_CUR_IND", lit('N')).select(column_list)

            hist_expired_df,error = add_audit_column(job_param,hist_expired_df,col_list,col_value,delimiter)
            hist_expired_df = hist_expired_df.withColumn("ETL_PRCS_RUN_ID", col('ETL_PRCS_RUN_ID').cast(DecimalType(38,0)))
            hist_expired_df = hist_expired_df.withColumn("ETL_BTCH_NUM", col('ETL_BTCH_NUM').cast(DecimalType(38,0)))
            

            if is_partitioned == 'TRUE':
                hist_expired_df = hist_expired_df.repartition(*partition_list)
                hist_expired_df.write.partitionBy(*partition_list).format("parquet").mode("append").save(expire_file_path)
            else:
                hist_expired_df.write.format("parquet").mode("append").save(expire_file_path)

            incr_df_N = incr_df.filter(col('SCD_CUR_IND') == 'N').alias("incr_N")
            

            insert_df_N = incr_df_N.join(hist_curr_old, join_keys_list, 'left_outer') \
                                   .withColumn("Expire_Flag", expr("case when hist.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                                   .filter(col("Expire_Flag") == 'Y').select("incr_N.*").select(column_list)


            insert_df_N,error = add_audit_column(job_param,insert_df_N,col_list,col_value,delimiter)
            insert_df_N = insert_df_N.withColumn("ETL_PRCS_RUN_ID", col('ETL_PRCS_RUN_ID').cast(DecimalType(38,0)))
            insert_df_N = insert_df_N.withColumn("ETL_BTCH_NUM", col('ETL_BTCH_NUM').cast(DecimalType(38,0)))
           

            if is_partitioned == 'TRUE':
                insert_df_N = insert_df_N.repartition(*partition_list)
                log.info("Writing Expired Records")
                insert_df_N.write.partitionBy(*partition_list).format("parquet").mode("append").save(
                    expire_file_path)
                log.info("Expired Written. ")
            else:
                log.info("Writing Expired Records")
                insert_df_N.write.format("parquet").mode("append").save(expire_file_path)
                log.info("Expired Written. ")

            return True, None
        else:
            return None, None
    except:
        e = get_exception()
        return None, e


'''
Description : Function for capturing cdc in Active Partition for SCD-Type 2 Tables
Input : Source Dataframe, Target Dataframe, Target Path
Output : True/None
'''
def capture_history_active(spark, job_param, source_df, current_df, active_file_path, **kwargs):
    try:
        col_list = 'ETL_PRCS_RUN_ID|ETL_BTCH_NUM|ETL_JOB_NAME'
        col_value = 'jp_etl_prcs_run_id|jp_etl_btch_num|jp_etl_job_name'
        param_dict = {key.encode().decode('utf-8'): value.encode().decode('utf-8') for (key, value) in
                      job_param.items()}
        last_process_date = param_dict['jp_EtlPrcsDt']

        if "delimiter" in kwargs:
            delimiter = kwargs.get("delimiter")
        else:
            delimiter = '|'
        if "key_cols" in kwargs:
            key_cols = kwargs.get("key_cols")
        else:
            e = "Key Columns Not Provided"

            raise Exception(e)
        if "scd_key" in kwargs:
            scd_key = kwargs.get("scd_key")
            param = 'TRUE'
            log.info("Using SCD HASH VAL for comparison")
        else:
            param = 'FALSE'
            log.info("SCD HASH VAL not used for comparison")
        if "partition_cols" in kwargs:
            partition_cols = kwargs.get("partition_cols")
            is_partitioned = 'TRUE'
            log.info("Partitioned Load")
        else:
            is_partitioned = 'FALSE'
            log.info("Non-Partitioned Load")

        join_keys_list, error1 = get_join_key(key_cols, delimiter)
        log.info(f'Join Keys Column List : {join_keys_list}')

        incr_df = source_df.filter(col('ETL_UPDT_DTM') > last_process_date).alias("incr")
        if incr_df.rdd.isEmpty() == False:
            if is_partitioned == 'TRUE':
                partition_list, error2 = get_join_key(partition_cols, delimiter)
                log.info(f'Partition Column List : {partition_list}')
                partition_df = incr_df.select(partition_list).distinct()
                for partition in partition_list:
                    partition_df = partition_df.withColumn(partition,
                                                           concat(lit(partition), lit("='"), col(partition), lit("'")))
                if len(partition_list) != 1:
                    partition_df = partition_df.withColumn("Combination", concat_ws(' AND ', *partition_list))
                    partition_df = partition_df.withColumn("Combination",
                                                           concat(lit("("), col("Combination"), lit(")")))
                else:
                    partition_df = partition_df.withColumn("Combination",
                                                           concat(lit("("), col(*partition_list), lit(")")))
                combination_list = [row["Combination"] for row in partition_df.collect()]
                subset_query = " OR ".join(combination_list)
                query = " select * from target_table where" + subset_query

                current_df.registerTempTable("target_table")

                hist_curr_old = spark.sql(query).alias("hist")
            else:
                hist_curr_old = current_df.alias("hist")
            column_list = hist_curr_old.columns
            if param.upper() == 'TRUE':
                update_df = incr_df.drop('ETL_CRT_DTM', 'SCD_EFF_DTM').join(hist_curr_old, join_keys_list) \
                    .withColumn('SCD_CHN_IND',
                                when(expr('incr.' + scd_key + ' != hist.' + scd_key), 'Y').otherwise('N')) \
                    .select("incr.*", "SCD_CHN_IND", "ETL_CRT_DTM", "SCD_EFF_DTM") \
                    .withColumn('ETL_CRT_DTM',
                                when((col('SCD_CHN_IND') == 'Y'), col('ETL_UPDT_DTM')).otherwise(col('ETL_CRT_DTM'))) \
                    .withColumn('SCD_EFF_DTM',
                                when((col('SCD_CHN_IND') == 'Y'), col('ETL_UPDT_DTM')).otherwise(col('SCD_EFF_DTM'))) \
                    .filter(col('SCD_CUR_IND') == 'Y').select(column_list)
            else:
                update_df = incr_df.drop('ETL_CRT_DTM', 'SCD_EFF_DTM').join(hist_curr_old, join_keys_list) \
                    .select("incr.*", "ETL_CRT_DTM", "SCD_EFF_DTM") \
                    .withColumn('ETL_CRT_DTM', col('ETL_UPDT_DTM')) \
                    .withColumn('SCD_EFF_DTM', col('ETL_UPDT_DTM')) \
                    .filter(col('SCD_CUR_IND') == 'Y').select(column_list)

            update_df,error = add_audit_column(job_param,update_df,col_list,col_value,delimiter)
            update_df = update_df.withColumn("ETL_PRCS_RUN_ID", col('ETL_PRCS_RUN_ID').cast(DecimalType(38,0)))
            update_df = update_df.withColumn("ETL_BTCH_NUM", col('ETL_BTCH_NUM').cast(DecimalType(38,0)))

            temp_write_path = '/'.join(active_file_path.split('/')[0:-4])+'/temp/'+'/'.join(active_file_path.split('/')[-4:-1])+'/'
            update_df.write.format("parquet").mode("overwrite").save(temp_write_path)


            unchanged_df = hist_curr_old.join(incr_df, join_keys_list, 'left_outer') \
                                        .withColumn("UnChanged_Flag", expr("case when incr.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                                        .filter(col("UnChanged_Flag") == 'Y').select("hist.*").select(column_list)

            unchanged_df.write.format("parquet").mode("append").save(temp_write_path)
            
            incr_df = incr_df.filter(col('SCD_CUR_IND') == 'Y').alias("incr")
            insert_df = incr_df.join(hist_curr_old, join_keys_list, 'left_outer') \
                .withColumn("Insert_Flag", expr("case when hist.ETL_CRT_DTM is null then 'Y' else 'N' end")) \
                .filter(col("Insert_Flag") == 'Y').select("incr.*").select(column_list)

            insert_df,error = add_audit_column(job_param,insert_df,col_list,col_value,delimiter)
            insert_df = insert_df.withColumn("ETL_PRCS_RUN_ID", col('ETL_PRCS_RUN_ID').cast(DecimalType(38,0)))
            insert_df = insert_df.withColumn("ETL_BTCH_NUM", col('ETL_BTCH_NUM').cast(DecimalType(38,0)))

            insert_df.write.format("parquet").mode("append").save(temp_write_path)


            if is_partitioned == 'TRUE':
                hist_active_df = spark.read.parquet(temp_write_path)
                hist_active_df = hist_active_df.repartition(*partition_list)
                log.info("Writing Active Records")

                hist_active_df.write.partitionBy(*partition_list).format("parquet").mode("overwrite").save(active_file_path)
                log.info("Active Written. ")
            else:
                log.info("Writing Active Records")
                s3_file_copy(temp_write_path + "|" + active_file_path)
                log.info("Active Written. ")
            return True, None
        else:
            return None, None
    except:
        e = get_exception()
        return None, e

'''
Description : Function to  copy incremental/all files from Source to Target
Input : Source Path, Target Path,File Format
Output : Files to be Copied/Archived
'''
def copy_files(param_dict, source_path, target_path, file_format, **kwargs):
    try:
        param_dict = {key.encode().decode('utf-8'): value.encode().decode('utf-8') for (key, value) in
                      param_dict.items()}
        cdc_lower_dtm = param_dict['jp_CDCLowDtm']
        cdc_lower_dtm = cdc_lower_dtm.split()
        cdc_lower_dtm = cdc_lower_dtm[0] + 'T' + cdc_lower_dtm[1]
        cdc_upper_dtm = param_dict['jp_CDCUprDtm']
        cdc_upper_dtm = cdc_upper_dtm.split()
        cdc_upper_dtm = cdc_upper_dtm[0] + 'T' + cdc_upper_dtm[1]
        log.info("lower dtm :"+str(cdc_lower_dtm))
        log.info("upper dtm :"+str(cdc_upper_dtm))
        bucket_name = source_path.split('/')[2]
        log.info("bucket name :"+str(bucket_name))
        folder_name = '/'.join(source_path.split('/')[3:-1]) + '/'
        log.info("Prefix Folder :"+folder_name)
        files_list = []
        if "fetch_type" in kwargs:
            fetch_type = kwargs.get("fetch_type")
        else:
            fetch_type = "INCREMENTAL"
        if fetch_type.upper() == 'INCREMENTAL':
            command = "aws s3api list-objects-v2 --bucket " + bucket_name + " --prefix " + folder_name + " --query 'Contents[?(LastModified > `" + cdc_lower_dtm + "` && LastModified < `" + cdc_upper_dtm + "`)].[Key]' --output text"
        else:
            command = "aws s3api list-objects-v2 --bucket " + bucket_name + " --prefix " + folder_name + " --query 'Contents[].[Key]' --output text"
        modified_files = subprocess.getoutput(command)
        modified_files_list = modified_files.split('\n')
        for file in modified_files_list:
            if '.' + file_format in file:
                files_list.append(file)
        if len(files_list) != 0:
            for file in files_list:
                s3_copy_command = "aws s3 cp s3://" + bucket_name + "/" + file + " " + target_path
                os.system(s3_copy_command)
            log.info("Files Copied")
        else:
            log.warn("No Files to Copy")
        return files_list, None
    except:
        e = get_exception()
        return None, e



'''
Description : Function to  archive incremental/all files from Source to Target
Input : Source Path, Target Path
Output : True(Files Archived)
'''
def archive_files(files_list, source_path, target_path, **kwargs):
    try:
        bucket_name = ccs.bucket_name
        
        archive_type = kwargs.get('archive_type','INCREMENTAL').upper()

        if archive_type == 'FULL':
            files_list =[]
            file_format = kwargs.get('file_format','').upper()
            bucket_name = source_path.split('/')[2]
            folder_name = '/'.join(source_path.split('/')[3:])
            command = f"aws s3api list-objects-v2 --bucket {bucket_name} --prefix {folder_name} --query 'Contents[].[Key]' --output text"
            #log.info(command)
            modified_files = subprocess.getoutput(command)
            modified_files_list = modified_files.split('\n')
            #print(modified_files_list)
            for file in modified_files_list:
                if '.'+file_format in file.upper():
                    files_list.append(file)

        ct_dt = dt.datetime.now()
        ct_str = ct_dt.strftime('%Y%m%d%H%M%S')
        if len(files_list) != 0:
            for file in files_list:
                s3_move_command = "aws s3 mv s3://" + bucket_name + "/" + file + " " + target_path + ct_str +"/"
                os.system(s3_move_command)
            log.info("Files Archived")
        else:
            log.info("No Files to Archive")
        return True, None
    except:
        e = get_exception()
        return False, e



'''
Description : Function to read data from Glue table for JDE sources in Spark Dataframe
Input : Table Name 
Output : Spark Dataframe
15-Feb-2021: Added functionality to read partitioned glue table using two new op tags - partn_col and yr_diff_list
'''
def read_glue(spark, param_dict, **kwargs):
    max_tries=3
    counter=1
    for i in range(max_tries):
        try:
            param_dict = {key.encode().decode('utf-8'): value.encode().decode('utf-8') for (key, value) in
                          param_dict.items()}
            cdc_lower_dtm = param_dict['jp_CDCLowDtm']
            cdc_upper_dtm = param_dict['jp_CDCUprDtm']
            load_type = param_dict['jp_LoadType']
            db_name = param_dict['jp_DBName']
            log.info("Load Type :"+str(load_type))
            if 'jp_TableName' in param_dict:
                table_name = param_dict['jp_TableName']
            elif 'table_name' in kwargs:
                table_name = kwargs.get('table_name')

            result_df = None        
            if load_type.upper() == 'FULL':
                log.info("CDC Upper DTM :"+cdc_upper_dtm)
                query = "select * from {}.{} where _hoodie_commit_time <= date_format('{}','yyyyMMddHHmmss')".format(
                    db_name, table_name, cdc_upper_dtm)
                log.info("Query :"+str(query))
                result_df = spark.sql(query)
                log.info("COUNT : "+str(result_df.count()))

            elif load_type.upper() == 'INCREMENTAL':
                log.info("CDC Lower DTM :"+str(cdc_lower_dtm))
                log.info("CDC Upper DTM :"+str(cdc_upper_dtm))
                query = "select * from {}.{} where (_hoodie_commit_time >= date_format('{}','yyyyMMddHHmmss') and _hoodie_commit_time <= date_format('{}','yyyyMMddHHmmss'))".format(
                    db_name, table_name, cdc_lower_dtm, cdc_upper_dtm)
                log.info("Query :"+query)
                result_df = spark.sql(query)
                log.info("COUNT : "+str(result_df.count()))

            elif load_type.upper() == 'ADHOC':
                cdc_lower_dtm_adhoc = param_dict['jp_CDCLowDtmAdhoc']
                log.info("CDC Lower DTM for Adhoc:",str(cdc_lower_dtm_adhoc))
                query = "select * from {}.{} where (_hoodie_commit_time >= date_format('{}','yyyyMMddHHmmss') and _hoodie_commit_time <= date_format('{}','yyyyMMddHHmmss'))".format(
                    db_name, table_name, cdc_lower_dtm_adhoc, cdc_upper_dtm)
                log.info("Query :"+query)
                result_df = spark.sql(query)
                log.info("COUNT : "+str(result_df.count()))
 
            elif load_type.upper() == 'PARTITIONED':
                if 'partn_col' in kwargs:
                    partn_col = kwargs.get('partn_col')
                if 'yr_diff_list' in kwargs:
                    yr_diff_list = kwargs.get('yr_diff_list')
                
                if 'lookup_keys' in kwargs:
                    lookup_keys = kwargs.get('lookup_keys')
                    lookup_keys = lookup_keys.split("|")
                    for item in lookup_keys:
                        if item in param_dict:
                            yr_diff_list = yr_diff_list.replace(item, param_dict[item])
                
                log.info("CDC Lower DTM :"+str(cdc_lower_dtm))
                log.info("CDC Upper DTM :"+str(cdc_upper_dtm))

                now = int(datetime.now().strftime("%Y"))
                yr_limit = 6
                
                ls=[
                    "'"+str(i)[:2]+".0000000000_"+str(i)[2:]+".0000000000"+"'" 
                    for i in range(now,now-yr_limit,-1)
                    ]

                inpt_ls=yr_diff_list.split('|')
                
                filter_ls=','.join(ls[(int(i))] for i in inpt_ls)
                
                query = "select * from {}.{} where {} in ({}) and (_hoodie_commit_time >= date_format('{}','yyyyMMddHHmmss') and _hoodie_commit_time <= date_format('{}','yyyyMMddHHmmss'))".format(
                    db_name, table_name, partn_col, filter_ls,cdc_lower_dtm,cdc_upper_dtm)
                log.info("Query :"+query)
                result_df = spark.sql(query)
                log.info("COUNT : "+str(result_df.count()))
            else:
                e = 'INVALID LOAD TYPE'
                raise Exception(e)
            return result_df, None
        except:
            if i>1:
                e = get_exception()
                return None, e
            else:
                time.sleep(15)
                log.info("Got exception in read_glue, performing retry count : - "+ str(counter))
                counter=counter+1
                continue


'''
Description : Function to filter a Spark Dataframe based on CDC_LOWER_DTM and CDC_UPPER_DTM
Input : Spark Dataframe 
Output : Spark Dataframe
'''

def filter_df(df, etl_prcs_id):
    try:
        con = get_rds_con(ccs)
        sql = "select max(strt_dtm) from etl_prcs_run epr where etl_prcs_id =" + str(etl_prcs_id) + " AND run_sts_cd=1";
        now = datetime.now()
        upperBound = now.strftime("%Y-%m-%d %H:%M:%S")
        with con.cursor() as cursor:
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) != 1:
                e = 'Process does not exist or More than one entry with same process name'
                raise Exception(e)
            else:
                lowerBound = str(res_set[0][0])
        close_rds_con(con)
        filter_cond = "ETL_UPDT_DTM>='" + str(lowerBound) + "' AND ETL_UPDT_DTM<='" + str(upperBound) + "'"
        df = df.filter(filter_cond)
        return df
    except:
        e = get_exception()
        return None


'''
Description : Function to de-duplicate records in Spark Dataframe
Input : Spark Dataframe, Key Columns, Order By Columns 
Output : Spark Dataframe
'''

def dedup_dataset(df, **kwargs):
    try:
        if 'delimiter' in kwargs:
            delimiter = kwargs.get('delimiter')
        else:
            delimiter = '|'
        if 'orderby_type' in kwargs:
            orderby_type = kwargs.get('orderby_type')
        else:
            orderby_type = 'DESC'
        key_cols = kwargs.get('key_cols')
        orderby_cols = kwargs.get('orderby_cols')
        key_cols = key_cols.split(delimiter)
        orderby_cols = orderby_cols.split(delimiter)
        if orderby_type.upper() == 'DESC':
            df = df.withColumn('row_num',row_number().over(Window.partitionBy(key_cols).orderBy([desc(i) for i in orderby_cols]))).filter("row_num == 1")
        else:
            df = df.withColumn('row_num',row_number().over(Window.partitionBy(key_cols).orderBy(orderby_cols))).filter("row_num == 1")
        df = df.drop("row_num")
        return df, None
    except:
        e = get_exception()
        return None, e


'''
Description : Function to generate surrogate key by concating multiple columns with a separator
Input : delimiter,column list,new column name, separator
Output : Spark Dataframe
'''
def add_sk_curate(spark, df, **kwargs):
    try:
        table_name = 'temp_table'
        delimiter = cc.input_column_delimiter
        if "delimiter" in kwargs:
            delimiter = kwargs.get("delimiter")
        if "id_columns" in kwargs:
            id_columns = kwargs.get("id_columns").strip()
        else:
            e = "INVALID_PARAMS", " <id_columns> have not been provided"
            raise Exception(e)
            return None, e
        if "column_name" in kwargs:
            column_name = kwargs.get("column_name").strip()
        else:
            e = "INVALID_PARAMS", " <column_name> have not been provided"
            raise Exception(e)
            return None, e
        if 'separator' in kwargs:
            separator = kwargs.get('separator')
            len_sep = len(separator)
        else:
            e = "INVALID_PARAMS", " <separator> have not been provided"
            raise Exception(e)
            return None, e

        cols = id_columns.split(delimiter)
        expr = "concat("
        for col in cols:
            expr = expr + "t." + col + ",'" + separator + "',"
        expr = expr[0:(len(expr) - (4 + len_sep))]
        expr = expr + ")"
        df.createOrReplaceTempView(table_name)

        eval_expr = ("select t.*,{} as {} from {} t".format(expr, column_name, table_name))

        df = spark.sql(eval_expr)
        return df, None
    except:
        e = get_exception()
        return None, e


'''
Description : Function to read files (csv,parquet,excel,txt or any other format) in Spark Dataframe
Input : File Path,Column Delimiter,Encoding,Schema File Path
Output : Spark Dataframe
16-Feb-2021: Added retry utility in case read file operation aborted as source job is writting the files in the same path at same time. After few second of wait read_file operation will get re-trigger and try to perform the same activity again
'''
def read_file(spark, **kwargs):
        max_tries=3
        for i in range(max_tries):
            try:
                file_path = kwargs.get('file_path')
                file_format = kwargs.get('file_format')
                result_dataframe_name = kwargs.get('result_dataframe')
                result_df = None
                is_multiline = True
                has_header = True
                column_delimiter = ','
                quote_char = '"'
                escape_char = '\\'
                schema_file = ''

                if "schema_file" in kwargs:
                    schema_file = kwargs.get("schema_file")

                if "pre_process" in kwargs:
                    pre_process = kwargs.get('pre_process')
                else:
                    pre_process = 'FALSE'
                if "key_cols" in kwargs:
                    key_cols = kwargs.get('key_cols')
                else:
                    key_cols = None
                if "has_header" in kwargs:
                    hh = kwargs.get('has_header')
                    if hh.strip().upper() == 'FALSE':
                        has_header = False
                else:
                    has_header = True
                if "schema_file" in kwargs:
                    sfp = kwargs.get('schema_file')
                    if len(sfp.strip()) > 0:
                        schema_file = sfp
                if "is_multiline" in kwargs:
                    im = kwargs.get("is_multiline")
                    im_key = im.strip().upper()
                    if im_key == 'FALSE':
                        is_multiline = False
                if "column_delimiter" in kwargs:
                    cd = kwargs.get("column_delimiter")
                    if cd and cd.strip() != '':
                        column_delimiter = cd
                if "quote_char" in kwargs:
                    qc = kwargs.get('quote_char')
                if "escape_char" in kwargs:
                    ec = kwargs.get('escape_char')
                    if ec and ec.strip() != '':
                        escape_char = ec
                if "encoding" in kwargs:
                    enc = kwargs.get('encoding')
                else:
                    enc = 'UTF-8'
                if "replace_header" in kwargs:
                    replace_header = kwargs.get("replace_header")
                else:
                    replace_header = 'FALSE'

                if "is_compressed" in kwargs:
                    is_compressed = kwargs.get("is_compressed")
                else:
                    is_compressed = 'FALSE'

                if is_compressed.upper() == 'TRUE':
                    if has_header == False:
                        schema, error = create_header(spark, schema_file)
                        result_df_temp = spark.read.option("header",False).schema(schema).csv(file_path)
                        result_df = change_schema(spark, result_df_temp, schema_file)
                        result_df.alias(result_dataframe_name)
                    else:
                        if replace_header == 'FALSE':
                            result_df = spark.read.option("header",True).csv(file_path).alias(result_dataframe_name)
                        else:
                            schema, error = create_header(spark, schema_file)
                            result_df_temp = spark.read.option("header",True).schema(schema).csv(file_path)
                            result_df = change_schema(spark, result_df_temp, schema_file)
                            result_df.alias(result_dataframe_name)
                    #print(result_df.printSchema())
                    return result_df, None, None


                if file_format == 'csv' and len(column_delimiter) == 1:
                    validate,error = validate_file(spark,file_path,file_format,schema_file,column_delimiter)
                    if error:
                        return None,error
                    else:
                        log.info ("File Validation is Successfull")

                if pre_process.strip().upper() == 'TRUE':
                    result_df, error = read_unicode_csv(spark, **kwargs)
                    if error:
                        return None, None, error
                    else:
                        return result_df, None, None
                if key_cols:
                    result_df, error = read_non_jde(spark, **kwargs)
                    if error:
                        return None, None, error
                    else:
                        return result_df, None, None
                if file_format == 'csv':
                    if has_header == False:
                        schema, error = create_header(spark, schema_file)
                        if error:
                            return None, None, error
                        result_df_temp = spark.read.format("csv").option(
                            "multiline", is_multiline).option(
                            "delimiter", column_delimiter).option(
                            'quote', quote_char).option(
                            'escape', escape_char).option(
                            "header", False).option("encoding", enc).schema(schema).load(file_path)
                        result_df = change_schema(spark, result_df_temp, schema_file)
                        result_df.alias(result_dataframe_name)
                    else:
                        if replace_header == 'FALSE':
                            result_df = spark.read.format("csv").option(
                                "multiline", is_multiline).option(
                                "delimiter", column_delimiter).option(
                                'quote', quote_char).option(
                                'escape', escape_char).option("header", True).option("encoding", enc).load(file_path).alias(
                                result_dataframe_name)
                        else:
                            schema, error = create_header(spark, schema_file)
                            if error:
                                return None, None, error
                            result_df_temp = spark.read.format("csv").option("multiline", is_multiline).option("delimiter",
                                                                                                            column_delimiter).option(
                                'quote', quote_char).option('escape', escape_char).option("header", True).option("encoding",
                                                                                                                enc).schema(
                                schema).load(file_path)
                            result_df = change_schema(spark, result_df_temp, schema_file)
                            result_df.alias(result_dataframe_name)
                        if result_df.columns[-1].endswith('\r'):
                            result_df = result_df.withColumnRenamed(result_df.columns[-1], result_df.columns[-1].strip('\r'))
                elif file_format == 'parquet':
                    result_df = spark.read.parquet(file_path).alias(result_dataframe_name)
                elif file_format == 'text':
                    result_df = spark.read.format(file_format).option("encoding", enc).load(file_path).alias(
                        result_dataframe_name)
                elif file_format == 'xlsx':
                    data_address = 'A1'
                    if "data_address" in kwargs:
                        da = kwargs.get('data_address')
                        if da and da.strip() != '':
                            data_address = da
                    header_val = 0
                    if "has_header" in kwargs:
                        hh = kwargs.get('has_header')
                        if hh and hh.strip().upper() == 'FALSE':
                            has_header = False
                        if not has_header:
                            header_val = None
                    files = get_file_list_excel(spark, file_path)
                    for f in files:
                        if result_df:
                            df = pandas.read_excel(f, sheet_name=data_address, dtype=str, header=header_val)
                            df = df.replace({np.nan: None})
                            i_df = spark.createDataFrame(df)
                            result_df = result_df.union(i_df)
                        else:
                            df = pandas.read_excel(f, sheet_name=data_address, dtype=str, header=header_val)
                            df = df.replace({np.nan: None})
                            result_df = spark.createDataFrame(df)
                else:
                    result_df = spark.read.format(file_format).load(file_path).alias(result_dataframe_name)
                log.info("Count from read file - " + str(result_df.count()))
                return result_df, None, None
            except:
                if i>1:
                    e = get_exception()
                    return None,None, e
                else:
                    log.info("Got exception in read_file operation, retry read_file operation")
                    log.info("In waiting state for 30 seconds")                    
                    time.sleep(30)
                    continue

'''
Description : Function to read Non-JDE files in sequence
Input : File Path,Column Delimiter, Encoding,Key Columns,Schema File Path
Output : Spark Dataframe
'''
def read_non_jde(spark, **kwargs):
    try:
        s3 = boto3.client('s3')
        file_path = kwargs.get('file_path')
        bucket_list = file_path.split("/")
        bucket_name = bucket_list[2]
        prefix = '/'.join(file_path.split('/')[3:])
        objects = s3.list_objects(Bucket=bucket_name, Prefix=prefix)
        na_values_considered = "‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’, ‘<NA>’, ‘NULL’, ‘NaN’, ‘nan’, ‘null’"
        if "column_delimiter" in kwargs:
            column_delimiter = kwargs.get('column_delimiter').strip()
            col_len = len(column_delimiter)
            if col_len==1:
                separator = column_delimiter
            else:
                separator = '\\' + ('\\'.join(list(column_delimiter)))
        else:
            column_delimiter = ","
            separator = ","
        if "encoding" in kwargs:
            encoding = kwargs.get('encoding').strip().upper()
        else:
            encoding = 'UTF-8'
        if 'key_cols' in kwargs:
            key_cols = kwargs.get('key_cols')
        else:
            e = "Key Column(S) should be passed"
            raise Exception (e)
        if "multiquote" in kwargs:
            flag_multiquote = kwargs.get('multiquote').strip().upper()
        else:
            flag_multiquote="FALSE"
        if "has_header" in kwargs:
            has_header = kwargs.get('has_header')
            if has_header.strip().upper() == 'FALSE':
                has_header = False
        else:
            has_header = True
        if "schema_file" in kwargs:
            schema_file = kwargs.get("schema_file")
        if "replace_header" in kwargs:
            replace_header = kwargs.get("replace_header")
        else:
            replace_header = 'FALSE'        
        schema, error = create_header(spark, schema_file)
        if error:
            return None, error
        files_list = []
        for obj in objects.get('Contents'):
            if not obj.get('Key').endswith("/"):
                size_of_file = int(float(obj['Size'])/1000)
                if size_of_file==0:
                    continue
                else:
                    files_list.append(obj.get('Key'))
        if len(files_list)==0:
            result_df = spark.createDataFrame([],schema=schema)
            return result_df,None
        dictstr = "{"
        for obj in objects.get('Contents'):
            if ".csv" in obj.get('Key'):
                if obj.get('Key') in files_list:
                    dictstr = dictstr +"'"+str(obj.get('Key'))+ "':'"+str(obj.get('LastModified')) +"',"
        dictstr = dictstr[0:(len(dictstr) - 1)]
        dictstr = dictstr + "}"
        dictKey = eval(dictstr)
        odKey = OrderedDict(sorted(dictKey.items()))
        odKey = dict(odKey)
        dfs = []
        i = 1
        print(odKey)
        for value in odKey.keys():
            new_path = "s3://" + bucket_name + "/" + value
            if not has_header:
                schema, error = create_header(spark, schema_file)
                if error:
                    return None, error
                if flag_multiquote=="TRUE":
                    tempDf = pandas.read_csv(new_path, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)
                    dequote=lambda x: x[1:-1] if x.startswith('"') and x.endswith('"') else x
                    tempDf = tempDf.applymap(dequote)
                    tempDf = tempDf.replace('""','"',regex=True)
                else:
                    tempDf = pandas.read_csv(new_path, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)
                #trim_strings = lambda x: x.strip() if isinstance(x, str) else x
                #tempDf = tempDf.applymap(trim_strings)
                tempDf = tempDf.replace({np.nan: None})
                result_temp_df = spark.createDataFrame(tempDf, schema=schema)
                if i ==1:
                    result_df = change_schema(spark, result_temp_df, schema_file)
                    result_df = result_df.withColumn("rank", lit(i).cast(LongType()))
                else:
                    temp_df = change_schema(spark, result_temp_df, schema_file)
                    temp_df = temp_df.withColumn("rank", lit(i).cast(LongType()))
                    result_df = result_df.union(temp_df)
            else:
                if replace_header == 'FALSE':
                    schema, error = create_header(spark, schema_file)
                    if error:
                        return None, error
                    if flag_multiquote=="TRUE":
                        df = pandas.read_csv(new_path, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)
                        dequote=lambda x: x[1:-1] if x.startswith('"') and x.endswith('"') else x
                        df = df.applymap(dequote)
                        df = df.replace('""','"',regex=True)
                    else:
                        df = pandas.read_csv(new_path, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)
                    df = df.replace({np.nan: None})
                    result_temp_df = spark.createDataFrame(df.astype(str))
                    if i==1:
                        result_df = result_temp_df
                        result_df = result_df.withColumn("rank", lit(i).cast(LongType()))
                    else:
                        temp_df = result_temp_df
                        temp_df = temp_df.withColumn("rank", lit(i).cast(LongType()))
                        result_df = result_df.union(temp_df)
                else:
                    schema, error = create_header(spark, schema_file)
                    if error:
                        return None, error
                    if flag_multiquote=="TRUE":
                        df = pandas.read_csv(new_path, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)
                        dequote=lambda x: x[1:-1] if x.startswith('"') and x.endswith('"') else x
                        df = df.applymap(dequote)
                        df = df.replace('""','"',regex=True)
                    else:
                        df = pandas.read_csv(new_path, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)                    
                    df = df.replace({np.nan: None})
                    print(df)
                    result_temp_df = spark.createDataFrame(df, schema=schema)
                    if i ==1:
                        result_df = change_schema(spark, result_temp_df, schema_file)
                        result_df = result_df.withColumn("rank", lit(i).cast(LongType()))
                    else:
                        temp_df = change_schema(spark, result_temp_df, schema_file)
                        temp_df = temp_df.withColumn("rank", lit(i).cast(LongType()))
                        result_df = result_df.union(temp_df)                  
                i = i + 1
        key_cols = key_cols.split("|")
        result_df = result_df.withColumn('dense_rank',dense_rank().over(Window.partitionBy(key_cols).orderBy(col("rank").desc()))).filter(
                    col("dense_rank") == 1)
        print(result_df.select("dense_rank","rank").show())
        result_df = result_df.drop("dense_rank","rank")
        return result_df, None
    except:
        e = get_exception()

'''
Description : Function To read excel file in Spark Dataframe
Input : File Path
'''
def get_file_list_excel(spark, path):
    res = []
    sc = spark.sparkContext
    URI = sc._jvm.java.net.URI
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._jvm.org.apache.hadoop.conf.Configuration
    c = Configuration()
    u = URI(path)
    p = Path(u)
    fs = FileSystem.get(u, c)
    l = fs.listFiles(p, False)
    while l.hasNext():
        l1 = l.next()
        p = l1.getPath()
        res.append(p.toString())
    return res

'''
Description : Mapping for DataType and Column Type passed in Schema File
'''
def get_type_name(s: str) -> str:
    d_map = {
        'VARCHAR': StringType(),
        'CHAR': StringType(),
        'NCHAR': StringType(),
        'LONG': DoubleType(),
        'INT': DoubleType(),
        'INTEGER': DoubleType(),
        'BIGINT': DoubleType(),

        'FLOAT': FloatType(),
        'DOUBLE': DoubleType(),
        'DECIMAL': DecimalType(),
        'DATE': DateType(),
        'TIMESTAMP': TimestampType(),
        'BOOLEAN': BooleanType(),
    }
    return d_map.get(s)


'''
Description : Function to create customer Header of a CSV File
Input : Schema File Path
Output : Spark Schema
'''
def create_header(spark, schema_file):
    try:
        headerFileDF = spark.read.format("csv").option("delimiter", ',').option('quote', '"').option('escape',
                                                                                                     '\\').option(
            "header", "TRUE").load(schema_file)
        field_list = (headerFileDF.select(headerFileDF.src_column_name)).collect()
        fieldnameList = []
        for rows in field_list:
            fieldnameList.append(str(rows[0]))
        field_lst = []
        for cols in fieldnameList:
            field_lst.append({'metadata': {}, 'name': cols, 'nullable': True, 'type': 'string'})
        schema_dict = {'fields': field_lst, 'type': 'struct'}
        return StructType.fromJson(schema_dict), None
    except:
        e = get_exception()
        return None, e


'''
Description : Function To change dataTypes of columns in spark dataframe
Input : Spark Dataframe,Schema File Path
Output : Spark Dataframe
'''

def change_schema(spark, inp_df, schema_file):
    SchemaFileDF = spark.read.format("csv").option("delimiter", ',').option('quote', '"').option('escape', '\\').option(
        "header", "TRUE").load(schema_file)
    field_list = (SchemaFileDF.select(SchemaFileDF.src_column_name)).collect()
    data_type = (SchemaFileDF.select(SchemaFileDF.data_type)).collect()

    fieldnameList = []
    dataTypeList = []

    for rows, types in zip(field_list, data_type):
        fieldnameList.append(str(rows[0]))
        dataTypeList.append(str(types[0]))

    for cols, types in zip(fieldnameList, dataTypeList):
        inp_df = inp_df.withColumn(cols, col(cols).cast(get_type_name(types)))
    return inp_df

'''
Description : Function to read CSV files with multi-character delimiter, multi-line and Non UTF-8 characters
Input : File Path, Delimiter, Schema File Path, Enconding
Output : Spark Dataframe
'''
def read_unicode_csv(spark, **kwargs):
    try:
        s3 = boto3.client('s3')
        bucket_name = ccs.bucket_name
        file_path = kwargs.get('file_path')
        prefix = '/'.join(file_path.split('/')[3:])
        objects = s3.list_objects(Bucket=bucket_name, Prefix=prefix)
        s3_object_list = []
        csv_list = []
        na_values_considered = "‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’, ‘<NA>’, ‘NULL’, ‘NaN’, ‘nan’, ‘null’"
        if "column_delimiter" in kwargs:
            column_delimiter = kwargs.get('column_delimiter').strip()
            col_len = len(column_delimiter)
            if col_len==1:
                separator = column_delimiter
            else:
                separator = '\\' + ('\\'.join(list(column_delimiter)))
        else:
            column_delimiter = ","
            separator = ","
        if "encoding" in kwargs:
            encoding = kwargs.get('encoding').strip().upper()
        else:
            encoding = 'UTF-8'
        if "multiquote" in kwargs:
            flag_multiquote = kwargs.get('multiquote').strip().upper()
        else:
            flag_multiquote="FALSE"
        if "has_header" in kwargs:
            has_header = kwargs.get('has_header')
            if has_header.strip().upper() == 'FALSE':
                has_header = False
        else:
            has_header = True
        if "replace_header" in kwargs:
            replace_header = kwargs.get("replace_header")
        else:
            replace_header = 'FALSE'
        
        if "schema_file" in kwargs:
            schema_file = kwargs.get("schema_file")
        schema, error = create_header(spark, schema_file)
        if error:
            return None, error

        for obj in objects.get('Contents'):
            if not obj.get('Key').endswith("/"):
                size_of_file = float(obj['Size'])/1000
                if int(size_of_file)==0:
                    continue
                else:
                    s3_object_list.append(obj.get('Key'))         
        print(s3_object_list)
        if len(s3_object_list)==0:
            result_df = spark.createDataFrame([],schema=schema)
            return result_df, None
        for item in s3_object_list:
            if ".csv" in item:
                csv_list.append(item)
        new_csv_list = []
        for item in csv_list:
            new_path = "s3://" + bucket_name + "/" + item
            new_csv_list.append(new_path)
        dfs = []
        if not has_header:
            schema, error = create_header(spark, schema_file)
            if error:
                return None, error
            for item in new_csv_list:
                if flag_multiquote=="TRUE":
                    tempDf = pandas.read_csv(item, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)
                    dequote=lambda x: x[1:-1] if x.startswith('"') and x.endswith('"') else x
                    tempDf = tempDf.applymap(dequote)
                    tempDf = tempDf.replace('""','"',regex=True)
                else:
                    tempDf = pandas.read_csv(item, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)
                dfs.append(tempDf)
                df = pandas.concat(dfs)
            trim_strings = lambda x: x.strip() if isinstance(x, str) else x
            df = df.applymap(trim_strings)
            result_temp_df = spark.createDataFrame(df, schema=schema)
            result_temp_df = result_temp_df.replace('nan', None)
            result_df = change_schema(spark, result_temp_df, schema_file)
            return result_df, None
        else:
            if replace_header == 'FALSE':
                schema, error = create_header(spark, schema_file)
                if error:
                    return None, error
                for item in new_csv_list:
                    if flag_multiquote=="TRUE":
                        tempDf = pandas.read_csv(item, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)
                        dequote=lambda x: x[1:-1] if x.startswith('"') and x.endswith('"') else x
                        tempDf = tempDf.applymap(dequote)
                        tempDf = tempDf.replace('""','"',regex=True)
                    else:
                        tempDf = pandas.read_csv(item, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)
                    dfs.append(tempDf)
                    df = pandas.concat(dfs)
                trim_strings = lambda x: x.strip() if isinstance(x, str) else x
                df = df.applymap(trim_strings)
                result_temp_df = spark.createDataFrame(df.astype(str))
                result_temp_df = result_temp_df.replace('nan', None)
                return result_temp_df, None
            else:
                schema, error = create_header(spark, schema_file)
                if error:
                    return None, error
                for item in new_csv_list:
                    if flag_multiquote=="TRUE":
                        tempDf = pandas.read_csv(item, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)
                        dequote=lambda x: x[1:-1] if x.startswith('"') and x.endswith('"') else x
                        tempDf = tempDf.applymap(dequote)
                        tempDf = tempDf.replace('""','"',regex=True)
                    else:
                        tempDf = pandas.read_csv(item, sep=separator, dtype = str,encoding=encoding, header=None,keep_default_na=False,na_values=na_values_considered)
                    dfs.append(tempDf)
                    df = pandas.concat(dfs)
                trim_strings = lambda x: x.strip() if isinstance(x, str) else x
                df = df.applymap(trim_strings)
                result_temp_df = spark.createDataFrame(df, schema=schema)
                result_temp_df = result_temp_df.replace('nan', None)
                result_df = change_schema(spark, result_temp_df, schema_file)
                return result_df, None
    except:
        e = get_exception()

'''
Description : Function to read fixed width file
Input : File Path, Schema File Path
Output : Spark Dataframe
'''
def read_fwf(spark, file_path, fwf_schema_file, **kwargs):
    try:
        s3 = boto3.client('s3')
        bucket_name = ccs.bucket_name
        prefix = '/'.join(file_path.split('/')[3:])
        objects = s3.list_objects(Bucket=bucket_name, Prefix=prefix)
        s3_object_list = []
        csv_list = []
        na_values_considered = "‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’, ‘<NA>’, ‘NULL’, ‘NaN’, ‘nan’, ‘null’"
        fwf_schema_df = spark.read.format("csv").option("header",True).load(fwf_schema_file)
        columns = [str(row['column_name']) for row in fwf_schema_df.collect()]
        column_widths = [int(row['column_width']) for row in fwf_schema_df.collect()]
        if "has_header" in kwargs:
            has_header = kwargs.get('has_header')
            if has_header.strip().upper() == 'FALSE':
                has_header = False
        else:
            has_header = True
        if "replace_header" in kwargs:
            replace_header = kwargs.get("replace_header")
        else:
            replace_header = 'FALSE'
        for obj in objects.get('Contents'):
            s3_object_list.append(obj.get('Key'))
        for item in s3_object_list:
            csv_list.append(item)
        new_csv_list = []
        if "schema_file" in kwargs:
            schema_file = kwargs.get("schema_file")
        if "add_file_name" in kwargs:
            add_file_name = kwargs.get("add_file_name")
        else:
            add_file_name = 'FALSE'
        for item in csv_list:
            new_path = "s3://" + bucket_name + "/" + item
            new_csv_list.append(new_path)
        dfs = []
        if has_header == False:
            schema, error = create_header(spark, schema_file)
            if error:
                return None, error
            for item in new_csv_list:
                tempDf = pandas.read_fwf(item, widths=column_widths, names=columns,dtype = str,keep_default_na=False,na_values=na_values_considered)
                dfs.append(tempDf)
                df = pandas.concat(dfs)
            result_temp_df = spark.createDataFrame(df, schema=schema)
            result_temp_df = result_temp_df.replace('nan', None)
            result_df = change_schema(spark, result_temp_df, schema_file)
            return result_df, None
        else:
            if replace_header == 'FALSE':
                for item in new_csv_list:
                    tempDf = pandas.read_fwf(item, widths=column_widths, names=columns,dtype = str, header=0,keep_default_na=False,na_values=na_values_considered)
                    if add_file_name.upper() == 'TRUE':
                        tempDf = tempDf.assign(FileName=os.path.basename(item))
                    dfs.append(tempDf)
                    df = pandas.concat(dfs)
                result_df = spark.createDataFrame(df.astype(str))
                result_df = result_df.replace('nan', None)
                return result_df, None
            else:
                schema, error = create_header(spark, schema_file)
                if error:
                    return None, error
                for item in new_csv_list:
                    tempDf = pandas.read_fwf(item, widths=column_widths, names=columns,dtype = str, header=0,keep_default_na=False,na_values=na_values_considered)
                    if add_file_name.upper() == 'TRUE':
                        tempDf = tempDf.assign(FileName=os.path.basename(item))
                    dfs.append(tempDf)
                    df = pandas.concat(dfs)
                #df = df.replace({np.nan: None})
                result_temp_df = spark.createDataFrame(df, schema=schema)
                result_temp_df = result_temp_df.replace('nan', None)
                result_df = change_schema(spark, result_temp_df, schema_file)
                return result_df, None
    except:
        e = get_exception()


'''
Description : Function to write partitions
Input : Spark Dataframe, Parition columns, Delimiter, Target Path, File Format, Write Mode
Output : Spark Dataframe
'''
def write_partitions(src_df, target_base_path, **kwargs):
    try:
        if "delimiter" in kwargs:
            delimiter = kwargs.get('delimiter')
        else:
            delimiter = '|'
        if "partition_cols" in kwargs:
            partition_cols = kwargs.get("partition_cols")
            log.info("Partition Columns : ", str(partition_cols))
        else:
            e = "Partition By column not provided"
            raise Exception(e)
        if "write_mode" in kwargs:
            write_mode = kwargs.get("write_mode")
        else:
            write_mode = "overwrite"
        if "format" in kwargs:
            file_format = kwargs.get("format")
        else:
            file_format = "parquet"
        partition_list, error2 = get_join_key(partition_cols, delimiter)
        write_df = src_df.repartition(*partition_list)
        write_df.write.partitionBy(*partition_list).format(file_format).mode(write_mode).save(target_base_path)
        return write_df, None
    except:
        e = get_exception()
        return None, e

'''
Description : Function to generate hierarchy
Input : Spark Dataframe, Seed Condition, Prior Column, Parent Column,Max Level
Output : Spark Dataframe
'''
def generate_hierarchy(src_df, **kwargs):    
    try:
        seed_cond = kwargs.get('seed_condition')
        prior_col = kwargs.get('prior_col')
        parent_col = kwargs.get('parent_col')
        gen_path_for = kwargs.get('gen_path_for')
        delimiter = kwargs.get('delimiter')
        gen_path_for = gen_path_for.split(delimiter)
        path_sep = kwargs.get('path_separator')
        max_level = kwargs.get('max_level')
        max_level = int(max_level)
        
        if src_df.rdd.isEmpty():
            e = 'Input Dataframe cannot be empty'
            raise Exception(e)
        lvl_counter = 1
        rslt_df = src_df.filter(f"{seed_cond}").withColumn('LEVEL', lit(lvl_counter))
        if rslt_df.rdd.isEmpty():
            e = 'There are no records in the input dataframe that matches the seed condition.'
            raise Exception(e)
        rslt_df = generate_path(rslt_df, **kwargs)
        tmp_src_df = src_df.alias('A').join(rslt_df.alias('B'), col(f'A.{prior_col}') == col(f'B.{prior_col}'), 'left_anti')
        while not tmp_src_df.rdd.isEmpty():
            lvl_counter += 1
            if lvl_counter > max_level:
                e = 'Max level depth has been reached.'
                log.info(e)
            
            nxt_rslt_dF = rslt_df.alias('A') \
                                .join(tmp_src_df.alias('B'), col(f'A.{prior_col}') == col(f'B.{parent_col}')) \
                                .withColumn('LEVEL', lit(lvl_counter)) \
                                .select(*[f'B.{cols}' for cols in tmp_src_df.columns], 'LEVEL', *[col(f'A.{cols}_path') for cols in gen_path_for])
            rslt_df = rslt_df.union(generate_path(nxt_rslt_dF, **kwargs))

            tmp_src_df = src_df.alias('A').join(rslt_df.alias('B'), col(f'A.{prior_col}') == col(f'B.{prior_col}'), 'left_anti')

        return rslt_df
    except:
        e = get_exception()

'''
Description : Function to generate path for hierarchy
Input : Spark Dataframe, Path,Delimiter,Path Separator
Output : Spark Dataframe
'''

def generate_path(df, **kwargs):
    try:
        gen_path_for = kwargs.get('gen_path_for')
        delimiter = kwargs.get('delimiter')
        gen_path_for = gen_path_for.split(delimiter)
        path_sep = kwargs.get('path_separator')
        for cols in gen_path_for:
            if f'{cols}_path' in df.columns:
                prefix = col(f'{cols}_path')
            else:
                prefix = lit('')
            df = df.withColumn(f'{cols}_path', concat_ws(path_sep, prefix, coalesce(col(cols), lit(''))))
        return df
    except:
        e = get_exception()


'''
Description : Function to generate Email
Input : Email Type, Email Content
'''

def email_generation(email_type, **kwargs):
    if email_type == 'empty_file':
        email_content = """
        <div id="appendonsend">&nbsp;</div>
        <div>Hi ARO Team,<br /> <br /> Either Source file is missing or file is having 0 byte data for Job %s. Please re-run the job once files are available.  <br /><br /> Regards,<br /> Data Processing Team</div>
        """%(ETL_PRCS_NAME)
        try:
            SERVER = "localhost"
            MESSAGE = MIMEMultipart('alternative')
            MESSAGE['subject'] = "Missing Source File/Empty File in %s - "%(ccs.environment)+ str(ETL_PRCS_NAME)
            MESSAGE['To'] = ccs.aro_team_mail_id
            MESSAGE['From'] = ccs.audit_team_mail_id
            HTML_BODY = MIMEText(email_content, 'html')
            MESSAGE.attach(HTML_BODY)
            server = smtplib.SMTP(SERVER)
            server.sendmail(ccs.audit_team_mail_id,ccs.aro_team_mail_id.split(","), MESSAGE.as_string())
            server.quit()
            log.info("Email Notification has been sent!")
            return True
        except:
            e = get_exception()
    elif email_type =='multiple_files':
        file_path = kwargs.get('file_path')
        email_content = """
        <div id="appendonsend">&nbsp;</div>
        <div>Hi User(s),<br /> <br /> There are multiple files for Concur in the below path - 
          <br/><br/>
        <table class="x_mystyle" style="font-size: 9pt; border-collapse: collapse;">
        <tbody>
        <tr>
        <th style="border: 2px solid black; padding: 12px; text-align: left; background: #333399; color: white;">Files Location</th>
        <th style="border: 2px solid black; padding: 12px; text-align: left; background: #333399; color: white;">:</th>
        <td style="border: 2px solid black; padding: 12px; text-align: left;">%s</td>
        </tr>
        </tbody>
        </table>
        <br /><br /> Reagrds,<br /> Data Processing Team</div>
        """ % (file_path)
        send_mail(email_content, **kwargs)


'''
Description : Function to send Email
Input : To, From, Subject, Body, File Path, File Name
Output : TRUE (Email is Sent)
'''
def send_mail(file_path,**kwargs):
    try:
        SERVER = "localhost"
        MESSAGE = MIMEMultipart('alternative')
        MESSAGE['subject'] = kwargs.get('subject') +" in "+ccs.environment+" for "+ETL_PRCS_NAME
        MESSAGE['To'] = kwargs.get('to')
        body = kwargs.get('body')
        MESSAGE['From'] = kwargs.get('from')
        file_name = kwargs.get('file_name')
        timestr = time.strftime("%Y%m%d")
        file_name = file_name+"_"+timestr+".csv"
        email_content = """
        <div id="appendonsend">&nbsp;</div>
        <div>Hi Team,<br /> <br /> %s
          <br/><br/>
        <table class="x_mystyle" style="font-size: 9pt; border-collapse: collapse;">
        <tbody>
        <tr>
        <th style="border: 2px solid black; padding: 12px; text-align: left; background: #333399; color: white;">Report Location</th>
        <td style="border: 2px solid black; padding: 12px; text-align: left;">%s</td>
        </tr>
        <tr>
        <th style="border: 2px solid black; padding: 12px; text-align: left; background: #333399; color: white;">File Name</th>
        <td style="border: 2px solid black; padding: 12px; text-align: left;">%s</td>
        </tr>
        </tbody>
        </table>
        <br /><br /> Regards,<br /> Data Processing Team</div>
        """ % (body,file_path,file_name)
        HTML_BODY = MIMEText(email_content, 'html')
        MESSAGE.attach(HTML_BODY)
        server = smtplib.SMTP(SERVER)
        server.sendmail(MESSAGE['From'], MESSAGE['To'].split(","), MESSAGE.as_string())
        server.quit()
        return True 
    except:
        e = get_exception()

'''
Description : Function to join two dataset
Input : Two Spark Dataframe between whom join needs to be performed
Ouput : Spark Dataframe (Joined Output)
'''

def join_dataset(primary_df, join_df, **kwargs):
    try:
        result_df = None
        filter_clause = None
        not_nullable_column_list = None
        not_nullable_default_value = None
        delimiter = cc.input_column_delimiter
        all_cols_from_left = False
        all_cols_from_right = False
        join_type = "inner"
        l_df_cols = []
        l_join_df_cols = []
        l_rename_df_cols = []
        l_rename_join_df_cols = []
        # mandatory tags
        df_name = kwargs.get("dataframe_name")
        join_df_name = kwargs.get("join_dataframe_name")
        result_df_name = kwargs.get("result_dataframe")
        df_key_cols = kwargs.get("dataframe_keys")
        join_df_key_cols = kwargs.get("join_dataframe_keys")
        result_df_name = kwargs.get("result_dataframe")
        df_cols = kwargs.get("dataframe_columns")
        join_df_cols = kwargs.get("join_dataframe_columns")

        # check and set optional tags, if present
        # setting delimiter
        if "delimiter" in kwargs:
            delimiter = kwargs.get("delimiter")

        if "not_nullable_column_list" in kwargs:
            c_l = kwargs.get("not_nullable_column_list")
            if c_l and c_l.strip() != '':
                not_nullable_column_list = c_l.strip().split(delimiter)
                not_nullable_default_value = str(kwargs.get("not_nullable_value")).strip()

        if "rename_dataframe_columns" in kwargs:
            rdc = kwargs.get('rename_dataframe_columns')
            if rdc and rdc.strip() != '':
                l_rename_df_cols = rdc.strip().split(delimiter)
        if "rename_join_dataframe_columns" in kwargs:
            rjdc = kwargs.get('rename_join_dataframe_columns')
            if rjdc and rjdc.strip() != '':
                l_rename_join_df_cols = rjdc.strip().split(delimiter)

        # Added Logic to exclude columns from dataframe or join dataframe
        # setting df_columns
        if not df_cols or df_cols.strip() == '':
            pass
        elif df_cols.strip() == '*':
            all_cols_from_left = True
            l_df_cols = primary_df.columns
        else:
            l_df_cols = df_cols.split(delimiter)

        # setting join_df columns
        if not join_df_cols or join_df_cols.strip() == '':
            pass
        elif join_df_cols.strip() == '*':
            all_cols_from_right = True
            l_join_df_cols = join_df.columns
        else:
            l_join_df_cols = join_df_cols.split(delimiter)
        # setting join_type
        if "join_type" in kwargs:
            jt = kwargs.get('join_type')
            if jt:
                jt = jt.strip().lower()
            if jt in ['inner', 'outer', 'left_outer', 'right_outer', 'leftsemi']:
                join_type = jt

        else:
            e = "Join Type was not found in kwargs"
            raise Exception(e)
        if "filter_clause" in kwargs:
            fcls = kwargs.get("filter_clause")
            if fcls and fcls.strip() != '':
                filter_clause = fcls

        filter_join_clause = None
        if "filter_join_dataframe" in kwargs:
            fcls = kwargs.get("filter_join_dataframe")
            if fcls and fcls.strip() != '':
                filter_join_clause = fcls
        if filter_join_clause:
            join_df = join_df.filter(filter_join_clause)

        check_df_key_cols = df_key_cols.strip()
        check_join_df_key_cols = join_df_key_cols.strip()
        if check_df_key_cols == '' or check_join_df_key_cols == '':
            e = "INVALID_PARAMS"," Insuffucient key columns provided in params, dataframe_keys:<{}> v/s join_dataframe_keys:<{}> to formulate a join".format(df_key_cols, join_df_key_cols)
            raise Exception(e)

        l_df_keys = df_key_cols.split(delimiter)
        l_join_df_keys = join_df_key_cols.split(delimiter)
        if len(l_df_keys) != len(l_join_df_keys):
            e = "INVALID_PARAMS", "There seems to be a mismatch between the number" + "of keys provided in dataframe_keys<{} v/s join_dataframe_keys<{}.".format(len(l_df_keys), len(l_join_df_keys))
            raise Exception(e)

        l_key_set = zip(l_df_keys, l_join_df_keys)
        cond_set = []
        common_cols = OrderedDict()
        all_join_cols_match = True
        for s in l_key_set:
            left_expr = s[0]
            right_expr = s[1]
            if left_expr != right_expr:
                all_join_cols_match = False
            elif left_expr == right_expr:
                common_cols[left_expr] = 0

            cond_set.append(primary_df[left_expr] == join_df[right_expr])
        if all_join_cols_match:
            cond_set = list(common_cols.keys())
        

        if all_cols_from_left and all_cols_from_right:
            result_df = primary_df.join(join_df, cond_set, join_type).alias(result_df_name)
        else:
            select_cols, rename_cols = cmp_duplicates(l_df_cols, l_join_df_cols, df_name, join_df_name,
                                                      l_rename_df_cols, l_rename_join_df_cols)

            result_df = primary_df.alias("A").join(join_df.alias("B"), cond_set, join_type).select(*select_cols).toDF(
                *rename_cols).alias(result_df_name)

        # implementing filter
        if filter_clause:
            result_df = result_df.filter(filter_clause)

        if not_nullable_column_list:
            col = ""
            for cols in not_nullable_column_list:
                col = col + cols + ", "
            col = col[0:(len(col) - 2)]
            expr = "result_df.na.fill({}, '{}')".format(not_nullable_default_value, col.strip())
            result_df = eval(expr)
        return result_df, None
    except:
        e = get_exception()
        log.warn(e)
        return None, e

'''
Description : Function to validate CSV file for -
    1. Incorrect File Extension
    2. Incorrect Delimiter
    3. Incorrect Number of Columns 
Input : File Path, File Format, Schema File Path, Delimiter
Output : TRUE (File Validation Successfull)
'''

def validate_file(spark,file_path,file_format,schema_file,column_delimiter,**kwargs):
    try:
        schema_df = spark.read.format('csv').option('header',True).option('quote', '"').load(schema_file)
        column_count = schema_df.count()
        log.info("Number of Columns in Schema :"+str(column_count ))
        prefix = '/'.join(file_path.split('/')[3:])
        bucket_name = file_path.split('/')[2]
        command = "aws s3api list-objects-v2 --bucket " + bucket_name + " --prefix " + prefix + " --query 'Contents[].[Key]' --output text"
        files = subprocess.getoutput(command)
        file_list = files.split('\n')
        file_format_set = set()
        for file in file_list:
            file_format_set.add(file.split('.')[-1])
        file_format_list = list(file_format_set)
        if prefix in file_format_list:
            file_format_list.remove(prefix)
        if (prefix + '_SUCCESS') in file_format_list:
            file_format_list.remove((prefix + '_SUCCESS'))
        if len(file_format_list) == 1 and file_format_list[0] == file_format:
            df = spark.read.format(file_format).option('delimiter',column_delimiter).load(file_path)
            if df.rdd.isEmpty() == False:
                number_of_columns = len(df.columns)
                log.info("Number of Columns in Data :"+str(number_of_columns))
                if number_of_columns != column_count:
                    log.error("Incorrect Delimiter or Incorrect Number of Columns in File")
                    e = get_exception()
                    return None,e
            else:
                log.warn("Source File is Empty")
            return True, None
        else:
            log.error("Incorrect File Extension")
            e = get_exception()
            return None,e
    except:
        e = get_exception()
        return None, e


'''
Description : Function to write file with custom file name
Input : Spark Dataframe
Output : File Name and File Path
'''

def custom_write_file(df, spark, **kwargs):
    try:
        file_path = kwargs.get('file_path')
        
        delimiter = kwargs.get('delimiter', ',')
        has_header = kwargs.get('has_header', 'FALSE').strip().upper()
        add_ts_to_file_name = kwargs.get('add_ts_to_file_name', 'TRUE').strip().upper()
        file_extension = kwargs.get('file_extension', '.csv').strip()

        bucket_list = file_path.split('/')
        bucket = bucket_list[2]
        prefix = '/'.join(file_path.split('/')[3:])
        file_name  = kwargs.get('file_name')
        file_format = kwargs.get('file_format')
        mode = kwargs.get('mode')
        
        if file_format != 'text':
            pandas_df = df.toPandas()
        
        if add_ts_to_file_name == 'TRUE':
            timestr = time.strftime("%Y%m%d")
            file_name = file_name+"_"+timestr
        
        s3_conn = boto3.client('s3')
        
        if file_format == 'text':
            file_name = file_name + file_extension 

            if has_header == 'TRUE':
                header = delimiter.join(df.columns)
                header_df = spark.createDataFrame([(header, 1)], ['Value', 'ROW_NUM']).withColumn('ROW_NUM', col('ROW_NUM').cast('int'))
                temp_df = df.select(concat_ws(delimiter, *df.columns).alias('Value')).withColumn('ROW_NUM', lit('2').cast('int'))
                temp_df = header_df.union(temp_df).orderBy('ROW_NUM').select('Value')
            else:
                temp_df = df.select(concat_ws(delimiter, *df.columns)).toDF('Value')

            temp_df.coalesce(1).write.mode(mode).format('text').option('header', 'true').save(file_path)

            cmd = f"aws s3api list-objects-v2 --bucket {bucket} --prefix {prefix} --query 'Contents[].[Key]' --output text"
            results = subprocess.getoutput(cmd)
            if results:
                results = results.split()

                temp_file_name = [x for x in results if '.txt' in x.lower() and 'part-00000-' in x.lower()]
                if len(temp_file_name) == 1:
                    temp_file_name = f's3://{bucket}/{temp_file_name[0]}'
                else:
                    e = f"More than one text file was found at {file_path}"
                    raise Exception(e)
            else:
                e = f"No files were found at {file_path}"
                raise Exception(e)

            cmd = f'aws s3 cp {temp_file_name} {file_path}{file_name}'
            os.system(cmd)
            cmd = f'aws s3 rm {temp_file_name}'
            os.system(cmd)

            return file_name, file_path

        if file_format=='parquet':
            file_name = file_name+".parquet"
            prefix = prefix+file_name
            table = pa.Table.from_pandas(pandas_df)
            pq.write_table(table, file_name)
            if mode == 'append':
                s3_conn.upload_file(file_name, bucket, prefix)
            if mode == 'overwrite':
                s3_rm_command = "aws s3 rm " + file_path + " --recursive --quiet --only-show-errors"
                os.system(s3_rm_command)
                s3_conn.upload_file(file_name, bucket, prefix)
        
        if file_format == 'csv':
            file_name = file_name+".csv"
            prefix = prefix+file_name
            pandas_df.to_csv(file_name,index=False)
            if mode == 'append':
                s3_conn.upload_file(file_name, bucket, prefix)
            if mode == 'overwrite':
                s3_rm_command = "aws s3 rm " + file_path + " --recursive --quiet --only-show-errors"
                os.system(s3_rm_command)
                s3_conn.upload_file(file_name, bucket, prefix)
        os.remove(file_name)
        return file_name, file_path
    except:
        e = get_exception()


def delete_records_from_partitions(spark, src_df, target_base_path,**kwargs):
    try:
        if "delimiter" in kwargs:
            delimiter = kwargs.get('delimiter')
        else:
            delimiter = '|'
        if "join_keys" in kwargs:
            join_keys = kwargs.get("join_keys")
            #log.info("Join Keys : "+str(join_keys))
        else:
            e = "Joining Key Columns Not Provided"
            raise Exception(e)
        if "partition_cols" in kwargs:
            partition_cols = kwargs.get("partition_cols")
            #log.info("Partition Columns : "+str(partition_cols))
        else:
            e = "Partition By column  not provided"
            raise Exception(e)
        join_keys_list, error1 = get_join_key(join_keys, delimiter)
        partition_list, error2 = get_join_key(partition_cols, delimiter)
        check_file_path = '/'.join(target_base_path.split('/')[3:])
        s3 = boto3.client('s3', region_name='us-east-2')
        bucketname = ccs.translate_bucket_name
        # ct_dt = datetime.now()
        # ct_dt = ct_dt - timedelta(days=365)
        # ct_dt_str = ct_dt.strftime('%Y-%m') 
        # log.info("Year - "+ct_dt_str)       
        response = s3.list_objects_v2(Bucket=bucketname, Prefix=check_file_path)
        if 'Contents' in response:
            join_keys_list_temp = join_keys_list+partition_list
            tgt_df  = spark.read.parquet(target_base_path)
            print(join_keys_list_temp)
            temp_df = tgt_df.select(join_keys_list_temp)
            src_temp_df = src_df.select(join_keys_list_temp)
            temp_df = temp_df.unionAll(src_temp_df).distinct()
            #print(temp_df.count())
            temp_df.registerTempTable("dup_check")
            dedup_query = "Select count(*) as dedup_count from (Select count(*),"
            for cols in join_keys_list:
                dedup_query = dedup_query+cols+","
            dedup_query = dedup_query[0:(len(dedup_query) - 1)]
            dedup_query = dedup_query+" from dup_check group by "
            for cols in join_keys_list:
                dedup_query = dedup_query+cols+","
            dedup_query = dedup_query[0:(len(dedup_query) - 1)]
            dedup_query = dedup_query+" having count(*)>1)"
            #print(dedup_query)
            dedup_df = spark.sql(dedup_query)
            #print(dedup_df.show())
            dedup_df = dedup_df.filter("dedup_count>0")
            if dedup_df.rdd.isEmpty():
                log.info("No Duplicate Records found!")
                return True
            #tgt_df = tgt_df.filter("date_format(partn_gl_dt,'yyyy-MM')>='"+ct_dt_str+"' or date_format(partn_gl_dt,'yyyy-MM')='1899-12'")
            #print(tgt_df.count())
            tgt_col_list = tgt_df.columns
            #tgt_df.registerTempTable("tgt")
            #sql = "Select * from tgt where partn_gl_dt>='"+ct_dt_str+"' or partn_gl_dt='1899-12'"
            #sql = "Select * from tgt where partn_gl_dt>='2019-07' or partn_gl_dt='1899-12'"
            #print(sql)
            #tgt_df = spark.sql(sql)
            tgt_df.registerTempTable("tgt")
            src_df.registerTempTable("src")

            """
            Steps to identify the impacted records from Target for which Source is having different partition Value
            Start ----
            """
            filter_clause = ""
            select_cols_list = ["tgt.*"]
            src_partition_cols_list = []
            partition_map_list= []
            for partition in partition_list:
                filter_clause=filter_clause+"src."+partition+"<>"+"tgt."+partition+" AND "
                src_partition_cols_list.append("src."+str(partition)+" as new_"+str(partition))
                partition_map_list.append("new_"+str(partition))
            filter_clause = filter_clause[0:(len(filter_clause) - 5)]
            log.info(filter_clause)
            select_cols = select_cols_list+src_partition_cols_list
            log.info(select_cols)
            cmn_df = tgt_df.alias("tgt").join(broadcast(src_df.alias("src")),join_keys_list,'inner') \
            .where(filter_clause).selectExpr(select_cols)
            # cmn_df.cache()
            cmn_df = cmn_df.drop(*partition_list)
            log.info(partition_map_list)
            log.info(partition_list)
            for src_cols,part_cols in zip(partition_map_list,partition_list):
                cmn_df = cmn_df.withColumnRenamed(src_cols,part_cols)
            cmn_df = cmn_df.select(tgt_col_list)
            """
            END ----
            """
            
            """
            Steps to identify the unchanged record from target for impacted partitions
            Start: - 
            """
            partition_df = tgt_df.alias("tgt").join(broadcast(src_df.alias("src")),join_keys_list,'inner') \
                .where(filter_clause).select([col('tgt.'+cols) for cols in partition_list]).distinct()
            #join_df = src_df.select(join_keys_list).distinct()
            for partition in partition_list:
                partition_df = partition_df.withColumn(partition,concat(lit(partition), lit("='"), col(partition), lit("'")))
            
            #for keys in join_keys_list:
            #    join_df = join_df.withColumn(keys,concat(lit(keys),lit(" ='"),col(keys),lit("'")))
            if len(partition_list)!=1:
                partition_df = partition_df.withColumn("Combination", concat_ws(' AND ', *partition_list))
                partition_df = partition_df.withColumn("Combination", concat(lit("("), col("Combination"), lit(")")))
            else:
                partition_df = partition_df.withColumn("Combination", concat(lit("("), col(*partition_list), lit(")")))
            
            #if len(join_keys_list)!=1:
            #    join_df = join_df.withColumn("Combination", concat_ws(' AND ', *join_keys_list))
            #    join_df = join_df.withColumn("Combination", concat(lit("("), col("Combination"), lit(")")))
            #else:
            #    join_df = join_df.withColumn("Combination", concat(lit("("), col(*join_keys_list), lit(")")))
            
            combination_list = [row["Combination"] for row in partition_df.collect()]
            log.info(combination_list)
            if len(combination_list)==0:
                return True
            #join_list = [row["Combination"] for row in join_df.collect()]
            #log.info(join_list)
            subset_query = " OR ".join(combination_list)
            #key_subset_query = " OR ".join(join_list)
            
            #unchanged_df = tgt_df.alias("tgt").join(broadcast(cmn_df.alias("cmn")),join_keys_list,'left_anti').where("(PARTN_GL_DT='2019-10')").select("tgt.*")

            unchanged_df = tgt_df.alias("tgt").join(broadcast(cmn_df.alias("cmn")),join_keys_list,'left_anti').where(subset_query).select("tgt.*")


            #query = ""
            #for item in join_list:
            #    query=query+" Select * from tgt where (("+subset_query+") and !("+item+")) union"
            #
            #query = query[0:(len(query) - 5)]
            # query = "Select * from tgt where (("+subset_query+") and !("+key_subset_query+"))"
            #log.info(query)
            #unchanged_df = spark.sql(query)
            #print(unchanged_df.count())
            log.info("Refreshing the impacted partitions...")
            write_path = '/'.join(target_base_path.split('/')[:-3]) + '/temp/' + '/'.join(target_base_path.split('/')[-3:-1]) + '/'
            unchanged_df.write.mode("overwrite").parquet(write_path)
            delete_write_path = '/'.join(target_base_path.split('/')[:-3]) + '/delete_temp/' + '/'.join(target_base_path.split('/')[-3:-1]) + '/'
            # log.info(" delete records count - "+ str(cmn_df.count()))
            cmn_df.write.mode("overwrite").parquet(delete_write_path)           
            unchanged_df = spark.read.parquet(write_path)
            unchanged_df=unchanged_df.repartition(*partition_list)
            unchanged_df.write.mode("overwrite").partitionBy(*partition_list).parquet(target_base_path)
            log.info("Impacted Partitions Refresh completed...")
            #log.info("Refreshing the incoming partitions...")
            cmn_df = spark.read.parquet(delete_write_path)
            cmn_df = cmn_df.repartition(*partition_list)
            cmn_df.write.mode("append").partitionBy(*partition_list).parquet(target_base_path) 
            # log.info("Incoming partitions refresh completed...")                      

            """
            End: -

            """
    except Exception as err:
        log.error(err)
