from multiprocessing import Process, Queue, Pool, Manager
import os
import sys
import subprocess
import pandas as pd
from datetime import datetime
import signal
import hub_config as hub
import boto3
import psycopg2
from io import StringIO

prcs_name = None
schd_name = None
btch_name = None
sbjct_area_name = None
job_param = {}

def get_file_name(bucket_name,folder):
    s3=boto3.client('s3',region_name='us-east-2')
    response = s3.list_objects_v2(Bucket=bucket_name,Prefix=folder)
    file_name = ''
    for k,v in response.items():
        if k == 'Contents':
            file_name = str(v[1]).split(':')[1].split(',')[0].split("'")[1]
    return file_name


def job_submit(i,sql_var,part_val,job_id,region,prcs_name,schd_name,btch_name,sbjct_area_name,q,):
    message='''spark-submit  --packages net.snowflake:snowflake-jdbc:3.8.4,net.snowflake:spark-snowflake_2.11:2.5.0-spark_2.4 --jars /usr/lib/spark/jars/httpclient-4.5.9.jar,/usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar --deploy-mode client --master yarn --py-files /home/hadoop/Framework/Data_Processing/SparkHub/hub_config.py /home/hadoop/Framework/Data_Processing/SparkHub/run.py --job_id '''+job_id+''' --region '''+region+''' --prcs_name '''+prcs_name+''' --schd_name '''+schd_name+''' --btch_name '''+btch_name+''' --sbjct_area_name '''+sbjct_area_name+''' --sql_vars '''+sql_var+''' --part_val '''+part_val+''' '''
    q.put(message)
    
def get_rds_con(self):
    try:
        rds = boto3.client('rds', region_name=hub.rds_region)
        password = rds.generate_db_auth_token(DBHostname=hub.rds_host, Port=hub.rds_port, DBUsername=hub.rds_user,
                                              Region=hub.rds_region)
        conn = psycopg2.connect(user=hub.rds_user, password=password, host=hub.rds_host, database=hub.rds_database,
                                options=f'-c search_path={hub.rds_schema}')
        return conn
    except Exception as err:
        print(err)
    
    
def job_exec(i,q):
    message = q.get()
    ret, out = subprocess.getstatusoutput(message)
    print(out)

def init_worker():
    """
    Pool worker initializer for keyboard interrupt on Windows
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def close_rds_con(conn):
    if conn is not None:
        conn.close()
        print("Connection is closed")
    
def get_partitions():
    job_id = sys.argv[1] #Get the JOB_ID as argument
    prcs_name = sys.argv[2] #Get the Process for ABC
    region = sys.argv[3] #Get the Region as argument for S3 Path identification
    schd_name = sys.argv[4] #Get the Schedule Name for ABC
    btch_name = sys.argv[5] #Get the Batch Name for ABC
    sbjct_area_name = sys.argv[6] #Get the Subject Area Name for ABC
    s3client = boto3.client('s3',region_name='us-east-2')
    bucketname = hub.config_bucket_name
    restart_file_path="/home/hadoop/dynamic_partition/"+region+"/restart/restart_partition_file_"+job_id+".txt"
    isExist = os.path.exists(restart_file_path)
    if isExist:
        restart_file="dynamic_partition/"+region+"/restart/restart_partition_file_"+job_id+".txt"
        fileobj = s3client.get_object(Bucket=hub.config_bucket_name,Key=restart_file)
        partition_list = fileobj['Body'].read().decode('utf-8').splitlines(False)
    else:
        folder = 'dynamic_partition/'+region+'/'+job_id+'/'
        print("Partition List Path :",folder)
        file_to_read = get_file_name(bucketname,folder)
        fileobj = s3client.get_object(Bucket=hub.config_bucket_name,Key=file_to_read)
        partition_list = fileobj['Body'].read().decode('utf-8').splitlines(False)
    os.chdir("/home/hadoop/Framework/Data_Processing/SparkHub")
    return partition_list,job_id,region,prcs_name,schd_name,btch_name,sbjct_area_name
    
    
def check_job_status(etl_prcs_run_id,job_id):
    region = sys.argv[3]
    restart_file_path="/home/hadoop/dynamic_partition/"+region+"/restart/restart_partition_file_"+job_id+".txt"
    isExist = os.path.exists(restart_file_path)
    if isExist:
        os.remove(restart_file_path)
        remove_from_s3_cmd="aws s3 rm s3://"+hub.config_bucket_name+"/dynamic_partition/"+region+"/restart/restart_partition_file_"+job_id+".txt"
        ret, out = subprocess.getstatusoutput(remove_from_s3_cmd)
    else:
        print("Not a restart scenario")
    failed_part = job_status(etl_prcs_run_id,job_id)
    return failed_part
    
    
def job_status(etl_prcs_run_id,job_id):
        try:
            region = sys.argv[3]
            connection = get_rds_con(hub)
            failed_part = 0
            with connection.cursor() as cursor:
                sql = "SELECT PARTN_VAL FROM ETL_JOB_RUN " \
                      "WHERE ETL_PRCS_RUN_ID = "+etl_prcs_run_id+" and RUN_STS_CD in (0,-1) "
                print(sql)
                cursor.execute(sql)
                res_set = cursor.fetchall()
                if len(res_set) == 0:
                    print("All Partition successfully ran")
                else:
                    print("Job Failed at Partition Level")
                    failed_part = len(res_set)
                    path="/home/hadoop/dynamic_partition/"+region+"/restart/restart_partition_file_"+job_id+".txt"
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    restart_file = open(path,"a")
                    for i in range(0,len(res_set)):
                        restart_file.write(res_set[i][0])
                        restart_file.write("\n")
                    restart_file.close()
                    move_to_s3_cmd="aws s3 cp " + path +" s3://"+hub.config_bucket_name+"/dynamic_partition/"+region+"/restart/"
                    print(move_to_s3_cmd)
                    ret, out = subprocess.getstatusoutput(move_to_s3_cmd)
                    if(ret==0):
                        print("Restart file moved to S3")    
                        print(out)
                    else:
                        print("Failed to move restart file in S3")
                        print(out)
                return failed_part
            connection.commit()
        finally:
            connection.close()
            
            


## Function to retrieve Schedule Id for a given Schedule and Region ##
def get_schd_id(schd_name, region):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_SCHD_ID FROM ETL_SCHD WHERE ETL_SCHD_NAME = '" + schd_name + "' AND RGN_CD = '" + region + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) != 1:
                    raise Exception("Schedule does not exist for Schedule Name : {} and Region : {}".format(schd_name,region))
            else:
                    etl_schd_id = str(res_set[0][0])
            return etl_schd_id
    except Exception as err:
        raise Exception(err)

## Function to retrieve Batch Id for a given Batch and Region ##
def get_btch_id(btch_name, region):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_BTCH_ID FROM ETL_BTCH WHERE ETL_BTCH_NAME = '" + btch_name + "' AND RGN_CD = '" + region + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) != 1:
                    raise Exception("Batch does not exist for Batch Name : {} and Region : {}".format(btch_name,region))
            else:
                    etl_btch_id = str(res_set[0][0])
            return etl_btch_id
    except Exception as err:
        raise Exception(err)

## Function to retrieve Subject Area Id for a given Subject Area and Region ##
def get_sbjct_area_id(sbjct_area_name, region):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_SBJCT_AREA_ID FROM ETL_SBJCT_AREA WHERE ETL_SBJCT_AREA_NAME = '" + sbjct_area_name + "' AND RGN_CD = '" + region + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) != 1:
                    raise Exception("Subject Area does not exist for Subject Area Name : {} and Region : {}".format(sbjct_area_name,region))
            else:
                    etl_sbjct_area_id = str(res_set[0][0])
            return etl_sbjct_area_id
    except Exception as err:
        raise Exception(err)


## Function to retrieve Process Id for a given Process and Region ##
def get_prcs_id(prcs_name, region):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_PRCS_ID FROM ETL_PRCS WHERE ETL_PRCS_NAME = '" + prcs_name + "' AND RGN_CD = '" + region + "' "
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) != 1:
                    raise Exception("Process does not exist for Process Name : {} and Region : {}".format(prcs_name,region))
            else:
                    etl_prcs_id = str(res_set[0][0])
            return etl_prcs_id
    except Exception as err:
        raise Exception(err)

## Function to retrieve Job Id for a given Process and Region ##
def get_job_id(prcs_name, region):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_JOB_ID FROM ETL_JOB WHERE ETL_PRCS_ID = (SELECT ETL_PRCS_ID FROM ETL_PRCS WHERE ETL_PRCS_NAME = '" + prcs_name + "' AND RGN_CD = '" + region + "')"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) != 1:
                    raise Exception("Job does not exist for Process Name : {} and Region : {}".format(prcs_name,region))
            else:
                    etl_job_id = str(res_set[0][0])
            return etl_job_id
    except Exception as err:
        raise Exception(err)


## Function to retrieve Run Ids for a given Schedule, Batch and Subject Area ##
def get_run_ids(etl_schd_id,etl_btch_id,etl_sbjct_area_id):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql1 = "SELECT ETL_SCHD_RUN_ID FROM ETL_SCHD_RUN WHERE ETL_SCHD_ID ='" + etl_schd_id + "' and END_DTM IS NULL AND CUR_IND = 'Y'"
            cursor.execute(sql1)
            res_set = cursor.fetchall()
            if len(res_set) > 1:
                raise Exception("More than one Schedule is RUNNING for Schedule Id : {}".format(etl_schd_id))
            elif len(res_set) < 1:
                print("No Schedule is currently RUNNING for Schedule Id : {}. Starting Adhoc Process Execution!!".format(etl_schd_id))
                etl_schd_run_id = '99999'
            else:
                etl_schd_run_id = str(res_set[0][0])
            sql2 = "SELECT ETL_BTCH_RUN_ID FROM ETL_BTCH_RUN WHERE ETL_BTCH_ID ='" + etl_btch_id + "' and ETL_SCHD_ID ='" + etl_schd_id + "' and END_DTM IS NULL AND CUR_IND = 'Y'"
            cursor.execute(sql2)
            res_set = cursor.fetchall()
            if len(res_set) > 1:
                raise Exception("More than one Batch is RUNNING for Schedule Id : {} and Batch Id :{}".format(etl_schd_id,etl_btch_id))
            elif len(res_set) < 1:
                print("No Batch is currently RUNNING for Schedule Id : {} and Batch Id : {}. Starting Adhoc Process Execution!!".format(etl_schd_id,etl_btch_id))
                etl_btch_run_id = '99999'
            else:
                etl_btch_run_id = str(res_set[0][0])
            sql3 = "SELECT ETL_SBJCT_AREA_RUN_ID FROM ETL_SBJCT_AREA_RUN WHERE ETL_SBJCT_AREA_ID ='" + etl_sbjct_area_id + "' and ETL_BTCH_ID ='" + etl_btch_id + "' and ETL_SCHD_ID ='" + etl_schd_id + "' and END_DTM IS NULL AND CUR_IND = 'Y'"
            cursor.execute(sql3)
            res_set = cursor.fetchall()
            if len(res_set) > 1:
                raise Exception("More than one Subject Area is RUNNING for Schedule Id : {} , Batch Id : {} and Subject Area Id : {}".format(etl_schd_id,etl_btch_id,etl_sbjct_area_id))
            elif len(res_set) < 1:
                print("No Subject Area is currently RUNNING for Schedule Id : {} , Batch Id : {} and Subject Area Id : {}. Starting Adhoc Process Execution!!".format(etl_schd_id,etl_btch_id,etl_sbjct_area_id))
                etl_sbjct_area_run_id = '99999'
            else:
                etl_sbjct_area_run_id = str(res_set[0][0])
        return etl_schd_run_id,etl_btch_run_id,etl_sbjct_area_run_id
    except Exception as err:
        raise Exception(err)

## Function to generate Parameter Values ##
def generate_param_file():
   connection = get_rds_con(hub)
   ct_dt = datetime.now()
   prcs_name = sys.argv[2]
   region = sys.argv[3]
   if '/' in region:
    region = region.replace('/','_')
   schd_name = sys.argv[4]
   btch_name = sys.argv[5]
   sbjct_area_name = sys.argv[6]
   etl_prcs_id = get_prcs_id(prcs_name,region)
   etl_schd_id = get_schd_id(schd_name, region)
   etl_btch_id = get_btch_id(btch_name, region)
   etl_sbjct_area_id = get_sbjct_area_id(sbjct_area_name, region)
   etl_job_id = get_job_id(prcs_name, region)
   etl_schd_run_id,etl_btch_run_id,etl_sbjct_area_run_id = get_run_ids(etl_schd_id,etl_btch_id,etl_sbjct_area_id)
   print('ETL_SCHD_RUN_ID: {}'.format(etl_schd_run_id))
   print('ETL_BTCH_RUN_ID: {}'.format(etl_btch_run_id))
   print('ETL_SBJT_AREA_RUN_ID: {}'.format(etl_sbjct_area_run_id))
   
   try:
       sql = "SELECT R.parm_name as PARM_NAME, R.parm_val as PARM_VAL FROM " \
                  "(SELECT DISTINCT t2.parm_name, coalesce(t2.parm_val,'NULL') as parm_val " \
                  "FROM ETL_PRCS t1 JOIN ETL_PRCS_PARM t2 " \
                  "ON t1.etl_prcs_id = t2.etl_prcs_id " \
                  "WHERE UPPER (t2.actv_ind) = 'Y' " \
                  "AND UPPER (t1.actv_ind) = 'Y' " \
                  "AND t1.etl_prcs_name = '" + prcs_name + "' " \
                  "AND t1.RGN_CD = '" + region + "' " \
                  "ORDER BY t2.parm_name)R " \
                  "UNION " \
                  "SELECT CAST ('jp_EtlPrcsDt' AS VARCHAR(20)) AS parm_name, " \
                  "CAST (TO_CHAR (P.MAX_STRT_DTM, 'YYYY-MM-DD HH24:MI:SS') AS VARCHAR(20)) AS parm_val FROM (  " \
                  "SELECT coalesce(EPR.ETL_PRCS_ID,EP.ETL_PRCS_ID) AS ETL_PRCS_ID, " \
                  "coalesce (MAX (EPR.STRT_DTM), TO_DATE ('1950-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) AS MAX_STRT_DTM " \
                  "FROM ETL_PRCS EP LEFT OUTER JOIN ETL_PRCS_RUN EPR " \
                  "ON EPR.etl_prcs_id = EP.etl_prcs_id " \
                  "WHERE coalesce(EPR.RUN_STS_CD, 9999) > 0 " \
                  "AND EP.etl_prcs_name = '" + prcs_name + "' " \
                  "AND EP.RGN_CD = '" + region + "' " \
                  "GROUP BY coalesce(EPR.ETL_PRCS_ID,EP.ETL_PRCS_ID))P " \
                  "UNION " \
                  "SELECT CAST ('jp_CDCLowDtm' AS VARCHAR(20)) AS parm_name, " \
                  "CAST (TO_CHAR (P.MAX_CDC_UPR_DTM + interval '1 second', 'YYYY-MM-DD HH24:MI:SS') AS VARCHAR(20)) AS parm_val FROM (  " \
                  "SELECT coalesce(EPR.ETL_PRCS_ID,EP.ETL_PRCS_ID) AS ETL_PRCS_ID, " \
                  "coalesce (MAX (EPR.CDC_UPR_DTM), TO_DATE ('1950-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) AS MAX_CDC_UPR_DTM " \
                  "FROM ETL_PRCS EP LEFT OUTER JOIN ETL_PRCS_RUN EPR " \
                  "ON EPR.etl_prcs_id = EP.etl_prcs_id " \
                  "WHERE coalesce(EPR.RUN_STS_CD, 9999) > 0 " \
                  "AND EP.etl_prcs_name = '" + prcs_name + "' " \
                  "AND EP.RGN_CD = '" + region + "' " \
                  "GROUP BY coalesce(EPR.ETL_PRCS_ID,EP.ETL_PRCS_ID))P " \
                  "UNION " \
                  "SELECT CAST ('jp_CDCUprDtm' AS VARCHAR(20)) AS parm_name, " \
                  "CAST (TO_CHAR (current_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS VARCHAR(20)) AS parm_val "\
                  "UNION " \
                  "SELECT CAST('jp_etl_prcs_run_id' AS VARCHAR(20)) AS parm_name, "\
                  "(SELECT CAST(nextval('ETL_PRCS_RUN_SEQ') AS VARCHAR(20))) AS parm_val "
       
       prcs_param_df = pd.read_sql_query(sql, connection)
       etl_prcs_run_id = str(prcs_param_df.query("parm_name=='jp_etl_prcs_run_id'")['parm_val'].iloc[0])
       print('ETL_PRCS_RUN_ID: {}'.format(etl_prcs_run_id))
       CDC_Flag = 'jp_CDCLowDtm' in prcs_param_df.parm_name.values
       Load_Flag = 'jp_LoadType' in prcs_param_df.parm_name.values


       dict = {}
       if CDC_Flag != True :
                dict["jp_CDCLowDtm"] = '1950-01-01 00:00:00'
       if Load_Flag != True :
                dict["jp_LoadType"] = 'INCREMENTAL'

       dict["jp_etl_prcs_run_id"] = etl_prcs_run_id
       dict["jp_etl_schd_run_id"] = etl_schd_run_id
       dict["jp_etl_btch_run_id"] = etl_btch_run_id
       dict["jp_etl_sbjct_area_run_id"] = etl_sbjct_area_run_id
       dict["jp_etl_prcs_name"] = prcs_name
       dict["jp_etl_prcs_id"] = etl_prcs_id
       dict["jp_ScheduleName"] = schd_name
       dict["jp_Region"] = region
       dict["jp_etl_job_id"] = etl_job_id

       
       df = pd.DataFrame(list(dict.items()), columns=['parm_name', 'parm_val'])
       prcs_param_df = prcs_param_df.append(df, ignore_index=True)
       print("Param file generated for process {} for {} region".format(prcs_name, region))
       return prcs_param_df,prcs_name,etl_prcs_run_id,etl_prcs_id
   except Exception as err:
       return None
       raise Exception(err)


## Function to Write the Parameter Values in a file in S3 ##
def write_param_file(df,prcs_name):
   try:
       s3 = boto3.resource('s3')
       key = "abc/param_file/" + prcs_name + ".parm"
       csv_buffer = StringIO()
       df.to_csv(csv_buffer, sep="=", index=False, header=False)
       s3.Object(hub.config_bucket_name, key).put(Body=csv_buffer.getvalue())
       print(
           "Successfully uploaded Param file for {} to S3".format(prcs_name + ".parm"))
   except Exception as err:
        raise Exception(err)

## Function to set the parameter values for the job from the parameter file into a dictionary ##
def set_job_parms(prcs_name):
        try:
            key = 'abc/param_file/' + prcs_name + '.parm'
            s3 = boto3.client('s3')
            read_file = s3.get_object(Bucket=hub.config_bucket_name, Key=key)
            col_name = ['param_key', 'param_val']
            df = pd.read_csv(read_file['Body'], sep='=', names=col_name)
            df = df.where(pd.notnull(df), 'NULL')
            json_param = '{'
            for index, row in df.iterrows():
                json_param = json_param + '"' + str(row["param_key"]) + '":"' + str(row["param_val"]) + '",'
            json_param = json_param[0:(len(json_param) - 1)]
            json_param = json_param + '}'
            return eval(json_param)
        except Exception as err:
            raise Exception(err)



## Function to make an entry in ABC for ETL_PRCS_RUN Table ##
def insert_abc_entry():
    ct_dt = datetime.now()
    ct_dt_str = ct_dt.strftime('%Y-%m-%d %H:%M:%S')
    connection = get_rds_con(hub)
    region = sys.argv[3]
    param_dict = {key.encode().decode('utf-8'): value.encode().decode('utf-8') for (key, value) in job_param.items()}
    etl_schd_run_id = param_dict['jp_etl_schd_run_id']
    etl_btch_run_id = param_dict['jp_etl_btch_run_id']
    etl_sbjct_area_run_id = param_dict['jp_etl_sbjct_area_run_id']
    etl_prcs_run_id = param_dict['jp_etl_prcs_run_id']
    prcs_name = param_dict['jp_etl_prcs_name']
    etl_prcs_id = param_dict['jp_etl_prcs_id']
    cdc_lower_dtm = param_dict['jp_CDCLowDtm']
    cdc_upper_dtm = param_dict['jp_CDCUprDtm'] 
    load_type = param_dict['jp_LoadType']
    if load_type.upper() == 'ADHOC':
        cdc_lower_dtm = param_dict['jp_CDCLowDtmAdhoc']
    elif load_type.upper() == 'FULL':
        cdc_lower_dtm = '1950-01-01 00:00:00'
    try:
        with connection.cursor() as cursor:
            sig_file_name = prcs_name + '.sig'
            sql = "INSERT INTO ETL_PRCS_RUN(ETL_PRCS_RUN_ID,ETL_PRCS_ID,ETL_SCHD_RUN_ID,ETL_BTCH_RUN_ID,ETL_SBJCT_AREA_RUN_ID,STRT_DTM,RUN_STS_CD,RUN_STS_DESCR," \
                      "SIG_FILE_NAME,CRT_DTM,CRT_USER_ID,CDC_LOW_DTM,CDC_UPR_DTM) VALUES " \
                      "(" + etl_prcs_run_id + "," + etl_prcs_id + "," + etl_schd_run_id + "," + etl_btch_run_id + "," + etl_sbjct_area_run_id + ",'" + ct_dt_str + "',0," \
                      "'Running','" + sig_file_name + "','" + ct_dt_str + "','emruser','"+cdc_lower_dtm+"','"+cdc_upper_dtm+"')"
            cursor.execute(sql)
            connection.commit()
            close_rds_con(connection)
        print("Inserted entry in ETL_PRCS_RUN with ETL_PRCS_RUN_ID as {} and STATUS as Running for process {}".format(etl_prcs_run_id, etl_prcs_id))
    except Exception as err:
        raise Exception(err)
    finally:
        close_rds_con(connection)

## Function to update a successfull run in ABC for ETL_PRCS_RUN Table ##
def update_abc_success(etl_prcs_run_id,etl_prcs_id,prcs_name):
    ct_dt = datetime.now()
    ct_str = ct_dt.strftime('%Y-%m-%d %H:%M:%S')
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_PRCS_RUN_ID, ETL_PRCS_ID, RUN_STS_CD FROM ETL_PRCS_RUN " \
                  "WHERE ETL_PRCS_RUN_ID = '" + etl_prcs_run_id + "' AND ETL_PRCS_ID = '" + etl_prcs_id + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) > 1:
                raise Exception("More than one entry for ETL_PRCS_RUN_ID : {}".format(etl_prcs_run_id))
            if len(res_set) < 1:
                raise Exception("ETL_PRCS_RUN_ID : {} doesn't exist".format(etl_prcs_run_id))
            if int(res_set[0][2]) == 1:
                raise Exception("Process already completed for ETL_PRCS_RUN_ID : {}".format(etl_prcs_run_id))
            sql = "UPDATE ETL_PRCS_RUN " \
                  "SET RUN_STS_DESCR = 'Finished', END_DTM = '" + ct_str + "', RUN_STS_CD = " + '1' + ", UPDT_DTM = '" + ct_str + "', UPDT_USER_ID = 'emruser'" + " WHERE ETL_PRCS_RUN_ID = '" + etl_prcs_run_id + "' AND ETL_PRCS_ID = '" + etl_prcs_id + "'"
            cursor.execute(sql)
            connection.commit()
            close_rds_con(connection)
        print("All operations for process {} completed Successfully!".format(prcs_name))
        print("Updated STATUS as Finished in ETL_PRCS_RUN for ETL_PRCS_RUN_ID {} for process {}".format(etl_prcs_run_id, etl_prcs_id))
    except Exception as err:
        raise Exception(err)


## Function to update a failed run in ABC for ETL_PRCS_RUN Table ##
def update_abc_failure(etl_prcs_run_id, etl_prcs_id):
    ct_dt = datetime.now()
    ct_str = ct_dt.strftime('%Y-%m-%d %H:%M:%S')
    try:
        connection = get_rds_con(hub)
        with connection.cursor() as cursor:
            sql = "SELECT ETL_PRCS_RUN_ID, ETL_PRCS_ID, RUN_STS_CD FROM ETL_PRCS_RUN" \
                  " WHERE ETL_PRCS_RUN_ID = '" + etl_prcs_run_id + "' AND ETL_PRCS_ID = '" + etl_prcs_id + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            sql = "UPDATE ETL_PRCS_RUN " \
                  "SET RUN_STS_DESCR = 'Aborted', END_DTM = '" + ct_str + "', RUN_STS_CD = " + '-1' + ", UPDT_DTM = '" + ct_str + "', UPDT_USER_ID = 'emruser'" + " WHERE ETL_PRCS_RUN_ID = '" + etl_prcs_run_id + "' AND ETL_PRCS_ID = '" + etl_prcs_id + "'"
            cursor.execute(sql)
        connection.commit()
        close_rds_con(connection)
        print("Updated STATUS as Aborted in ETL_PRCS_RUN for ETL_PRCS_RUN_ID {} for process {}".format(etl_prcs_run_id,etl_prcs_id))
    except Exception as err:
        raise Exception(err)

## Main Function ##
if __name__ == '__main__':
    m = Manager()
    q = m.Queue()
    df,prcs_name,etl_prcs_run_id,etl_prcs_id = generate_param_file()
    write_param_file(df,prcs_name)
    job_param = set_job_parms(prcs_name)
    insert_abc_entry()
    partition_list,job_id,region,prcs_name,schd_name,btch_name,sbjct_area_name=get_partitions()

    for i in range(len(partition_list)):
        sql_var=partition_list[i]
        part_val=partition_list[i]
        Process(target=job_submit, args=(i,sql_var,part_val,job_id,region,prcs_name,schd_name,btch_name,sbjct_area_name,q,)).start()
    
    arg_list = sys.argv
    if len(arg_list) == 8:
        num_threads=int(sys.argv[7])
    elif len(arg_list) == 7:
        num_threads = 3
    else:
        print("Incorrect number of arguments")

    p = Pool(num_threads, init_worker)
    
    readers = []

    for i in range(len(partition_list)):
        readers.append(p.apply_async(job_exec, (i,q,)))
    
    [r.get() for r in readers]

    failed_part = check_job_status(etl_prcs_run_id,job_id)
    if failed_part != 0:
       update_abc_failure(etl_prcs_run_id,etl_prcs_id)
    else:
       update_abc_success(etl_prcs_run_id,etl_prcs_id,prcs_name)
