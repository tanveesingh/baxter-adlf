import boto3
import psycopg2
import hub_config as hub
import datetime as datetime
import pandas as pd
from io import StringIO
import random
from hub_app.func_utils import get_exception


log = None
job_param= {}
part_val = None

'''
Description : Function to connect RDS
'''
def get_rds_con(self):
    try:
        rds = boto3.client('rds', region_name=self.rds_region)
        password = rds.generate_db_auth_token(DBHostname=self.rds_host, Port=self.rds_port, DBUsername=self.rds_user,
                                              Region=self.rds_region)
        conn = psycopg2.connect(user=self.rds_user, password=password, host=self.rds_host, database=self.rds_database,
                                options=f'-c search_path={self.rds_schema}')
        return conn

    except:
        e = get_exception()
        return e


'''
Description : Function to close RDS connection
'''
def close_rds_con(conn):
    if conn is not None:
        conn.close()



'''
Description : Function to retrieve Schedule Id for a given Schedule and Region
'''
def get_schd_id(schd_name, region):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_SCHD_ID FROM ETL_SCHD WHERE ETL_SCHD_NAME = '" + schd_name + "' AND RGN_CD = '" + region + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) != 1:
                    e = "Schedule does not exist or it has more than one entry for Schedule Name : {} and Region : {}".format(schd_name,region)

                    raise Exception(e)
            else:
                    etl_schd_id = str(res_set[0][0])
            return etl_schd_id,None
    except:
        e = get_exception()
        return None,e

'''
Description : Function to retrieve Batch Id for a given Batch and Region
'''
def get_btch_id(btch_name, region):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_BTCH_ID FROM ETL_BTCH WHERE ETL_BTCH_NAME = '" + btch_name + "' AND RGN_CD = '" + region + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) != 1:
                    e = "Batch does not exist or it has more than one entry for Batch Name : {} and Region : {}".format(btch_name,region)
                    raise Exception(e)
            else:
                    etl_btch_id = str(res_set[0][0])
            return etl_btch_id,None
    except:
        e = get_exception()
        return None,e

'''
Description : Function to retrieve Subject Area Id for a given Subject Area and Region
'''
def get_sbjct_area_id(sbjct_area_name, region):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_SBJCT_AREA_ID FROM ETL_SBJCT_AREA WHERE ETL_SBJCT_AREA_NAME = '" + sbjct_area_name + "' AND RGN_CD = '" + region + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) != 1:
                    e = "Subject Area does not exist or it has more than one entry for Subject Area Name : {} and Region : {}".format(sbjct_area_name,region)
                    raise Exception(e)
            else:
                    etl_sbjct_area_id = str(res_set[0][0])
            return etl_sbjct_area_id,None
    except:
        e = get_exception()
        return None,e


'''
Description : Function to retrieve Process Id for a given Process and Region
'''
def get_prcs_id(prcs_name, region):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_PRCS_ID FROM ETL_PRCS WHERE ETL_PRCS_NAME = '" + prcs_name + "' AND RGN_CD = '" + region + "' "
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) != 1:
                    e = "Process does not exist or it has more than one entry for Process Name : {} and Region : {}".format(prcs_name,region)
                    raise Exception(e)
            else:
                    etl_prcs_id = str(res_set[0][0])
            return etl_prcs_id,None
    except:
        e = get_exception()
        return None,e

'''
Description : Function to retrieve Job Id for a given Job and Region
'''
def get_job_id(prcs_name, region):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_JOB_ID FROM ETL_JOB WHERE ETL_PRCS_ID = (SELECT ETL_PRCS_ID FROM ETL_PRCS WHERE ETL_PRCS_NAME = '" + prcs_name + "' AND RGN_CD = '" + region + "')"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) != 1:
                    e = "Job does not exist or it has more than one entry for Process Name : {} and Region : {}".format(prcs_name,region)
                    raise Exception(e)
            else:
                    etl_job_id = str(res_set[0][0])
            return etl_job_id,None
    except:
        e = get_exception()
        return None,e


'''
Description : Function to retrieve Run Ids for a given Schedule, Batch and Subject Area
'''
def get_run_ids(etl_schd_id,etl_btch_id,etl_sbjct_area_id):
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql1 = "SELECT ETL_SCHD_RUN_ID FROM ETL_SCHD_RUN WHERE ETL_SCHD_ID =" + etl_schd_id + " and END_DTM IS NULL AND CUR_IND = 'Y'"
            cursor.execute(sql1)
            res_set = cursor.fetchall()
            if len(res_set) > 1:
                e = "More than one Schedule is RUNNING for Schedule Id : {}".format(etl_schd_id)
                raise Exception(e)
            elif len(res_set) < 1:
                log.warn("No Schedule is currently RUNNING for Schedule Id : {}. Starting Adhoc Process Execution!!".format(etl_schd_id))
                etl_schd_run_id = '99999'
            else:
                etl_schd_run_id = str(res_set[0][0])
            sql2 = "SELECT ETL_BTCH_RUN_ID FROM ETL_BTCH_RUN WHERE ETL_BTCH_ID =" + etl_btch_id + " and ETL_SCHD_ID ='" + etl_schd_id + "' and END_DTM IS NULL AND CUR_IND = 'Y'"
            cursor.execute(sql2)
            res_set = cursor.fetchall()
            if len(res_set) > 1:
                e = "More than one Batch is RUNNING for Schedule Id : {} and Batch Id :{}".format(etl_schd_id,etl_btch_id)
                raise Exception(e)
            elif len(res_set) < 1:
                log.warn("No Batch is currently RUNNING for Schedule Id : {} and Batch Id : {}. Starting Adhoc Process Execution!!".format(etl_schd_id,etl_btch_id))
                etl_btch_run_id = '99999'
            else:
                etl_btch_run_id = str(res_set[0][0])
            sql3 = "SELECT ETL_SBJCT_AREA_RUN_ID FROM ETL_SBJCT_AREA_RUN WHERE ETL_SBJCT_AREA_ID =" + etl_sbjct_area_id + " and ETL_BTCH_ID ='" + etl_btch_id + "' and ETL_SCHD_ID ='" + etl_schd_id + "' and END_DTM IS NULL AND CUR_IND = 'Y'"
            cursor.execute(sql3)
            res_set = cursor.fetchall()
            if len(res_set) > 1:
                e = "More than one Subject Area is RUNNING for Schedule Id : {} , Batch Id : {} and Subject Area Id : {}".format(etl_schd_id,etl_btch_id,etl_sbjct_area_id)
                raise Exception(e)
            elif len(res_set) < 1:
                log.warn("No Subject Area is currently RUNNING for Schedule Id : {} , Batch Id : {} and Subject Area Id : {}. Starting Adhoc Process Execution!!".format(etl_schd_id,etl_btch_id,etl_sbjct_area_id))
                etl_sbjct_area_run_id = '99999'
            else:
                etl_sbjct_area_run_id = str(res_set[0][0])
        return etl_schd_run_id,etl_btch_run_id,etl_sbjct_area_run_id,None
    except:
        e = get_exception()
        return None,None,None,e


'''
Description : Function to check if the Process is Already Running
'''
def check_status(prcs_name,schd_name,btch_name,sbjct_area_name,region,part_val):
    connection = get_rds_con(hub)
    etl_prcs_id,error = get_prcs_id(prcs_name, region)
    etl_schd_id,error = get_schd_id(schd_name, region)
    etl_btch_id,error = get_btch_id(btch_name, region)
    etl_sbjct_area_id,error = get_sbjct_area_id(sbjct_area_name, region)
    etl_schd_run_id, etl_btch_run_id, etl_sbjct_area_run_id,error = get_run_ids(etl_schd_id, etl_btch_id, etl_sbjct_area_id)
    try:
      if part_val == None:
        with connection.cursor() as cursor:
            sql = "SELECT COUNT(*) FROM ETL_PRCS_RUN WHERE ETL_PRCS_ID = '" + etl_prcs_id +"' AND RUN_STS_CD = 0"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if res_set[0][0] > 0:
                e = 'Process is already running'
                raise Exception(e)
            else:
                pass
    except:
        e = get_exception()
        return e


'''
Description : Function to generate Parameter Values
'''
def generate_param_file(prcs_name,schd_name,btch_name,sbjct_area_name,region,o_date,reporting_code):
   connection = get_rds_con(hub)
   ct_dt = datetime.datetime.now()
   etl_prcs_id,error = get_prcs_id(prcs_name,region)
   etl_schd_id,error = get_schd_id(schd_name, region)
   etl_btch_id,error = get_btch_id(btch_name, region)
   etl_sbjct_area_id,error = get_sbjct_area_id(sbjct_area_name, region)
   etl_job_id,error = get_job_id(prcs_name, region)
   ct_str = ct_dt.strftime('%Y%m%d%H%M%S')
   etl_schd_run_id,etl_btch_run_id,etl_sbjct_area_run_id,error = get_run_ids(etl_schd_id,etl_btch_id,etl_sbjct_area_id)
   log.info('ETL_SCHD_RUN_ID: {}'.format(etl_schd_run_id))
   log.info('ETL_BTCH_RUN_ID: {}'.format(etl_btch_run_id))
   log.info('ETL_SBJT_AREA_RUN_ID: {}'.format(etl_sbjct_area_run_id))
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
                  "(SELECT CAST(nextval('ETL_PRCS_RUN_SEQ') AS VARCHAR(20))) AS parm_val " \
                  "UNION " \
                  "SELECT CAST('jp_etl_job_run_id' AS VARCHAR(20)) AS parm_name, "\
                  "(SELECT CAST(nextval('ETL_JOB_RUN_SEQ') AS VARCHAR(20))) AS parm_val"


       prcs_param_df = pd.read_sql_query(sql, connection)
       etl_prcs_run_id = str(prcs_param_df.query("parm_name=='jp_etl_prcs_run_id'")['parm_val'].iloc[0])
       log.info('ETL_PRCS_RUN_ID: {}'.format(etl_prcs_run_id))
       etl_job_run_id = str(prcs_param_df.query("parm_name=='jp_etl_job_run_id'")['parm_val'].iloc[0])
       log.info('ETL_JOB_RUN_ID: {}'.format(etl_job_run_id))
       CDC_Flag = 'jp_CDCLowDtm' in prcs_param_df.parm_name.values
       CDC_UPR_FLAG = 'jp_CDCUprDtm' in prcs_param_df.parm_name.values
       Load_Flag = 'jp_LoadType' in prcs_param_df.parm_name.values
       Process_Flag = 'jp_EtlPrcsDt' in prcs_param_df.parm_name.values
       DB_Flag = 'jp_DBName' in prcs_param_df.parm_name.values
       dict = {}
       if CDC_Flag != True :
                dict["jp_CDCLowDtm"] = '1950-01-01 00:00:00'
       if CDC_UPR_FLAG != True :
                current_timestamp = datetime.datetime.now()
                cdc_upper_dtm = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                dict["jp_CDCUprDtm"] = cdc_upper_dtm
       if Load_Flag != True :
                dict["jp_LoadType"] = 'INCREMENTAL'
       if Process_Flag != True:
           dict["jp_EtlPrcsDt"] = '1950-01-01 00:00:00'
       if DB_Flag != True:
          dict["jp_DBName"] = ''
       
       dict["jp_etl_schd_run_id"] = etl_schd_run_id
       dict["jp_etl_btch_run_id"] = etl_btch_run_id
       dict["jp_etl_job_name"] = prcs_name
       dict["jp_ScheduleName"] = schd_name
       dict["jp_Region"] = region
       dict["jp_etl_sbjct_area_run_id"] = etl_sbjct_area_run_id
       dict["jp_etl_prcs_name"] = prcs_name
       dict["jp_etl_prcs_id"] = etl_prcs_id
       dict["jp_etl_job_id"] = etl_job_id
       dict["jp_odate"] = o_date
       dict["jp_RPTG_TM_PRD_CD"] = reporting_code
       df = pd.DataFrame(list(dict.items()), columns=['parm_name', 'parm_val'])
       prcs_param_df = prcs_param_df.append(df, ignore_index=True)
       log.info("Param file generated for process {} for {} region".format(prcs_name, region))
       log.info("\n"+str(prcs_param_df))
       return prcs_param_df,etl_prcs_run_id,etl_prcs_id,etl_job_id,None
   except:
       e = get_exception()
       return None,None,None,None,e


'''
Description : Function to Write the Parameter Values in a file in S3
'''
def write_param_file(df,prcs_name,region):
   try:
       s3 = boto3.resource('s3')
       key = "abc/param_file/" + prcs_name +'_'+ region + ".parm"
       csv_buffer = StringIO()
       df.to_csv(csv_buffer, sep="=", index=False, header=False)
       s3.Object(hub.config_bucket_name, key).put(Body=csv_buffer.getvalue())
       log.info("Successfully uploaded Param file for {} to S3".format(prcs_name + ".parm"))
   except:
       e = get_exception()
       return e

'''
Description : Function to set the parameter values for the job from the parameter file into a dictionary 
'''
def set_job_parms(prcs_name,region):
        try:
            key = "abc/param_file/" + prcs_name +'_'+ region + ".parm"
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
            return eval(json_param),None
        except:
            e = get_exception()
            return None, e


'''
Description : Function to make an entry in ABC for ETL_PRCS_RUN Table
'''
def insert_abc_entry(job_param):
    ct_dt = datetime.datetime.now()
    ct_dt_str = ct_dt.strftime('%Y-%m-%d %H:%M:%S')
    connection = get_rds_con(hub)
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
        log.info("Inserted entry in ETL_PRCS_RUN with ETL_PRCS_RUN_ID as {} and STATUS as Running for process {}".format(etl_prcs_run_id, etl_prcs_id))
    except:
        e = get_exception()
        return e
    finally:
        close_rds_con(connection)


'''
Description : Function to make an entry in ABC for ETL_JOB_RUN Table
'''
def insert_abc_job_entry(job_param,part_val):
    ct_dt = datetime.datetime.now()
    ct_dt_str = ct_dt.strftime('%Y-%m-%d %H:%M:%S')
    ct_str = ct_dt.strftime('%Y%m%d%H%M%S')
    param_dict = {key.encode().decode('utf-8'): value.encode().decode('utf-8') for (key, value) in job_param.items()}
    etl_prcs_id = param_dict['jp_etl_prcs_id']
    etl_job_id = param_dict["jp_etl_job_id"]
    etl_prcs_run_id = param_dict["jp_etl_prcs_run_id"]
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            if part_val != None:
                sql_job_run_id ="(SELECT nextval('ETL_JOB_RUN_SEQ'))"
                cursor.execute(sql_job_run_id)
                res_set = cursor.fetchall()
                etl_job_run_id = str(res_set[0][0])
                print("ETL_JOB_RUN_ID : ",etl_job_run_id)
                sql = "INSERT INTO ETL_JOB_RUN(ETL_JOB_RUN_ID,ETL_JOB_ID,ETL_PRCS_RUN_ID,PARTN_VAL,STRT_DTM,RUN_STS_CD,RUN_STS_DESCR," \
                      "CRT_DTM,CRT_USER_ID) VALUES " \
                      "(" + etl_job_run_id + "," + etl_job_id + "," + etl_prcs_run_id + ",'" + part_val + "','" + ct_dt_str + "',0," \
                      "'Running','" + ct_dt_str + "','emruser')"
                cursor.execute(sql)
                connection.commit()

            else:
                etl_job_run_id = param_dict["jp_etl_job_run_id"]
                sql = "INSERT INTO ETL_JOB_RUN(ETL_JOB_RUN_ID,ETL_JOB_ID,ETL_PRCS_RUN_ID,PARTN_VAL,STRT_DTM,RUN_STS_CD,RUN_STS_DESCR," \
                      "CRT_DTM,CRT_USER_ID) VALUES " \
                      "(" + etl_job_run_id + "," + etl_job_id + "," + etl_prcs_run_id + ",'" + 'Non-Partitioned' + "','" + ct_dt_str + "',0," \
                      "'Running','" + ct_dt_str + "','emruser')"
                cursor.execute(sql)
                connection.commit()

            log.info("Inserted entry in ETL_JOB_RUN with ETL_JOB_RUN_ID as {} and STATUS as Running for job {}".format(etl_job_run_id,etl_job_id))
            return etl_job_run_id,None
    except:
        e = get_exception()
        return None,e
    finally:
        close_rds_con(connection)


'''
Description : Function to update a successfull run in ABC for ETL_PRCS_RUN Table
'''
def update_abc_success(etl_prcs_run_id,etl_prcs_id,prcs_name):
    ct_dt = datetime.datetime.now()
    ct_str = ct_dt.strftime('%Y-%m-%d %H:%M:%S')
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_PRCS_RUN_ID, ETL_PRCS_ID, RUN_STS_CD FROM ETL_PRCS_RUN " \
                  "WHERE ETL_PRCS_RUN_ID = '" + etl_prcs_run_id + "' AND ETL_PRCS_ID = '" + etl_prcs_id + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) > 1:
                e = "More than one entry for ETL_PRCS_RUN_ID : {}".format(etl_prcs_run_id)
                raise Exception(e)
            if len(res_set) < 1:
                e = "ETL_PRCS_RUN_ID : {} doesn't exist".format(etl_prcs_run_id)
                raise Exception(e)
            if int(res_set[0][2]) == 1:
                e = "Process already completed for ETL_PRCS_RUN_ID : {}".format(etl_prcs_run_id)
                raise Exception(e)
            sql = "UPDATE ETL_PRCS_RUN " \
                  "SET RUN_STS_DESCR = 'Finished', END_DTM = '" + ct_str + "', RUN_STS_CD = " + '1' + ", UPDT_DTM = '" + ct_str + "', UPDT_USER_ID = 'emruser'" + " WHERE ETL_PRCS_RUN_ID = '" + etl_prcs_run_id + "' AND ETL_PRCS_ID = '" + etl_prcs_id + "'"
            cursor.execute(sql)
            connection.commit()
            close_rds_con(connection)
        log.info("Updated STATUS as Finished in ETL_PRCS_RUN for ETL_PRCS_RUN_ID {} for process {}".format(etl_prcs_run_id, etl_prcs_id))
        log.info("All operations for process {} completed Successfully!".format(prcs_name))
        
    except Exception as e:
        raise Exception(e)


'''
Description : Function to update a successfull run in ABC for ETL_JOB_RUN Table
'''
def update_abc_job_success(etl_job_run_id,etl_job_id):
    ct_dt = datetime.datetime.now()
    ct_str = ct_dt.strftime('%Y-%m-%d %H:%M:%S')
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_JOB_RUN_ID, ETL_JOB_ID, RUN_STS_CD FROM ETL_JOB_RUN " \
                  "WHERE ETL_JOB_RUN_ID = '" + etl_job_run_id + "' AND ETL_JOB_ID = '" + etl_job_id + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) > 1:
                e = "More than one entry for ETL_JOB_RUN_ID : {}".format(etl_job_run_id)
                raise Exception(e)
            if len(res_set) < 1:
                e = "ETL_JOB_RUN_ID : {} doesn't exist".format(etl_job_run_id)
                raise Exception(e)
            if int(res_set[0][2]) == 1:
                e = "Job already completed for ETL_JOB_RUN_ID : {}".format(etl_job_run_id)
                raise Exception(e)
            sql = "UPDATE ETL_JOB_RUN " \
                  "SET RUN_STS_DESCR = 'Finished', END_DTM = '" + ct_str + "', RUN_STS_CD = " + '1' + ", UPDT_DTM = '" + ct_str + "', UPDT_USER_ID = 'emruser'" + " WHERE ETL_JOB_RUN_ID = '" + etl_job_run_id + "' AND ETL_JOB_ID = '" + etl_job_id + "'"
            cursor.execute(sql)
            connection.commit()
            close_rds_con(connection)
        log.info("Updated STATUS as Finished in ETL_JOB_RUN for ETL_JOB_RUN_ID {} for job {}".format(etl_job_run_id,                                                                                                etl_job_id))
    except Exception as e:
        raise Exception(e)

'''
Description : Function to update a Failure run in ABC for ETL_PRCS_RUN Table
'''
def update_abc_failure(etl_prcs_run_id, prcs_id):
    ct_dt = datetime.datetime.now()
    ct_str = ct_dt.strftime('%Y-%m-%d %H:%M:%S')
    try:
        connection = get_rds_con(hub)
        with connection.cursor() as cursor:
            sql = "SELECT ETL_PRCS_RUN_ID, ETL_PRCS_ID, RUN_STS_CD FROM ETL_PRCS_RUN" \
                  " WHERE ETL_PRCS_RUN_ID = '" + etl_prcs_run_id + "' AND ETL_PRCS_ID = '" + prcs_id + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            sql = "UPDATE ETL_PRCS_RUN " \
                  "SET RUN_STS_DESCR = 'Aborted', END_DTM = '" + ct_str + "', RUN_STS_CD = " + '-1' + ", UPDT_DTM = '" + ct_str + "', UPDT_USER_ID = 'emruser'" + " WHERE ETL_PRCS_RUN_ID = '" + str(
                  etl_prcs_run_id) + "' AND ETL_PRCS_ID = '" + str(prcs_id) + "'"
            cursor.execute(sql)
        connection.commit()
        log.info("Updated STATUS as Aborted in ETL_PRCS_RUN for ETL_PRCS_RUN_ID {} for process {}".format(etl_prcs_run_id,prcs_id))
    except Exception as e:
        raise Exception(e)
    finally:
        close_rds_con(connection)

'''
Description : Function to update a Failure run in ABC for ETL_JOB_RUN Table
'''
def update_abc_job_failure(etl_job_run_id, etl_job_id):
    ct_dt = datetime.datetime.now()
    ct_str = ct_dt.strftime('%Y-%m-%d %H:%M:%S')
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_JOB_RUN_ID, ETL_JOB_ID, RUN_STS_CD FROM ETL_JOB_RUN WHERE ETL_JOB_RUN_ID = '" + etl_job_run_id + "' AND ETL_JOB_ID = '" + etl_job_id +"'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            sql = "UPDATE ETL_JOB_RUN " \
                  "SET RUN_STS_DESCR = 'Aborted', END_DTM = '" + ct_str + "', RUN_STS_CD = " + '-1' + ", UPDT_DTM = '" + ct_str + "', UPDT_USER_ID = 'emruser'" + " WHERE ETL_JOB_RUN_ID = '" + etl_job_run_id + "' AND ETL_JOB_ID = '" + etl_job_id + "'"
            cursor.execute(sql)
        connection.commit()
        log.info("Updated STATUS as Aborted in ETL_JOB_RUN for ETL_JOB_RUN_ID {} for job {}".format(etl_job_run_id,etl_job_id))
    except Exception as e:
        raise Exception(e)
    finally:
        close_rds_con(connection)


'''
16-Feb-2021:
Description : New Function created to update a skipped pipeline run in ABC for ETL_PRCS_RUN Table
Keep the cdc_lower_dtm and cdc_upper_dtm entries as max(cdc_upper_dtm) of last successful run
'''
def update_abc_skipping(etl_prcs_run_id,etl_prcs_id,prcs_name):
    ct_dt = datetime.datetime.now()
    ct_str = ct_dt.strftime('%Y-%m-%d %H:%M:%S')
    connection = get_rds_con(hub)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ETL_PRCS_RUN_ID, ETL_PRCS_ID, RUN_STS_CD FROM ETL_PRCS_RUN " \
                  "WHERE ETL_PRCS_RUN_ID = '" + etl_prcs_run_id + "' AND ETL_PRCS_ID = '" + etl_prcs_id + "'"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            if len(res_set) > 1:
                e = "More than one entry for ETL_PRCS_RUN_ID : {}".format(etl_prcs_run_id)
                raise Exception(e)
            if len(res_set) < 1:
                e = "ETL_PRCS_RUN_ID : {} doesn't exist".format(etl_prcs_run_id)
                raise Exception(e)
            if int(res_set[0][2]) == 1:
                e = "Process already completed for ETL_PRCS_RUN_ID : {}".format(etl_prcs_run_id)
                raise Exception(e)
            sql = "select to_char(max(cdc_upr_dtm),'YYYY-MM-DD HH24:MI:SS') from etl_prcs_run where etl_prcs_id = " + etl_prcs_id + " and RUN_STS_CD = 1"
            cursor.execute(sql)
            res_set = cursor.fetchall()
            cdc_upper_dtm = res_set[0][0]
            
            sql = "UPDATE ETL_PRCS_RUN " \
                  "SET RUN_STS_DESCR = 'Finished', END_DTM = '" + ct_str + "', RUN_STS_CD = " + '1' + ", UPDT_DTM = '" + ct_str + "', UPDT_USER_ID = 'emruser' , CDC_LOW_DTM ='" + cdc_upper_dtm  + "', CDC_UPR_DTM ='" + cdc_upper_dtm + "' WHERE ETL_PRCS_RUN_ID = " + str(etl_prcs_run_id) + " AND ETL_PRCS_ID = " + str(etl_prcs_id)
            cursor.execute(sql)
            connection.commit()
            close_rds_con(connection)
        log.info("Updated STATUS as Finished in ETL_PRCS_RUN for ETL_PRCS_RUN_ID {} for process {}".format(etl_prcs_run_id, etl_prcs_id))
        log.info("All operations for process {} completed Successfully!".format(prcs_name))
        
    except Exception as e:
        raise Exception(e)
