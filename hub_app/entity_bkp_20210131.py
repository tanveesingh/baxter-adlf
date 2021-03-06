from collections import OrderedDict
from pyspark.sql.types import *
import psycopg2
import os
import pandas as pd
import subprocess
import datetime
import boto3
from datetime import datetime
import hub_config as hub
import hub_app.abc_util as abc
import time
#pd.set_option('display.max_colwidth', -1)

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

def close_rds_con(conn):
    if conn is not None:
        conn.close()

def do_imports(region, etl_prcs_name, timestamp, part_val):
    global spark
    from hub_app.spoke.spark import get_session
    spark = get_session()
    global get_exception, errorify, handle_exception
    from hub_app.func_utils import get_exception, errorify, handle_exception
    global func
    import hub_app.func_utils as func
    global Operation
    from hub_app.operation import Operation
    global sop
    import hub_app.snowflake_operations as sop
    global error_vld
    import hub_app.error_check as error_vld
    global abc
    import hub_app.abc_util as abc
    global log
    from hub_app.log import setup_custom_logger
    log = setup_custom_logger(__name__, region, etl_prcs_name, timestamp, part_val)
    log.info("spark_application_id:<{}>".format(spark.sparkContext.applicationId))


class Entity:
    def __init__(self, **kwargs):
        self.job_id = None
        self.job_run_id = None
        self.f_job_config_table = None
        self.f_op_config_table = None
        self.sf_account = None
        self.sf_url = None
        self.sf_database = None
        self.sf_username = None
        self.sf_private_key_path = None
        self.sf_role = None
        self.sf_warehouse = None
        self.sf_passphrase = None
        self.application_id_path = None
        self.sf_schema = None
        self.rds_host = None
        self.rds_user = None
        self.rds_region = None
        self.rds_port = None
        self.rds_database = None
        self.rds_schema = None
        self.f_cfg_format = "csv"
        self.file_names={}
        self.debug = False
        self.take = 10
        self.f_override = None
        self.operations = OrderedDict()
        self.distinct_operations = {}
        self.d_override_dfs = {}
        self.d_ops = {}
        self.dataframes = {}
        self.df_paths = {}
        self.region = None
        self.s3_region = None
        self.bucket_name = None
        self.jde_bucket_name = None
        self.config_bucket_name = None
        self.translate_bucket_name = None
        self.schd_name = None
        self.prcs_name = None
        self.btch_name = None
        self.sbjct_area_name = None
        self.job_param = {}
        self.etl_sbjct_area_run_id = None
        self.etl_schd_run_id = None
        self.etl_btch_run_id = None
        self.etl_prcs_run_id = None
        self.etl_job_run_id = None
        self.sql_vars = ""
        self.part_val = ""
        self.part_run_id = None
        self.etl_prcs_id = None
        self.files_list = []
        self.etl_job_id = None
        self.log_timestamp = None
        self.db_name = None
        self.outbound_bucket_name =  None
        self.o_date = None
        self.reporting_code =  None
        self.folder_name =  None

        self.params_set = self.set_params(**kwargs)
        do_imports(self.region, self.prcs_name, self.log_timestamp, self.part_val)
        
        abc.log = log
        func.log = log
        error_vld.log = log
        sop.log = log

        abc.check_status(self.prcs_name,self.schd_name,self.btch_name,self.sbjct_area_name,self.region,self.part_val)

        if self.part_val is None:
            df,self.etl_prcs_run_id,self.etl_prcs_id,self.etl_job_id, error= abc.generate_param_file(self.prcs_name,self.schd_name,self.btch_name,self.sbjct_area_name,self.region,self.o_date,self.reporting_code)
            abc.write_param_file(df,self.prcs_name,self.region)
            self.job_param, error = abc.set_job_parms(self.prcs_name,self.region)
            abc.insert_abc_entry(self.job_param)
            self.etl_job_run_id,error = abc.insert_abc_job_entry(self.job_param,self.part_val)

            func.etl_prcs_run_id = self.etl_prcs_run_id
            func.prcs_id = self.etl_prcs_id
            func.etl_job_id = self.etl_job_id
            func.etl_job_run_id = self.etl_job_run_id
            func.part_val = self.part_val           
            error_vld.files_list = self.files_list
            
            
        else:
            self.get_prcs_run_id()
            self.job_param, error = abc.set_job_parms(self.prcs_name,self.region)
            self.etl_job_run_id,error = abc.insert_abc_job_entry(self.job_param,self.part_val)
            self.etl_job_id,error = abc.get_job_id(self.prcs_name,self.region)
            self.etl_prcs_id,error = abc.get_prcs_id(self.prcs_name, self.region)

            func.prcs_id = self.etl_prcs_id
            func.etl_job_id = self.etl_job_id
            func.part_val = self.part_val
            func.etl_prcs_run_id = self.etl_prcs_run_id
            func.etl_job_run_id = self.etl_job_run_id
            error_vld.files_list = self.files_list


        if self.params_set:
            if self.job_id and self.f_job_config_table and self.f_op_config_table:
                self.read_config()
            else:
                raise Exception("Config Table not Found")


    def set_params(self, **kwargs):
        try:
            ct_dt = datetime.now()
            ct_dt_str = ct_dt.strftime('%Y%m%d%H%M%S')
            self.job_id = kwargs.get('job_id')
            self.f_job_config_table = kwargs.get('job_config_table')
            self.f_op_config_table = kwargs.get('op_config_table')
            self.sf_account = kwargs.get('sf_account')
            self.sf_url = kwargs.get('sf_url')
            self.sf_database = kwargs.get('sf_database')
            self.sf_username = kwargs.get('sf_username')
            self.sf_private_key_path = kwargs.get('sf_private_key_path')
            self.sf_role = kwargs.get('sf_role')
            self.sf_warehouse = kwargs.get('sf_warehouse')
            self.sf_schema = kwargs.get('sf_schema')
            self.sf_passphrase = kwargs.get('sf_passphrase')
            self.rds_host = kwargs.get('rds_host')
            self.rds_port = kwargs.get('rds_port')
            self.rds_region = kwargs.get('rds_region')
            self.rds_user = kwargs.get('rds_user')
            self.rds_database = kwargs.get('rds_database')
            self.rds_schema = kwargs.get('rds_schema')
            self.f_cfg_format = kwargs.get('config_file_format')
            self.debug = kwargs.get('debug_mode')
            self.take = kwargs.get('num_debug_rows')
            self.f_override = kwargs.get('override_file')
            self.load_job_nr = kwargs.get('load_job_nr')
            self.region = kwargs.get('region')
            if '/' in self.region:
                self.region = self.region.replace('/','_')
            self.s3_region = kwargs.get('region')
            self.bucket_name = hub.bucket_name
            self.jde_bucket_name = hub.jde_bucket_name
            self.config_bucket_name = hub.config_bucket_name
            self.translate_bucket_name = hub.translate_bucket_name
            self.outbound_bucket_name =  hub.outbound_bucket_name
            self.schd_name = kwargs.get('schd_name')
            self.prcs_name = kwargs.get('prcs_name')
            self.btch_name = kwargs.get('btch_name')
            self.sbjct_area_name = kwargs.get('sbjct_area_name')
            self.sql_vars = kwargs.get('sql_vars')
            self.part_val = kwargs.get('part_val')
            self.o_date = kwargs.get('o_date')
            self.reporting_code = kwargs.get('reporting_code')
            self.log_timestamp = ct_dt_str
            if 'job_run_id' in kwargs:
                self.job_run_id = kwargs.get('job_run_id')
            if 'application_id_path' in kwargs:
                self.application_id_path = kwargs.get('application_id_path')
        except:
            return False
        return True


    def get_prcs_run_id(self):
        try:
            s3 = boto3.client('s3')
            key = "abc/param_file/" + self.prcs_name + ".parm"
            read_file = s3.get_object(Bucket=self.config_bucket_name, Key=key)
            col_name = ['param_key', 'param_val']
            df = pd.read_csv(read_file['Body'], sep='=', names=col_name)
            df = df.where(pd.notnull(df), 'NULL')
            print(df)
            json_param = '{'
            for index, row in df.iterrows():
                json_param = json_param + '"' + str(row["param_key"]) + '":"' + str(row["param_val"]) + '",'
            json_param = json_param[0:(len(json_param) - 1)]
            json_param = json_param + '}'
            dynamic_dict = eval(json_param)
            self.etl_prcs_run_id = dynamic_dict['jp_etl_prcs_run_id']
            log.info('ETL PRCS RUN ID : {}'.format(self.etl_prcs_run_id))
        except Exception as err:
            raise Exception(err)

    def run_last_ops(self,**op_tags):
        try:
            if 'run_ops' in op_tags:
                run_ops = op_tags.get('run_ops')
                conn = get_rds_con(self)
                query = "SELECT * FROM "+ self.f_job_config_table+" where job_id  = "+self.job_id+" AND actv_ind='Y' AND opn_id in ("+run_ops+") order by opn_id"
                print(query)
                job_op_df = pd.read_sql(query, conn)
                job_op_df.sort_values(by=['opn_run_ordr_num'], inplace=True)
                close_rds_con(conn)
                self.d_ops = {}
                self.distinct_operations = {}
                self.operations = OrderedDict()
                for rw in job_op_df.index:
                    k = job_op_df['opn_run_ordr_num'.lower()][rw]
                    v = job_op_df['opn_id'.lower()][rw]
                    self.distinct_operations[v] = ''
                    if k in self.operations:
                        l_v = self.operations.get(k)
                        l_v.append(v)
                        self.operations[k] = l_v
                    else:
                        self.operations[k] = [v]
                l_ops = set(self.distinct_operations.keys())
                conn = get_rds_con(self)
                query = "SELECT * FROM " + self.f_op_config_table + " WHERE job_id='" + self.job_id + "' ORDER BY opn_id"
                op_cfg_df = pd.read_sql_query(query, conn)

                op_cfg_df = op_cfg_df[op_cfg_df['opn_id'.lower()].isin(l_ops)]

                op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$bucket',
                                                                                      value=self.bucket_name, regex=True)
                op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$jde-bucket',
                                                                                      value=self.jde_bucket_name,
                                                                                      regex=True)
                op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$translate-bucket',
                                                                                      value=self.translate_bucket_name,
                                                                                      regex=True)
                op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$config-bucket',
                                                                                      value=self.config_bucket_name,
                                                                                      regex=True)
                op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$outbound-bucket',
                                                                                  value=self.outbound_bucket_name,
                                                                                  regex=True)

                op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$region',
                                                                                      value=self.region, regex=True)

                if 'ladta' in self.db_name:
                    self.folder_name = 'LADTA'
                elif 'lactl' in self.db_name:
                    self.folder_name = 'LACTL'
                elif 'apdta' in self.db_name:
                    self.folder_name = 'APDTA'
                elif 'apctl' in self.db_name:
                    self.folder_name = 'APCTL'
                elif 'ladd' in self.db_name:
                    self.folder_name = 'LADD'
                elif 'nactl' in self.db_name:
                    self.folder_name = 'NACTL'
                elif 'nadta' in self.db_name:
                    self.folder_name = 'NADTA'
                elif 'eudta' in self.db_name:
                    self.folder_name = 'EUDTA'
                elif 'eudd' in self.db_name:
                    self.folder_name = 'EUDD'
                elif 'euctl' in self.db_name:
                    self.folder_name = 'EUCTL'
                elif 'etdta' in self.db_name:
                    self.folder_name = 'ETDTA'


                op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$db-name',
                                                                                      value=self.db_name, regex=True)
                op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$folder-name',
                                                                                      value=self.folder_name, regex=True)              
                close_rds_con(conn)
                prev_op_id = ''
                op = None
                for rw in op_cfg_df.index:
                    if op_cfg_df['opn_id'.lower()][rw] != prev_op_id:
                        if op and prev_op_id != '':
                            self.d_ops[prev_op_id] = op

                        op = Operation()
                        op.set_params(op_cfg_df['opn_id'.lower()][rw], op_cfg_df['opn_class_val'.lower()][rw],
                                      op_cfg_df['opn_typ_val'.lower()][rw])

                    op.add_tag(op_cfg_df['opn_tag_descr'.lower()][rw], op_cfg_df['opn_tag_val'.lower()][rw])
                    prev_op_id = op_cfg_df['opn_id'.lower()][rw]
                if op and prev_op_id != '':
                    self.d_ops[prev_op_id] = op
            print(self.d_ops)
        except:
            e = get_exception()


    def read_config(self): 
        """
        Function
        """
        try:

            conn = get_rds_con(self)
            query = "SELECT * FROM " + self.f_job_config_table + " WHERE job_id='" + self.job_id + "' AND actv_ind='Y' \
            ORDER BY opn_run_ordr_num"


            job_op_df = pd.read_sql_query(query, conn)
            close_rds_con(conn)


            for rw in job_op_df.index:
                k = job_op_df['opn_run_ordr_num'.lower()][rw]
                v = job_op_df['opn_id'.lower()][rw]
                self.distinct_operations[v] = ''
                if k in self.operations:
                    l_v = self.operations.get(k)
                    l_v.append(v)
                    self.operations[k] = l_v
                else:
                    self.operations[k] = [v]
            l_ops = set(self.distinct_operations.keys())

            param_dict = {key.encode().decode('utf-8'): value.encode().decode('utf-8') for (key, value) in self.job_param.items()}
            self.db_name = param_dict['jp_DBName']


            conn = get_rds_con(self)
            query = "SELECT * FROM " + self.f_op_config_table + " WHERE job_id=" + str(self.job_id) + " ORDER BY opn_id"
            op_cfg_df = pd.read_sql_query(query, conn)

            op_cfg_df = op_cfg_df[op_cfg_df['opn_id'.lower()].isin(l_ops)]

            op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$bucket',
                                                                                  value=self.bucket_name, regex=True)
            op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$jde-bucket',
                                                                                  value=self.jde_bucket_name,
                                                                                  regex=True)
            op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$translate-bucket',
                                                                                  value=self.translate_bucket_name,
                                                                                  regex=True)
            op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$config-bucket',
                                                                                  value=self.config_bucket_name,
                                                                                  regex=True)

            op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$outbound-bucket',
                                                                                  value=self.outbound_bucket_name,
                                                                                  regex=True)

            op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$region',
                                                                                  value=self.region, regex=True)

            if 'ladta' in self.db_name:
                self.folder_name = 'LADTA'
            elif 'lactl' in self.db_name:
                self.folder_name = 'LACTL'
            elif 'apdta' in self.db_name:
                self.folder_name = 'APDTA'
            elif 'apctl' in self.db_name:
                self.folder_name = 'APCTL'
            elif 'ladd' in self.db_name:
                self.folder_name = 'LADD'
            elif 'nactl' in self.db_name:
                self.folder_name = 'NACTL'
            elif 'nadta' in self.db_name:
                self.folder_name = 'NADTA'
            elif 'eudta' in self.db_name:
                self.folder_name = 'EUDTA'
            elif 'eudd' in self.db_name:
                self.folder_name = 'EUDD'
            elif 'euctl' in self.db_name:
                self.folder_name = 'EUCTL'
            elif 'etdta' in self.db_name:
                self.folder_name = 'ETDTA'

            op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$db-name',
                                                                                  value=self.db_name, regex=True)
            op_cfg_df['opn_tag_val'.lower()] = op_cfg_df['opn_tag_val'.lower()].replace(to_replace='\$folder-name',
                                                                                      value=self.folder_name, regex=True)

            close_rds_con(conn)

            # looping over the operations and converting them to operation objects
            prev_op_id = ''
            op = None
            for rw in op_cfg_df.index:
                if op_cfg_df['opn_id'.lower()][rw] != prev_op_id:
                    if op and prev_op_id != '':
                        self.d_ops[prev_op_id] = op

                    op = Operation()
                    op.set_params(op_cfg_df['opn_id'.lower()][rw], op_cfg_df['opn_class_val'.lower()][rw],
                                  op_cfg_df['opn_typ_val'.lower()][rw])

                op.add_tag(op_cfg_df['opn_tag_descr'.lower()][rw], op_cfg_df['opn_tag_val'.lower()][rw])
                prev_op_id = op_cfg_df['opn_id'.lower()][rw]
            if op and prev_op_id != '':
                self.d_ops[prev_op_id] = op


        except:
            e = get_exception()

    def run_operations(self):
        try:
            status = True
            if not self.operations:
                log.warn("No operations found to run")
                e = "Please make an entry to Operation/Job Config Table"
                return False, e
            for op_order, l_v in self.operations.items():
                if status:
                    for op_id in l_v:
                        if op_id in self.d_ops:
                            op_obj = self.d_ops.get(op_id)

                            if self.debug:
                                log.warn(str(op_obj))
                            res, error = self.run_operation(op_obj)
                            if res and str(error) == 'pass':
                                status = False
                                log.warn("Skipping the whole Pipeline")
                                break
                            if not res:
                                e = "Error encountered during run of operation:<{}>,<{}>".format(op_id,
                                                                                                 op_obj.operation_class)
                                log.error(e)
                                return False, error

                        else:
                            log.warn("Operation <{}> is not defined, skipping".format(op_id))
                            continue
                else:
                    break
            return True, None
        except:
            e = get_exception()
            log.error(e)
            return False, e

    def dataframe_default_operations(self, name, df):
        log.info("{} dataframe , Count:{}".format(name, None))

    def dataframe_debug(self, df_name, df):
        try:
            log.info("Dataframe: <{}> ".format(df_name))
            log.info("Columns: {} ".format(','.join(df.columns)))
            log.info("Datatypes:{}".format(str(df.dtypes)))
            log.info(df.show(self.take, False))
        except:
            e = get_exception()
            log.error(e)
            return

    def add_dataframe(self, name, df):
        try:
            if df and type(df).__name__.endswith('DataFrame'):
                self.dataframes[name] = df
                if self.debug:
                    self.dataframe_debug(name, df)
                #self.dataframe_default_operations(name, df)
                return True
            return False
        except:
            e = get_exception()
            print(e)
            return False

    def add_filename(self,file_name,file_path):
        try:
            self.file_names[file_name]=file_path
            return True
        except:
            e =get_exception()
            return False

    def get_dataframe(self, name):
        if name in self.dataframes:
            return self.dataframes.get(name)
        else:
            return None

    def run_operation(self, op_obj):
        try:
            avl_funcs = dir(func)
            op_id = op_obj.operation_id
            op_class = op_obj.operation_class
            op_type = op_obj.operation_type
            op_tags = op_obj.operation_tags
            log.info('Running operation {},{}'.format(op_id, op_class))
            if op_type == 'entity':
                if op_class == 'register_temp_table':
                    status, error = func.register_temp_table(self.dataframes, **op_tags)
                    if error:
                        log.error(error)
                        return False, error
                    return True, None
                elif op_class == 'run_sql':
                    result_df_name = op_tags.get("result_dataframe")
                    result_df, error = func.run_sql(self.job_param, spark, **op_tags)
                    if 'run_ops' in op_tags and result_df.rdd.isEmpty():
                        self.run_last_ops(**op_tags)
                    if error:
                        return False, error
                    if result_df and type(result_df).__name__.endswith('DataFrame'):
                        self.dataframes[result_df_name] = result_df
                        if self.debug:
                            log.info("columns:" + str(result_df.columns))
                            log.info(result_df.take(self.take))
                    return True, None
                elif op_class =='exec_sf_dml':
                    stats = sop.exec_sf_dml(self.job_param, spark, **op_tags)
                    if stats:
                        return True, None
                    return False,"Error while performing DML Operation"
                elif op_class =='clone_sf_table':
                    stats = sop.clone_sf_table(**op_tags)
                    if stats:
                        return True, None                        
                    return False,"Error while performing Cloning Operation"                
                elif op_class == 'send_mail':
                    if 'dataframe_name' in op_tags:
                        df_name = op_tags.get('dataframe_name')
                        df = self.dataframes.get(df_name)
                        if df.rdd.isEmpty():
                            log.info("Dataframe is empty!")
                            return True, None
                    file_name = op_tags.get('file_name')
                    timestr = time.strftime("%Y%m%d")
                    file_name = file_name+"_"+timestr+".csv"
                    print(file_name)
                    file_path = self.file_names.get(file_name)
                    print(file_path)
                    stat = func.send_mail(file_path, **op_tags)
                    if stat:
                        return True, None
                    return False, "Mail was not sent to the recipient"                                         
                elif op_class == 'read_file':
                    result_df_name = op_tags.get("result_dataframe")
                    tgt_path = op_tags.get('file_path')
                    if 'skip_pipeline' in op_tags:
                        skip_pipeline = op_tags.get('skip_pipeline').strip().upper()
                    else:
                        skip_pipeline = 'TRUE'
                    if 'send_mail' in op_tags:
                        send_mail = op_tags.get('send_mail').strip().upper()
                    else:
                        send_mail = 'FALSE'
                    tgt_path = tgt_path.replace("*", "")
                    file_availability_cmd = "aws s3 ls " + tgt_path + "|wc -l"
                    flag = subprocess.check_output(file_availability_cmd, shell=True)
                    flag = flag.decode('utf-8')
                    if not int(flag) > 0:
                        log.warn("Files are not available in the Path : " + tgt_path)
                        if skip_pipeline == 'TRUE':
                            if send_mail == 'TRUE':
                                func.email_generation("empty_file", **op_tags)
                            if 'run_ops' in op_tags:
                                self.run_last_ops(**op_tags)
                            return True, "pass"
                    result_df, reject_df, error = func.read_file(spark, **op_tags)
                    if error:
                        return False, error
                    if result_df is None:
                        log.info("Zero byte file(s) are present in the location")
                        if skip_pipeline == 'TRUE':
                            if send_mail == 'TRUE':
                                func.email_generation("empty_file", **op_tags)
                            if 'run_ops' in op_tags:
                                self.run_last_ops(**op_tags)
                            return True, "pass"
                        else:
                            return True, None                        
                    if result_df.rdd.isEmpty():
                        log.info("Result DataFrame is Empty")
                        if skip_pipeline == 'TRUE':
                            if send_mail == 'TRUE':
                                func.email_generation("empty_file", **op_tags)
                            if 'run_ops' in op_tags:
                                self.run_last_ops(**op_tags)
                            return True, "pass"                        
                    if result_df:
                        stat = self.add_dataframe(result_df_name, result_df)
                        if not stat:
                            e = "Unable to persist dataframe <{}>".format(result_df_name)
                            log.error(e)
                            return False, e
                    return True, None
                elif op_class == 'read_sf':
                    result_df_name = op_tags.get("result_dataframe")
                    result_df = sop.read_sf(self,self.job_param,spark,**op_tags)
                    stat = self.add_dataframe(result_df_name, result_df)
                    if not stat:
                        e = "Unable to persist dataframe <{}>".format(result_df_name)
                        log.error(e)
                        return False, e
                    else:
                        return True, None
                elif op_class == 'read_sf':
                    result_df_name = op_tags.get("result_dataframe")
                    result_df = sop.read_sf(self, self.job_param, spark, **op_tags)
                    stat = self.add_dataframe(result_df_name, result_df)
                    if not stat:
                        e = "Unable to persist dataframe <{}>".format(result_df_name)
                        log.warn(e)
                        return False, e
                    else:
                        return True, None
                elif op_class == 'update_sf':
                    sop.move_update_snapshot_sf(**op_tags)
                elif op_class == 'cleanup_dir':
                    file_paths = op_tags.get("file_path")
                    delimiter = op_tags.get("delimiter")
                    file_path_list = file_paths.split(delimiter)
                    for file_path in file_path_list:
                        s3_rm_cmd = "aws s3 rm " + file_path + " --recursive --quiet --only-show-errors"
                        os.system(s3_rm_cmd)
                    return True, None
                elif op_class == 's3_file_copy':
                    file_path = op_tags.get("file_path")
                    values = file_path.split("|")
                    src_path = values[0]
                    tgt_path = values[1]
                    file_check_cmd = "aws s3 ls " + tgt_path + "|awk 'NR==1{print $3}'"
                    flag = subprocess.check_output(file_check_cmd, shell=True)
                    if flag == 0:
                        s3_copy_command = "aws s3 sync " + src_path + " " + tgt_path + " --quiet --only-show-errors"
                        os.system(s3_copy_command)
                        status = True
                    else:
                        log.info("File exists, removing existing files")
                        s3_rm_command = "aws s3 rm " + tgt_path + " --recursive --quiet --only-show-errors"
                        os.system(s3_rm_command)
                        s3_copy_command = "aws s3 sync " + src_path + " " + tgt_path + " --quiet --only-show-errors"
                        os.system(s3_copy_command)
                        status = True
                    # if status:
                        #log.info("success")
                    return True, None
                elif op_class == 'update_sf':
                    target_adw_table_name = op_tags.get("target_adw_table_name")
                    conn = get_rds_con(self)
                    status, error = sop.move_update_snapshot_sf(conn, target_adw_table_name, **op_tags)

                    if error:
                        log.error(error)
                        return False, error
                    log.info("success")
                    close_rds_con(conn)
                    return True, None
                elif op_class == 'copy_files':
                    source_path = op_tags.get("source_path")
                    target_path = op_tags.get("target_path")
                    file_format = op_tags.get("file_format")
                    self.files_list, error = func.copy_files(self.job_param, **op_tags)
                    if error:
                        log.error(error)
                        return False, error
                    if len(self.files_list) == 0:
                        return True,"pass"
                    else:
                        return True, None
                elif op_class == 'archive_files':
                    source_path = op_tags.get("source_path")
                    target_path = op_tags.get("target_path")
                    status, error = func.archive_files(self.files_list, **op_tags)
                    if error:
                        log.error(error)
                        return False, error
                    else:
                        return status, None
                elif op_class == 'read_glue':
                    result_df_name = op_tags.get("result_dataframe")
                    result_df, error = func.read_glue(spark, self.job_param, **op_tags)

                    # if result_df.rdd.isEmpty():
                    #     log.warn("Result DataFrame is Empty")
                    #     return True, "pass"

                    if result_df:
                        stat = self.add_dataframe(result_df_name, result_df)
                        if not stat:
                            e = "Unable to persist dataframe <{}>".format(result_df_name)
                            log.warn(e)
                            return False, e
                    return True, None
                elif op_class == 'read_fwf':
                    result_df_name = op_tags.get("result_dataframe")
                    file_path = op_tags.get("file_path")
                    result_df, error = func.read_fwf(spark, **op_tags)

                    if result_df.rdd.isEmpty():
                        log.warn("Result DataFrame is Empty")
                        return True, "pass"

                    if result_df:
                        stat = self.add_dataframe(result_df_name, result_df)
                        if not stat:
                            e = "Unable to persist dataframe <{}>".format(result_df_name)
                            log.warn(e)
                            return False, e
                    return True, None
            elif op_type == 'dataframe':
                df_name = op_tags.get('dataframe_name')
                result_df_name = df_name

                if 'result_dataframe' in op_tags:
                    result_df_name = op_tags.get("result_dataframe")

                if op_class == 'create_dataframe':
                    result_df = spark.create_dataframe(**op_tags)
                    if result_df:
                        stat = self.add_dataframe(result_df_name, result_df)
                        if not stat:
                            e = "Unable to persist dataframe <{}>".format(result_df_name)
                            log.warn(e)
                            return False, e
                elif df_name in self.dataframes:
                    inp_df = self.dataframes.get(df_name)
                    if op_class == 'create_hive_table':
                        status, error = func.create_hive_table(spark, inp_df, **op_tags)
                        if error:
                            log.error(error)
                            return False, error
                        if not status:
                            e = "Create hive table operation was unsuccessful"
                            log.error(e)
                            return False, e
                    elif op_class == 'write_file':
                        status, error = func.write_file(spark, inp_df, **op_tags)
                        if error:
                            log.error(error)
                            return False, error
                        if not status:
                            e = 'Operation write file was unsuccessful.'
                            log.error(e)
                            return False, e
                    elif op_class == 'write_partitions':
                        if inp_df.rdd.isEmpty():
                            status = True
                            log.warn("Input DataFrame is Empty!")
                            return True, None
                        status, error = func.write_partitions(inp_df, **op_tags)
                        if error:
                            log.error(error)
                            return False, error
                        if not status:
                            e = 'Operation write partitions was unsuccessful.'
                            log.error(e)
                            return False, e
                    elif op_class == 'write_sf':
                        status, error = sop.write_sf(spark,self.job_param, inp_df, **op_tags)
                        # if error:
                        #     #log.error(error)
                        #     return False, error
                        if not status:
                            e = 'Operation write sf was unsuccessful.'
                            log.error(e)
                            return False, e
                    elif op_class == 'cleanup_columns':
                        result_df_name = op_tags.get('result_dataframe')
                        result_df,error = func.cleanup_columns(inp_df, **op_tags)
                        if result_df:
                            stat = self.add_dataframe(result_df_name, result_df)
                            return True, None
                        else:
                            e = "Unable to persist dataframe <{}>".format(result_df_name)
                            log.warn(e)
                            return False, e
                    
                    elif op_class == 'rename_columns':
                    	result_df_name = op_tags.get('dataframe_name')
                    	result_df,error = func.rename_columns(inp_df, **op_tags)
                    	if result_df:
                    		stat = self.add_dataframe(result_df_name,result_df)
                    		return True, None
                    	else:
                    		e = "Unable to persist dataframe <{}>".format(result_df_name)
                    		log.warn(e)
                    		return False, e

                    elif op_class == 'dedup_dataset':
                        result_df_name = op_tags.get('result_dataframe')
                        result_df,error = func.dedup_dataset(inp_df, **op_tags)
                        stat = self.add_dataframe(result_df_name, result_df)
                        if result_df:
                            stat = self.add_dataframe(result_df_name, result_df)
                            return True, None
                        else:
                            e = "Unable to persist dataframe <{}>".format(result_df_name)
                            log.warn(e)
                            return False, e
                    elif op_class == 'error_check':
                        result_df_name = op_tags.get('result_dataframe')
                        error_df_name = op_tags.get('error_dataframe_name')
                        result_df, error_df = error_vld.error_check(inp_df, spark, **op_tags)
                        if result_df.rdd.isEmpty():
                            log.warn("Result Dataframe is empty")
                            if 'run_ops' in op_tags:
                                self.run_last_ops(**op_tags)
                            return True, "pass"
                        else:
                            stat = self.add_dataframe(result_df_name, result_df)
                            stat1 = self.add_dataframe(error_df_name, error_df)
                            if not stat1:
                                e = "Unable to persist dataframe <{}>".format(error_df_name)
                                log.error(e)
                                return False, e
                            if not stat:
                                e = "Unable to persist dataframe <{}>".format(result_df_name)
                                log.error(e)
                                return False, e
                            else:
                                return True, None
                    elif op_class == 'generate_hierarchy':
                        result_df_name = op_tags.get('result_dataframe')
                        result_df = func.generate_hierarchy(inp_df, **op_tags)
                        if result_df.rdd.isEmpty():
                            log.warn("Dataframe is Empty!")
                            return True, "pass"
                        else:
                            stat = self.add_dataframe(result_df_name, result_df)
                            if not stat:
                                e = "Unable to persist dataframe <{}>".format(result_df_name)
                                log.error(e)
                                return False, e
                            else:
                                return True, None
                    elif op_class == 'custom_write_file':
                        file_name,file_path = func.custom_write_file(inp_df, spark, **op_tags)
                        stat = self.add_filename(file_name,file_path)
                        if not stat:
                            e = "Unable to persist File Names <{}>".format(file_name)
                            log.error(e)
                            return False, e
                        else:
                            return True, None
                    elif op_class == 'load_sf_table':
                        status, error = sop.load_sf_table(inp_df, self.job_param, **op_tags)
                        if error:
                            log.error(error)
                            return False, error
                        if not status:
                            e = 'Operation write file was unsuccessful.'
                            log.error(e)
                            return False, e
                    elif op_class == 'join_dataset':
                        inp_df = self.dataframes.get(df_name)
                        join_df_name = op_tags.get('join_dataframe_name')

                        if join_df_name in self.dataframes:
                            join_df = self.dataframes.get(join_df_name)
                            result_df, error = func.join_dataset(inp_df, join_df, **op_tags)
                            if error:
                                log.error(error)
                                return False, error
                            if result_df:
                                stat = self.add_dataframe(result_df_name, result_df)
                                if stat:
                                    return True, None
                                else:
                                    e = "Unable to persist dataframe <{}>".format(result_df_name)
                                    log.error(e)
                                    return False, e
                        else:
                            e = "Dataframe: {} was not found in created dataframes.".format(join_df_name)
                            log.error(e)
                            return False, e
                    elif op_class == 'union_dataset':
                        inp_df = self.dataframes.get(op_tags.get('dataframe_name'))
                        union_df_name = self.dataframes.get(op_tags.get('union_dataframe_name'))
                        result_df_name = op_tags.get('result_df_name')

                        if union_df_name:
                            result_df, error = func.union_dataset(inp_df, union_df_name)
                            if error:
                                log.error(error)
                                return False, error
                            if result_df:
                                stat = self.add_dataframe(result_df_name, result_df)
                                if stat:
                                    return True, None
                                else:
                                    e = "Unable to union dataframe <{}>".format(result_df_name)
                                    log.error(e)
                                    return False, e
                        else:
                            e = "Dataframe: {} was not found in created dataframes.".format(union_df_name)
                            log.error("Dataframe: {} was not found in created dataframes.".format(union_df_name))
                            return False, e
                    elif op_class == 'capture_dml_flag':
                        inp_df = self.dataframes.get(op_tags.get('dataframe_name'))
                        result_df_name = op_tags.get('result_dataframe')
                        result_df, error = func.capture_dml_flag(spark, inp_df, **op_tags)
                        if error:
                            log.error(error)
                            return False, error
                        return True, None
                    elif op_class == 'delete_records':
                        inp_df = self.dataframes.get(op_tags.get('dataframe_name'))
                        result_df, error = func.delete_records(spark, inp_df, **op_tags)
                        if error:
                            log.error(error)
                            return False, error
                        return True, None
                    elif op_class == 'merge_dataset':
                        if 'param' in op_tags:
                            param = op_tags.get("param")
                            part_val = self.part_val
                            sql_vars = self.sql_vars
                        else:
                            param = False
                            part_val = self.part_val
                            sql_vars = self.sql_vars
                        inp_df = self.dataframes.get(op_tags.get('dataframe_name'))
                        result_df_name = op_tags.get("result_dataframe")
                        file_path = op_tags.get("target_path")
                        stat, error = func.merge_dataset(spark, inp_df, file_path, part_val, sql_vars, **op_tags)
                        if stat:
                            return True, None
                        else:
                            return False,error
                    elif op_class == 'capture_history':
                        inp_df = self.dataframes.get(op_tags.get('dataframe_name'))
                        current_df = self.dataframes.get(op_tags.get('history_current_dataframe'))
                        expired_df, error = func.capture_history_inactive(spark, self.job_param, inp_df, current_df,
                                                                      **op_tags)
                        if error:
                            log.error(error)
                            return False, error
                        elif True:
                            active_df, error = func.capture_history_active(spark, self.job_param, inp_df, current_df,
                                                                       **op_tags)
                            if error:
                                log.error(error)
                                return False, error
                        return True, None
                    elif op_class == 'merge_multiple_partitions':
                        inp_df = self.dataframes.get(op_tags.get('dataframe_name'))
                        result_df_name = op_tags.get("result_dataframe")
                        file_path = op_tags.get("target_base_path")
                        result_df, error = func.merge_multiple_partitions(spark, inp_df, **op_tags)
                        if error:
                            log.error(error)
                            return False, error
                        return True, None
                    elif op_class == 'add_audit_column':
                        col_list = op_tags.get('col_list')
                        col_value = op_tags.get('col_value')
                        delimiter = op_tags.get('delimiter')
                        result_df, error = func.add_audit_column(self.job_param,inp_df,col_list,col_value,delimiter)
                        if error:
                            log.error(error)
                            return False, error
                        if result_df:
                            stat = self.add_dataframe(result_df_name, result_df)
                            if stat:
                                return True, None
                            else:
                                e = "Unable to persist dataframe <{}>".format(result_df_name)
                                log.error("Unable to persist dataframe <{}>".format(result_df_name))
                                return False, e
                            return True, None
                    elif op_class == 'add_sk':
                        sk_type = op_tags.get("sk_type")
                        if sk_type.upper() == 'MD5':
                            result_df, error = func.add_sk(spark, inp_df, **op_tags)
                            if error:
                                log.error(error)
                                return False, error
                        elif sk_type.upper() == 'CONCAT':
                            result_df, error = func.add_sk_curate(spark, inp_df, **op_tags)
                            if error:
                                log.error(error)
                                return False, error
                        if result_df:
                            stat = self.add_dataframe(result_df_name, result_df)
                            if stat:
                                return True, None
                            else:
                                e = "Unable to persist dataframe <{}>".format(result_df_name)
                                log.error("Unable to persist dataframe <{}>".format(result_df_name))
                                return False, e
                        return True, None
                    elif op_class in avl_funcs:
                        df_function = getattr(func, op_class)
                        inp_df = self.dataframes.get(df_name)
                        result_df, error = df_function(inp_df, **op_tags)
                        if error:
                            log.error(error)
                            return False, error
                        if result_df:
                            stat = self.add_dataframe(result_df_name, result_df)

                            if stat:
                                return True, None
                            else:
                                e = "Unable to persist dataframe <{}>".format(result_df_name)
                                log.error("Unable to persist dataframe <{}>".format(result_df_name))
                                return False, e
                    else:
                        e = errorify(error_code="INVALID_OP_CLASS",
                                     error_message="Operation class <{}> is invalid.".format(op_class))
                        log.error(e)
                        handle_exception()
                        return False, e

                else:
                    e = errorify(error_code="INVALID_DATAFRAME",
                                 error_message="Input dataframe <{}> has not been created before invocation".format(
                                     df_name))
                    log.error(e)
                    handle_exception()
                    return False, e
                log.info('Completed operation {},{}'.format(op_id, op_class))
                return True, None
            else:
                e = errorify(error_code="INVALID_OP_TYPE",
                             error_message=" Operation type <{}> is invalid/undefined.".format(op_type))
                log.error(e)
                handle_exception()
                return False, e

        except:
            e = get_exception()
            log.error(e)
            return False, e

    def run(self):
        status, error = self.run_operations()
        if error == "Please make an entry to Operation/Job Config Table":
            abc.update_abc_job_failure(self.etl_job_run_id,self.etl_job_id)
            abc.update_abc_failure(self.etl_prcs_run_id,self.etl_prcs_id)
            log.error("Application run has ended with errors")
        elif not status:
            log.error("Application run has ended with errors")
            #abc.update_abc_job_failure(self.etl_job_run_id,self.etl_job_id)
            #abc.update_abc_failure(self.etl_prcs_run_id,self.etl_prcs_id)
            return False
        else:
            if self.part_val == None:
                abc.update_abc_job_success(self.etl_job_run_id,self.etl_job_id)
                abc.update_abc_success(self.etl_prcs_run_id,self.etl_prcs_id,self.prcs_name)
            else:
                abc.update_abc_job_success(self.etl_job_run_id,self.etl_job_id)
            return True


