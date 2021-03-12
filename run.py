import hub_config as cc
import argparse
import sys
import os

sys.path.append(os.getcwd())

hub_parser = argparse.ArgumentParser(description="Hub App")
hub_parser.add_argument('--job_id', help='job_id of the job to run', required=True)
hub_parser.add_argument('--job_run_id', help='job_run_id from an external system for tracking purposes')
hub_parser.add_argument('--job_config_table', default=cc.job_config_table)
hub_parser.add_argument('--op_config_table', default=cc.op_config_table)
hub_parser.add_argument('--sf_account', default=cc.sf_account_name)
hub_parser.add_argument('--sf_url', default=cc.sf_url)
hub_parser.add_argument('--sf_schema', default=cc.sf_schema)
hub_parser.add_argument('--sf_database', default=cc.sf_database)
hub_parser.add_argument('--sf_username', default=cc.sf_username)
hub_parser.add_argument('--sf_passphrase', default=cc.sf_passphrase)
hub_parser.add_argument('--sf_private_key_path', default=cc.sf_private_key_path)
hub_parser.add_argument('--sf_role', default=cc.sf_role)
hub_parser.add_argument('--sf_warehouse', default=cc.sf_warehouse)
hub_parser.add_argument('--rds_host', default=cc.rds_host)
hub_parser.add_argument('--rds_user', default=cc.rds_user)
hub_parser.add_argument('--rds_port', default=cc.rds_port)
hub_parser.add_argument('--rds_region', default=cc.rds_region)
hub_parser.add_argument('--rds_database', default=cc.rds_database)
hub_parser.add_argument('--rds_schema', default=cc.rds_schema)
hub_parser.add_argument('--region', help='region name', required=True)
hub_parser.add_argument('--prcs_name', help='process name', required=True)
hub_parser.add_argument('--sql_vars', help='parameters to sql file')
hub_parser.add_argument('--part_val', help='parameters to s3 location file')
hub_parser.add_argument('--memory_config', help='parameters for spark memory of the job')
hub_parser.add_argument('--schd_name', help='schedule name', required=True)
hub_parser.add_argument('--btch_name', help='batch name', required=True)
hub_parser.add_argument('--sbjct_area_name', help='subject area name', required=True)
hub_parser.add_argument('--o_date', help='Control M O Date')
hub_parser.add_argument('--reporting_code', help='Reporting time period code')

args = hub_parser.parse_args()
from hub_app import *


def main():
    args = hub_parser.parse_args()
    op_tags = {"job_id": args.job_id,
               "job_run_id": args.job_run_id,
               "job_config_table": args.job_config_table,
               "op_config_table": args.op_config_table,
               "sf_account": args.sf_account,
               "sf_url": args.sf_url,
               "sf_database": args.sf_database,
               "sf_username": args.sf_username,
               "sf_private_key_path": args.sf_private_key_path,
               "sf_role": args.sf_role,
               "sf_schema": args.sf_schema,
               "sf_warehouse": args.sf_warehouse,
               "sf_passphrase": args.sf_passphrase,
               "rds_host": args.rds_host,
               "rds_user": args.rds_user,
               "rds_port": args.rds_port,
               "rds_region": args.rds_region,
               "rds_database": args.rds_database,
               "rds_schema": args.rds_schema,
               "region": args.region,
               "prcs_name": args.prcs_name,
               "sql_vars": args.sql_vars,
               "part_val": args.part_val,
               "memory_config": args.memory_config,
               "schd_name": args.schd_name,
               "btch_name": args.btch_name,
               "sbjct_area_name": args.sbjct_area_name,
               "o_date": args.o_date,
               "reporting_code": args.reporting_code
               }
    e = Entity(**op_tags)
    flag = e.run()
    if flag:
        sys.exit(0)
    else:
        sys.exit(-1)


if __name__ == '__main__':
    main()
