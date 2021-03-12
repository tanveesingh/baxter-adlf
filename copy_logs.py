import datetime as datetime
import os
import hub_config as config
import sys

def copy_log():
    ct_dt = datetime.datetime.now()
    ct_dt_str = ct_dt.strftime('%Y%m%d')
    print("Current Date: ",ct_dt_str)
    region = sys.argv[1]
    source_log_path = config.log_path + region + "/" + ct_dt_str + "/"
    target_log_path = "s3://"+config.config_bucket_name+"/log/"+region+"/"+ct_dt_str+"/"
    cp_command = "aws s3 cp "+ source_log_path + " " + target_log_path + " --recursive --quiet --only-show-errors"
    print("Log Copy Command: ",cp_command)
    os.system(cp_command)
    print("Logs copied to S3")
    rm_command = "rm -R "+source_log_path
    os.system(rm_command)
    print("Logs removed from EMR")

if __name__ == '__main__':
    copy_log()
