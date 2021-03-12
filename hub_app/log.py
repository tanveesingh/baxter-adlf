import logging
import sys
import os
import hub_config as ccs
from datetime import datetime

'''
Description : Function to generate logs
'''
def setup_custom_logger(name, region, etl_prcs_name, timestamp, part_val=None):
    try:
        ct_dt = datetime.now()
        ct_dt_str = ct_dt.strftime('%Y%m%d')
        log = logging.getLogger(name)
        log.setLevel(logging.INFO)
        if part_val is None:
            log_filename = etl_prcs_name + "_" + timestamp + ".log"
        else:
            log_filename = etl_prcs_name + "_" + part_val + "_" + timestamp + ".log"
        log_filedir = ccs.log_path + region.upper()+ "/" +ct_dt_str
        log_filename = log_filedir + "/" + log_filename
        os.makedirs(os.path.dirname(log_filename), exist_ok=True)
        fh = logging.FileHandler(log_filename, mode='a', delay=False)
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        log.addHandler(fh)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        log.addHandler(handler)
        return log
    except Exception as err:
        raise Exception(err)
