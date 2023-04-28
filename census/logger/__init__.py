import logging
import os
import sys
from datetime import datetime


LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"

logs_path = os.path.join( "logs", LOG_FILE)

os.makedirs(logs_path,exist_ok=True)

log_file_path = os.path.join(logs_path,LOG_FILE)

logging.basicConfig(
filename=log_file_path,
format="[%(asctime)s]: %(levelname)s -['User_Name':Sukruth]: -['filename']:%(filename)s -['function_name']:%(funcName)s -['line_no']:%(lineno)d - %(message)s",
level=logging.INFO
)
