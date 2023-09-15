

import os
import sys
from dotenv import load_dotenv


# 환경변수 셋팅 -> 개발기/운영기 구분하는 경우에 사용 할 것 
# arguments = sys.argv
# if arguments[1].lower() == "-d" or arguments[1].lower() == "-dev":
#     ENV_PATH = '/.env.dev'  # 개발
#     print(">>>>>>>>>>>>>개발 모드>>>>>>>>>>>>>")
    
# elif arguments[1].lower() == "-p" or arguments[1].lower() == "-product":
#     ENV_PATH = '/.env.product'  # 개발
#     print(">>>>>>>>>>>>>운영 모드>>>>>>>>>>>>>")

ENV_PATH = '/.env'

#dev/env에 따른 KEY load
load_dotenv(dotenv_path=os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))) + ENV_PATH)

#ENV 환경 설정
ENV = str(os.environ.get("ENV"))

# GCS KEY 설정
GCS_KEY_FILE =  os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + str(os.environ.get("GCS_KEY_FILE"))
GCS_PROJECT_ID = str(os.environ.get("GCS_PROJECT_ID"))


#공공데이터 API 인증 KEY 설정
DATA_SERVICE_KEY = str(os.environ.get("DATA_SERVICE_KEY"))


#dev/env crontab 설정
# RENOVATION_DAY = str(os.environ.get("RENOVATION_DAY"))

#MAIL INFO
# EMAIL_ADDRESS = str(os.environ.get("EMAIL_ADDRESS"))
# EMAIL_PASSWORD = str(os.environ.get("EMAIL_PASSWORD"))
# TO_MAIL_ADDRESS = str(os.environ.get("TO_MAIL_ADDRESS"))
# CC_MAIL_ADDRESS = str(os.environ.get("CC_MAIL_ADDRESS"))




