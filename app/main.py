import time

from common import env, exception
from controller.get_data import GetPublicData as ctrl_getpublic
from common.logger import LoggerFactory
import os 
import sys

LoggerFactory.create_logger()


def delete_table():
    data = ctrl_getpublic().delete_table()
    # data = '1234'
    # print(data)
    
    return {'data' : data}

def public_api():
    data = ctrl_getpublic().property()
    # data = '1234'
    # print(data)
    
    return {'data' : data}

def bigquery_test():
    data = ctrl_getpublic().search_data()
    # data = '1234'
    # print(data)
    print(data)
    
    return {'data' : data}

def bigquery_test2():
    data = ctrl_getpublic().create_dataset()
    
    return {'data': data}
    
# public_api()
# bigquery_test()
# bigquery_test2()

