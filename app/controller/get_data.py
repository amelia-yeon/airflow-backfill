import pendulum
import time
import requests
import math
import pendulum
import os 
import sys
import requests
import json

from app.common import env, exception
from app.common.connector import BigqueryDac
from app.common.logger import LoggerFactory


class GetPublicData():
    def __init__(self):
        pass
    
    def create_dataset(self):
        # LoggerFactory._LOGGER.info("test")
        
        dataset ='airflow_test_3'
        
        try:
            dac = BigqueryDac()
            dac.create_dataset(dataset_id=dataset)
        
        except Exception as e:
            LoggerFactory._LOGGER.warning("test search error: {}".format(e))
    
    def search_data(self):
        LoggerFactory._LOGGER.info("test")
        
        try: 
            dac = BigqueryDac()
            query = f"""
                    SELECT * FROM `analytics.payment_2023` limit 10;
            """
            
            query_job = dac.select_query_only(query)
            
            total = []
            for row in query_job:
                data = {}
                data['ACC_YEAR'] = row['ACC_YEAR']
                data['SGG_CD'] = row['SGG_CD']
                data['SGG_NM'] = row['SGG_NM']
                data['BJDONG_CD'] = row['BJDONG_CD']
                data['BJDONG_NM'] = row['BJDONG_NM']
                data['LAND_GBN'] = row['LAND_GBN']
                data['LAND_GBN_NM'] = row['LAND_GBN_NM']
                data['BOBN'] = row['BOBN'] 
                data['BUBN'] = row['BUBN'] 
                data['FLR_NO'] = row['FLR_NO']
                data['CNTRCT_DE'] = row['CNTRCT_DE']
                data['RENT_GBN'] = row['RENT_GBN']
                data['RENT_AREA'] = row['RENT_AREA']
                data['RENT_GTN'] = row['RENT_GTN']
                data['RENT_FEE'] = row['RENT_FEE']
                data['BLDG_NM'] = row['BLDG_NM']
                data['BUILD_YEAR'] = row['BUILD_YEAR']
                data['HOUSE_GBN_NM'] = row['HOUSE_GBN_NM']
                data['CNTRCT_PRD'] = row['CNTRCT_PRD']
                data['NEW_RON_SECD'] = row['NEW_RON_SECD']
                data['CNTRCT_UPDT_RQEST_AT'] = row['CNTRCT_UPDT_RQEST_AT']
                data['BEFORE_GRNTY_AMOUNT'] = None if row['BEFORE_GRNTY_AMOUNT'] == '' else  row['BEFORE_GRNTY_AMOUNT'] 
                data['BEFORE_MT_RENT_CHRGE'] = None if row['BEFORE_MT_RENT_CHRGE'] == '' else row['BEFORE_MT_RENT_CHRGE']
                data['IF_DATETIME'] = pendulum.now('Asia/Seoul').strftime('%Y%m%d%H%M%S')
                total.append(data)
                
            return total
        
        except Exception as e:
            LoggerFactory._LOGGER.warning("test search error: {}".format(e))
    
    def delete_table(self):
        # LoggerFactory._LOGGER.info("테이블 삭제")
        
        dac = BigqueryDac()
        # table_id = 'analytics.payment_2023' 
        test_table_id = 'analytics.payment_2023_copy' 
        
        try:
            del_query = f"""
                    DELETE FROM `{test_table_id}` WHERE 1=1;
            """
            query_job = dac.select_query_only(del_query)
            
            if query_job:
                return True
            
            else:
                return False
        
        except Exception as e:
            LoggerFactory._LOGGER.warning("table delete error: {}".format(e))
            
    # API 통신 테스트 
    def status_test(self):
        KEY = str(os.environ.get("DATA_SERVICE_KEY"))
        type='json'
        service='tbLnOpendataRentV'
        start = 1
        end = 2
        year = pendulum.now('Asia/Seoul').strftime('%Y')

        
        pre_url = f'http://openapi.seoul.go.kr:8088/{KEY}/{type}/{service}/{start}/{end}/{year}'
        print("1")
        pre_res = requests.get(pre_url)
        try:
            if pre_res.status_code == 200:
                print("200 pass")
                
                if pre_res.text:
                    print(pre_res.text)
                    pre_res = pre_res.json()
                else:
                    print("fail")
        except requests.exceptions.HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except requests.exceptions.RequestException as req_err:
            print(f'Request error occurred: {req_err}') 
        except json.JSONDecodeError as json_err:
            print(f'JSON decoding error occurred: {json_err}')
        
    
    # 공공 API 데이터 가져오기 
    def property(self):
        
        # LoggerFactory._LOGGER.info("부동산 정보 적재 시작")
        
        KEY = str(os.environ.get("DATA_SERVICE_KEY"))
        type='json'
        service='tbLnOpendataRentV'
        start = 1
        end = 2
        year = pendulum.now('Asia/Seoul').strftime('%Y')
        
        dac = BigqueryDac()
        table_id = 'analytics.payment_2023_copy' 
        
        pre_url = f'http://openapi.seoul.go.kr:8088/{KEY}/{type}/{service}/{start}/{end}/{year}'
        pre_res = requests.get(pre_url)
        pre_res_json = pre_res.json()
        total_cnt = pre_res_json['tbLnOpendataRentV']['list_total_count']

        try:
            
            for i in range(1, math.ceil(total_cnt/1000)+1):
                end = i*1000
                start = end-1000 +1
                
                if end > total_cnt:
                    end = total_cnt
            
                url = f'http://openapi.seoul.go.kr:8088/{KEY}/{type}/{service}/{start}/{end}/{year}'
                
                res = requests.get(url)
                res_json = res.json()
                res_json = res_json['tbLnOpendataRentV']['row']
                print(res_json)
                    
                total = []
                
                for i in range(len(res_json)):
                    data = {}
                    data['ACC_YEAR'] = res_json[i]['ACC_YEAR']
                    data['SGG_CD'] = res_json[i]['SGG_CD']
                    data['SGG_NM'] = res_json[i]['SGG_NM']
                    data['BJDONG_CD'] = res_json[i]['BJDONG_CD']
                    data['BJDONG_NM'] = res_json[i]['BJDONG_NM']
                    data['LAND_GBN'] = None if res_json[i]['LAND_GBN'] =='' else res_json[i]['LAND_GBN'] 
                    data['LAND_GBN_NM'] = res_json[i]['LAND_GBN_NM']
                    data['BOBN'] = res_json[i]['BOBN'] 
                    data['BUBN'] = res_json[i]['BUBN'] 
                    data['FLR_NO'] = res_json[i]['FLR_NO']
                    data['CNTRCT_DE'] = res_json[i]['CNTRCT_DE']
                    data['RENT_GBN'] = res_json[i]['RENT_GBN']
                    data['RENT_AREA'] = res_json[i]['RENT_AREA']
                    data['RENT_GTN'] = res_json[i]['RENT_GTN']
                    data['RENT_FEE'] = res_json[i]['RENT_FEE']
                    data['BLDG_NM'] = res_json[i]['BLDG_NM']
                    data['BUILD_YEAR'] = res_json[i]['BUILD_YEAR']
                    data['HOUSE_GBN_NM'] = res_json[i]['HOUSE_GBN_NM']
                    data['CNTRCT_PRD'] = res_json[i]['CNTRCT_PRD']
                    data['NEW_RON_SECD'] = res_json[i]['NEW_RON_SECD']
                    data['CNTRCT_UPDT_RQEST_AT'] = res_json[i]['CNTRCT_UPDT_RQEST_AT']
                    data['BEFORE_GRNTY_AMOUNT'] = None if res_json[i]['BEFORE_GRNTY_AMOUNT'] == '' else  res_json[i]['BEFORE_GRNTY_AMOUNT'] 
                    data['BEFORE_MT_RENT_CHRGE'] = None if res_json[i]['BEFORE_MT_RENT_CHRGE'] == '' else res_json[i]['BEFORE_MT_RENT_CHRGE']
                    data['IF_DATETIME'] = pendulum.now('Asia/Seoul').strftime('%Y%m%d%H%M%S')
                    total.append(data)
                
                query_job = dac.insert_rows_json(table_id, total)
            
                if query_job !=[]:
                    LoggerFactory._LOGGER.warning("정보 적재 실패")

            
        except Exception as e:
            LoggerFactory._LOGGER.warning("공공API 에러 발생: {}".format(e))
             
            

