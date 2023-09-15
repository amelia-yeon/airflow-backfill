import pendulum

from logging import handlers
import logging
import pytz
import datetime
import time
import os
import sys

# =============================================================================== #
# ==================================| setting |================================== #
# =============================================================================== #

class Formatter(logging.Formatter):
    """override logging.Formatter to use an aware datetime object"""

    def converter(self, timestamp):
        # Create datetime in UTC
        dt = datetime.datetime.fromtimestamp(timestamp, tz=pytz.UTC)
        
        # Change datetime's timezone
        return dt.astimezone(pytz.timezone('Asia/Seoul'))
    
    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.strftime("%Y-%m-%d %H:%M:%S")
            except TypeError:
                s = dt.strftime("%Y-%m-%d %H:%M:%S")
        return s

class LoggerFactory(object) :
    _LOGGER = None
    
    @staticmethod
    def  create_logger() :
        #파일명
        now = pendulum.now('Asia/Seoul').format('YYYY-MM-DD')
        #루트 로거 생성
        LoggerFactory._LOGGER = logging.getLogger()
        LoggerFactory._LOGGER.setLevel(logging.INFO)

        # #log 폴더 없을 시 생성
        if (os.path.exists('./log') == False) :
            os.makedirs('./log')

        #로그 포맷 생성
        formatter = Formatter(
            '[%(asctime)s][%(levelname)s|%(filename)s-%(funcName)s:%(lineno)s] >> %(message)s')    
 
        #핸들러 생성
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        file_handler = handlers.TimedRotatingFileHandler(
            "./log/log_"+ pendulum.now('Asia/Seoul').format('YYYY-MM-DD'),
            when="midnight",
            interval=1,
            backupCount=3,
            atTime = datetime.time(9, 0, 0),
            encoding="utf-8",
        )

        file_handler.suffix = 'log_' + pendulum.now('Asia/Seoul').format('YYYY-MM-DD')
        file_handler.setFormatter(formatter)

        LoggerFactory._LOGGER.addHandler(stream_handler)
        LoggerFactory._LOGGER.addHandler(file_handler)

    @classmethod
    def get_logger(cls) :
        return cls._LOGGER
    


