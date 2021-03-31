from datetime import datetime
import os
from mongodboperation import mongodb

class logging_class:
    def __init__(self):
        self.mongodb = mongodb()
    def logs(self,dbname,colname,logmessage):
        try:
            self.now = datetime.now()
            self.currentdate = self.now.date()
            self.current_time = self.now.strftime("%H%M%S")

            self.record = {
                "Date":str(self.currentdate),
                "Time":str(self.current_time),
                "Message":str(logmessage)
            }

            self.mongodb.insert_one_record(self.record,dbname,colname)
        except Exception as e:
            raise e


