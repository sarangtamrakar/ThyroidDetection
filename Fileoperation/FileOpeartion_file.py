from s3_operation import s3_bucket_operation
from mongodboperation import mongodb
from Application_logging.logging_file import logging_class

class FileOperation_class:

    def __init__(self):
        self.s3 = s3_bucket_operation()
        self.mongo = mongodb()
        self.logger = logging_class()
        self.region = "us-east-2"
        self.model_bucket = "thyroidpicklemodel16022021"


    def save_model(self,model_obj,model_name):
        self.logger.logs("logs","training","entered into save_model method of FileOperation_class")
        try:
            self.s3.upload_ml_model(self.region,self.model_bucket,model_obj,model_name)
            self.logger.logs("logs","training","saved the model to the s3 bucket :"+str(self.model_bucket))
            self.logger.logs("logs","training","model name : "+str(model_name))
        except Exception as e:
            self.logger.logs("logs","training","Failed to save the model into bucket : model name :"+str(model_name))
            self.logger.logs("logs","training","exception occured : "+str(e))
            raise e

    def load_model(self,model_name):
        self.logger.logs("logs", "prediction", "entered into save_model method of FileOperation_class")

        try:
            model = self.s3.read_pickle_obj_from_s3_bucket(self.region,self.model_bucket,model_name+str('.pickle'))
            self.logger.logs("logs","prediction","we have loaded the model")
            return model
        except Exception as e:
            self.logger.logs("logs","prediction","we have failed to load the model fro bucket")
            self.logger.logs("logs","prediction","error : "+str(e))
            raise e




