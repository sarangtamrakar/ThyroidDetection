from s3_operation import s3_bucket_operation
from Application_logging.logging_file import logging_class

class data_loader_class:
    def __init__(self):
        self.s3 = s3_bucket_operation()
        self.logger = logging_class()
        self.input_bucket = "thyroidinputfiles16022021"
        self.training_inputFile = "inputfiletrain.csv"
        self.prediction_inputFile = "inputfilepredict.csv"
        self.region = "us-east-2"


    def training_loader(self):
        self.logger.logs("logs", "training", "Entered into training_loader method of data_loader_class")
        try:
            dataframe = self.s3.read_csv_obj_from_s3_bucket(self.region,self.input_bucket,self.training_inputFile)
            self.logger.logs("logs","training","Input Training Data Loaded")
            return dataframe
        except Exception as e:
            self.logger.logs("logs", "training", "Exception occured in loading training input file from s3 bucket")
            self.logger.logs("logs","training","Exception Message : "+str(e))
            raise e

    def prediction_loader(self):
        self.logger.logs("logs", "prediction", "Entered into prediction_loader method of data_loader_class")

        try:
            dataframe = self.s3.read_csv_obj_from_s3_bucket(self.region,self.input_bucket,self.prediction_inputFile)
            self.logger.logs("logs","prediction","Input Prediction Data loaded")
            return dataframe

        except Exception as e:
            self.logger.logs("logs", "prediction", "Exception occured in prediction_loader method of prediction_loader class")
            raise e

