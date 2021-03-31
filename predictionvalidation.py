from PredictionRawDataValidation.rawvalidation_file import rawValidation_class
from PredictionDataTransform.transform_file import DataTransform_class
from mongodboperation import mongodb
from s3_operation import s3_bucket_operation
from EmailSender import emailSenderClass
from Application_logging.logging_file import logging_class

class prediction_Validtion_class:
    def __init__(self,path):
        self.mongodb = mongodb()
        self.s3 = s3_bucket_operation()
        self.rawValidation = rawValidation_class(path)
        self.dataTransform = DataTransform_class()
        self.region = 'us-east-2'
        self.finaldb = "finaldb"
        self.finalcol = "predictiondatacol"
        self.goodbucket = "thyroidpredictgoodrawdata16022021"
        self.badbucket = "thyroidpredictbadrawdata16022021"
        self.inputfilebucket = "thyroidinputfiles16022021"
        self.inputfile = "inputfilepredict.csv"
        self.email = emailSenderClass()
        self.logger = logging_class()

    def Validate_predictions(self):
        try:
            self.logger.logs("logs","prediction","Prediction raw data validation start")

            # getting values from training schemas
            pattern, LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns  = self.rawValidation.values_from_schema()

            # regex creation
            regex = self.rawValidation.manual_regex_creation()

            # Validate prediction filename
            self.rawValidation.validateFileName(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)

            # validate no of columns in each file
            self.rawValidation.validateNoOfColumns(NumberofColumns)

            # Validate missing value in whole columns
            self.rawValidation.validateMissingValueInWholeColumn()

            # transform the data before pushing it to the db
            self.dataTransform.transform()

            # drop the previous collection then create new one because
            # we don't want to do prediction in previous stored data
            self.mongodb.drop_collection_before_prediction(self.finaldb,self.finalcol)


            # dump all data from s3 good bucket to mongodb
            self.mongodb.dump_all_dataframe_into_mongo_db(self.finaldb,self.finalcol,self.goodbucket)

            # delete existing good data folder
            self.rawValidation.DeleteExistingGoodDataFolder()

            # sending Bad files information to client via email
            try:
                files = self.s3.getting_list_of_object_in_bucket(self.region,self.badbucket)
                self.email.send_message(self.badbucket,files)
            except:
                pass


            # moving bad files to archieve bucket
            try:
                self.rawValidation.moveBadFilesToArchieve()
            except:
                pass

            # delete the bad data bucket after moving bad files to archieve bucket
            try:
                self.rawValidation.DeleteExistingBadDataFolder()
            except:
                pass

            # delete the previous input file
            try:
                self.s3.delete_single_obj_in_bucket(self.region,self.inputfilebucket,self.inputfile)
            except:
                pass

            # reading dataframe from db & dump into bucket as single csv file.
            self.mongodb.read_dataframe_from_db_and_dump_into_bucket(self.finaldb,self.finalcol,self.inputfilebucket,self.inputfile)

            self.logger.logs("logs","prediction","we have done the whole validation in prediction Data")
        except Exception as e:
            raise e
