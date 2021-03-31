from TrainingRawDataValidation.rawValidationfile import rawValidation_class
from TrainingDataTransform.transformFile import transform_class
from mongodboperation import mongodb
from s3_operation import s3_bucket_operation
from EmailSender import emailSenderClass
from Application_logging.logging_file import logging_class

class TrainValidation_class:
    def __init__(self,path):
        self.mongodb = mongodb()
        self.s3 = s3_bucket_operation()
        self.rawValidation = rawValidation_class(path)
        self.dataTransform = transform_class()
        self.region = 'us-east-2'
        self.finaldb = "finaldb"
        self.finalcol = "trainingdatacol"
        self.goodbucket = "thyroidtraingoodrawdata16022021"
        self.badbucket = "thyroidtrainbadrawdata16022021"
        self.inputfilebucket = "thyroidinputfiles16022021"
        self.inputfile = "inputfiletrain.csv"
        self.email = emailSenderClass()
        self.logger = logging_class()


    def validate(self):
        try:
            self.logger.logs("logs","training"," Training Raw data validation start")
            # getting values from training schema
            pattern, LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns = self.rawValidation.values_from_schema()

            # regex creation
            regex = self.rawValidation.manualRegexcreation()


            # validate raw file name
            self.rawValidation.validateFileName(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)


            # validate no of columns
            self.rawValidation.validateNoOfColumns(NumberofColumns)





            # validate missing values in whole columns
            self.rawValidation.validateMissingValueInWholeColumn()

            self.logger.logs("logs", "training", "Raw data validation completed")

            self.logger.logs("logs", "training","Data Transform Start")


            # data transform before pushing it to database
            self.dataTransform.transform()

            self.logger.logs("logs", "training", "Data Transform completed")

            self.logger.logs("logs", "training", "Dumping data into mongodb start")


            # dump all the dataframe from s3 good bucket to mongo db database
            try:
                self.mongodb.dump_all_dataframe_into_mongo_db(self.finaldb,self.finalcol,self.goodbucket)
                self.logger.logs("logs", "training", "Dumping data into db completed")
            except Exception as e:
                self.logger.logs("logs","training","Exception occured while dump_all_dataframe_into_mongo_db")
                self.logger.logs("logs","training","Exception Message : "+str(e))
                raise e



            # delete the good data bucket after pushing data into db
            self.rawValidation.DeleteExistingGoodDataFolder()
            self.logger.logs("logs", "training", "DeleteExistingGoodDataFolder completed")


            # sending the information of bad bucket to the client
            files = self.s3.getting_list_of_object_in_bucket(self.region,self.badbucket)
            self.email.send_message(self.badbucket,files)
            self.logger.logs("logs", "training", "Email sent to client")



            # then sending the bad files to the archieve bucket folder
            self.rawValidation.moveBadFileToArchieveBad()
            self.logger.logs("logs", "training", "Bad files moved to archieved bad data folder")




            # then delete the bad bucket folder from s3
            self.rawValidation.DeleteExistingBadDataFolder()
            self.logger.logs("logs", "training", "DeleteExistingBadDataFolder completed")





            # delete the previous input file from s3 bucket
            self.s3.delete_single_obj_in_bucket(self.region,self.inputfilebucket,self.inputfile)
            self.logger.logs("logs", "training", "deleted the previous inputfile from s3 completed")



            # read the data from db as Dataframe & dump into s3 bucket


            try:
                self.mongodb.read_dataframe_from_db_and_dump_into_bucket(self.finaldb,self.finalcol,self.inputfilebucket,self.inputfile)
                self.logger.logs("logs", "training", "inputfile for trainng completed completed")
            except Exception as e:
                self.logger.logs("logs", "training", "Exception occured while  read_dataframe_from_db_and_dump_into_bucket")
                self.logger.logs("logs","training","Exception Message : "+str(e))


        except Exception as e:
            self.logger.logs("logs", "training", "Training Validation Failed")
            self.logger.logs("logs", "training", "Error : "+str(e))

            raise e



