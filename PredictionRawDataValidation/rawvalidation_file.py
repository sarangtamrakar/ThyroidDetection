from s3_operation import s3_bucket_operation
from Application_logging.logging_file import logging_class
from datetime import datetime
import re


class rawValidation_class:
    def __init__(self,path):
        self.path = path
        self.logger = logging_class()
        self.schema_bucket = "thyroidschema16022021"
        self.schema_file = "schema_prediction.json"
        self.good_data = "thyroidpredictgoodrawdata16022021"
        self.bad_data = "thyroidpredictbadrawdata16022021"
        self.archieve_bucket = "thyroidpredictarchievebaddata16022021"
        self.region_name = "us-east-2"
        self.s3 = s3_bucket_operation()

    def values_from_schema(self):
        self.logger.logs("logs","prediction","entered into values_from_schema method of rawValidation_class")
        try:
            dic = self.s3.read_json_obj_from_s3_bucket(self.region_name,self.schema_bucket,self.schema_file)
            pattern = dic.get('SampleFileName')
            LengthOfDateStampInFile = dic.get('LengthOfDateStampInFile')
            LengthOfTimeStampInFile = dic.get('LengthOfTimeStampInFile')
            column_names = dic.get('ColName')
            NumberofColumns = dic.get('NumberofColumns')
            return pattern,LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,NumberofColumns

        except Exception as e:
            self.logger.logs("logs","prediction","Exception Occured in values_from_schema method of rawValidation_class")
            self.logger.logs("logs","prediction","Exception : {}".format(str(e)))
            raise e

    def manual_regex_creation(self):
        self.logger.logs("logs", "prediction", "entered into manual_regex_creation method of rawValidation_class")

        try:
            regex = "['hypothyroid']+['\_'']+[\d_]+[\d]+\.csv"
            # r = "['hypothyroid']+['\_']+['\d']+['\_']+['\d']+['\.csv']"
            return regex
        except Exception as e:
            self.logger.logs("logs", "prediction",
                             "Exception Occured in manual_regex_creation method of rawValidation_class")
            self.logger.logs("logs", "prediction", "Exception : {}".format(str(e)))
            raise e

    def createDirectoryForGoodBadRawData(self):
        self.logger.logs("logs", "prediction", "entered into createDirectoryForGoodBadRawData method of rawValidation_class")

        try:
            if self.good_data not in self.s3.get_list_of_bucket_in_s3(self.region_name):
                self.s3.create_bucket(self.region_name,self.good_data)
            else:
                pass

            if self.bad_data not in self.s3.get_list_of_bucket_in_s3(self.region_name):
                self.s3.create_bucket(self.region_name,self.bad_data)
            else:
                pass
        except Exception as e:
            self.logger.logs("logs", "prediction",
                             "Exception Occured in createDirectoryForGoodBadRawData method of rawValidation_class")
            self.logger.logs("logs", "prediction", "Exception : {}".format(str(e)))
            raise e


    def DeleteExistingGoodDataFolder(self):
        self.logger.logs("logs", "prediction", "entered into DeleteExistingGoodDataFolder method of rawValidation_class")

        try:
            if self.good_data in self.s3.get_list_of_bucket_in_s3(self.region_name):
                self.s3.delete_bucket_with_all_objects(self.region_name,self.good_data)
            else:
                pass
        except Exception as e:
            self.logger.logs("logs", "prediction",
                             "Exception Occured in DeleteExistingGoodDataFolder method of rawValidation_class")
            self.logger.logs("logs", "prediction", "Exception : {}".format(str(e)))
            raise e


    def DeleteExistingBadDataFolder(self):
        self.logger.logs("logs", "prediction", "entered into DeleteExistingBadDataFolder method of rawValidation_class")

        try:
            if self.bad_data in self.s3.get_list_of_bucket_in_s3(self.region_name):
                self.s3.delete_bucket_with_all_objects(self.region_name,self.bad_data)
            else:
                pass
        except Exception as e:
            self.logger.logs("logs", "prediction",
                             "Exception Occured in DeleteExistingBadDataFolder method of rawValidation_class")
            self.logger.logs("logs", "prediction", "Exception : {}".format(str(e)))
            raise e


    def moveBadFilesToArchieve(self):
        self.logger.logs("logs", "prediction", "entered into moveBadFilesToArchieve method of rawValidation_class")

        try:
            self.now = datetime.now()
            self.date = self.now.date()
            self.time = self.now.strftime("%H%M%S")

            if self.bad_data in self.s3.get_list_of_bucket_in_s3(self.region_name):
                if self.archieve_bucket not in self.s3.get_list_of_bucket_in_s3(self.region_name):
                    self.s3.create_bucket(self.region_name,self.archieve_bucket)

                dest = "BadData"+str(self.date)+str(self.time)
                self.s3.create_folder_in_bucket(self.region_name,self.archieve_bucket,dest)
                files = self.s3.getting_list_of_object_in_bucket(self.region_name,self.bad_data)

                for file in files:
                    self.s3.copy_file_to_another_bucket_folder(self.region_name,self.bad_data,self.archieve_bucket,file,dest)

        except Exception as e:
            self.logger.logs("logs", "prediction",
                             "Exception Occured in moveBadFilesToArchieve method of rawValidation_class")
            self.logger.logs("logs", "prediction", "Exception : {}".format(str(e)))
            raise e


    def validateFileName(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        self.logger.logs("logs", "prediction", "entered into validateFileName method of rawValidation_class")

        try:
            # delete the previous one
            self.DeleteExistingGoodDataFolder()
            self.DeleteExistingBadDataFolder()
            # make the new one
            self.createDirectoryForGoodBadRawData()

            files = self.s3.getting_list_of_object_in_bucket(self.region_name,self.path)

            for filename in files:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            self.s3.copy_file_to_another_bucket(self.region_name,self.path,self.good_data,str(filename))
                            self.s3.delete_single_obj_in_bucket(self.region_name,self.path,str(filename))
                        else:
                            self.s3.copy_file_to_another_bucket(self.region_name,self.path,self.bad_data,str(filename))
                            self.s3.delete_single_obj_in_bucket(self.region_name,self.path,str(filename))
                    else:
                        self.s3.copy_file_to_another_bucket(self.region_name, self.path, self.bad_data, str(filename))
                        self.s3.delete_single_obj_in_bucket(self.region_name, self.path, str(filename))
                else:
                    self.s3.copy_file_to_another_bucket(self.region_name, self.path, self.bad_data, str(filename))
                    self.s3.delete_single_obj_in_bucket(self.region_name, self.path, str(filename))
        except Exception as e:
            self.logger.logs("logs", "prediction",
                             "Exception Occured in validateFileName method of rawValidation_class")
            self.logger.logs("logs", "prediction", "Exception : {}".format(str(e)))
            raise e

    def validateNoOfColumns(self,NoOfColumns):
        self.logger.logs("logs", "prediction", "entered into validateNoOfColumns method of rawValidation_class")

        try:
            files = self.s3.getting_list_of_object_in_bucket(self.region_name,self.good_data)
            for file in files:
                data = self.s3.read_csv_obj_from_s3_bucket(self.region_name,self.good_data,file)
                if (data.shape[1]==NoOfColumns):
                    pass
                else:
                    self.s3.copy_file_to_another_bucket(self.region_name,self.good_data,self.bad_data,file)
                    self.s3.delete_single_obj_in_bucket(self.region_name,self.good_data,str(file))
        except Exception as e:
            self.logger.logs("logs", "prediction",
                             "Exception Occured in validateNoOfColumns method of rawValidation_class")
            self.logger.logs("logs", "prediction", "Exception : {}".format(str(e)))
            raise e

    def validateMissingValueInWholeColumn(self):
        self.logger.logs("logs", "prediction", "entered into validateMissingValueInWholeColumn method of rawValidation_class")

        try:
            files = self.s3.getting_list_of_object_in_bucket(self.region_name, self.good_data)

            for file in files:
                csv = self.s3.read_csv_obj_from_s3_bucket(self.region_name, self.good_data,file)


                for columns in list(csv.columns):
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        self.s3.copy_file_to_another_bucket(self.region_name,self.good_data,self.bad_data,file)
                        self.s3.delete_single_obj_in_bucket(self.region_name,self.good_data,file)
                        break
        except Exception as e:
            self.logger.logs("logs", "prediction",
                             "Exception Occured in validateMissingValueInWholeColumn method of rawValidation_class")
            self.logger.logs("logs", "prediction", "Exception : {}".format(str(e)))
            raise e

