from s3_operation import s3_bucket_operation
from mongodboperation import mongodb
from Application_logging.logging_file import logging_class


class transform_class:
    def __init__(self):
        self.s3 = s3_bucket_operation()
        self.mongodb = mongodb()
        self.good_data = "thyroidtraingoodrawdata16022021"
        self.region = "us-east-2"
        self.logger = logging_class()


    def transform(self):
        self.logger.logs("logs","training","we have entered into the transform method of training data transform_class")
        try:
            files = self.s3.getting_list_of_object_in_bucket(self.region,self.good_data)

            for file in files:
                data = self.s3.read_csv_obj_from_s3_bucket(self.region,self.good_data,file)

                cat_columns = ['sex', 'on_thyroxine', 'query_on_thyroxine','on_antithyroid_medication', 'sick', 'pregnant', 'thyroid_surgery','I131_treatment', 'query_hypothyroid', 'query_hyperthyroid', 'lithium','goitre', 'tumor', 'hypopituitary', 'psych','TSH_measured','T3_measured','TT4_measured','T4U_measured','FTI_measured','TBG_measured','referral_source','Class']

                for col in list(data.columns):
                    if col in cat_columns:
                        data[col] = data[col].apply(lambda x: "'"+str(x)+"'")
                    else:
                        data[col] = data[col].replace("?","'?'")

                self.s3.delete_single_obj_in_bucket(self.region,self.good_data,file)
                self.s3.upload_dataframe_to_bucket(self.region,self.good_data,file,data)
        except Exception as e:
            self.logger.logs("logs", "training",
                             "Exception occured into :  into the transform method of training data transform_class")
            self.logger.logs("logs","training","Exception : "+str(e))

            raise e




