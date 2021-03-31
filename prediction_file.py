from Application_logging.logging_file import logging_class
from DataIngestion.DataLoader_file import data_loader_class
from DataPreprocessing.Preprocessing_file import Preprocessing_class
from s3_operation import s3_bucket_operation
from mongodboperation import mongodb
from Fileoperation.FileOpeartion_file import FileOperation_class
import pandas as pd


class prediction_class:
    def __init__(self):
        self.logger = logging_class()
        self.data_loader = data_loader_class()
        self.preprocessing = Preprocessing_class()
        self.s3 = s3_bucket_operation()
        self.mongo = mongodb()
        self.file_operation = FileOperation_class()
        self.region = "us-east-2"
        self.model_bucket = "thyroidpicklemodel16022021"
        self.result_bucket = "thyroidresultbucket16022021"
        self.result_file = 'PredictionFile.csv'





    def prediction(self):
        try:
            # first delete the previous prediction that we have done last time
            try:
                self.s3.delete_single_obj_in_bucket(self.region,self.result_bucket,self.result_file)
                self.logger.logs("logs","prediction","deleted the previous prediction from bucket")

            except:
                self.logger.logs("logs","prediction","Failed in deleting the previous prediction from bucket")
                pass

            data = self.data_loader.prediction_loader()

            col_to_remove = ["TSH_measured", "T3_measured", "TT4_measured", "T4U_measured", "FTI_measured", "TBG_measured","TBG", "hypopituitary"]

            data = self.preprocessing.remove_columns(data,col_to_remove)

            data = self.preprocessing.replace_invalid_values_by_null(data)

            data = self.preprocessing.correct_data_type(data)

            # imputing the missing values
            cat_cols,discreate_col,continuous_col = self.preprocessing.segregate_column_names_for_imputation(data)

            for col in cat_cols:
                data = self.preprocessing.impute_by_mode(data,col)

            for col in continuous_col:
                data = self.preprocessing.impute_by_median(data,col)

            data = self.preprocessing.impute_by_capture_nan_in_continuous_cols(data,"T3")



            # encoding the categorical variable

            cat_cols,discreate_col,continuous_col = self.preprocessing.segragate_colums_for_encoding(data)

            two_cat_list = self.preprocessing.two_cat_list(cat_cols,data)
            # getting one cat list which columns has only one categories
            one_cat_list = self.preprocessing.one_cat_list(cat_cols, data)

            data = self.preprocessing.get_dummy_for_two_cat(two_cat_list,data)

            if len(one_cat_list) > 0:
                data = self.preprocessing.get_dummy_for_one_cat(columns_list=one_cat_list, xdata=data)
            else:
                pass


            data = self.preprocessing.get_dummy_for_more_than_two_cat(["referral_source"],data)


            # aaply the chi2 feature selection
            data = self.preprocessing.chi2_test(data)

            # getting model name from bucket
            lis = self.s3.getting_list_of_object_in_bucket(self.region,self.model_bucket)
            model_name = lis[0].split(".")[0]

            # loading the trained model for prediction
            model = self.file_operation.load_model(model_name)
            # doing the prediction
            prediction = list(model.predict(data))

            # converting the prediction integer into categories
            prediction = self.preprocessing.decode_class_label_ypred(prediction)

            result = pd.DataFrame({"PredictionResult":prediction})

            self.s3.upload_dataframe_to_bucket(self.region,self.result_bucket,self.result_file,result)

            return {"Prediction Location": " Result bucket : {} , Result File : {}".format(str(self.result_bucket),str(self.result_file))}


        except Exception as e:
            raise e

