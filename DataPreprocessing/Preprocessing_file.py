from Application_logging.logging_file import logging_class
from DataIngestion.DataLoader_file import data_loader_class
from s3_operation import s3_bucket_operation
import numpy as np
import pickle
from imblearn.over_sampling import RandomOverSampler
from sklearn.feature_selection import chi2,SelectKBest
from sklearn.preprocessing import LabelEncoder
import pandas as pd



class Preprocessing_class:
    def __init__(self):
        self.logger = logging_class()
        self.data_loader = data_loader_class()
        self.s3 = s3_bucket_operation()
        self.encoder_bucket = "thyroidencoderbucket16022021"
        self.region = "us-east-2"


    def replace_invalid_values_by_null(self,data):
        self.logger.logs("logs","preprocessing","entered into replace_invalid_values_by_null method of Preprocessing_class")
        try:
            df = data.replace("'?'",np.nan)
            self.logger.logs("logs", "preprocessing",
                             "replace_invalid_values_by_null completed")

            return df
        except Exception as e:
            self.logger.logs("logs","preprocessing","Exception occured in replace_invalid_values_by_null of Preprocessing_class ")
            self.logger.logs("logs","preprocessing","Exception Message : "+str(e))

            raise e

    def remove_columns(self,dataframe,col_list):
        self.logger.logs("logs", "preprocessing","entered into remove_columns method of Preprocessing_class")

        try:
            data = dataframe.drop(labels=col_list,axis=1)
            self.logger.logs("logs", "preprocessing",
                             "remove_columns completed")

            return data
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in remove_columns of Preprocessing_class ")
            self.logger.logs("logs", "training", "Exception Message : " + str(e))

            raise e

    def correct_data_type(self,df):
        self.logger.logs("logs", "preprocessing",
                         "entered into correct_data_type method of Preprocessing_class")

        try:
            df["age"] = df["age"].astype("float64")
            df["TSH"] = df["TSH"].astype("float64")
            df["T3"] = df["T3"].astype("float64")
            df["TT4"] = df["TT4"].astype("float64")
            df["T4U"] = df["T4U"].astype("float64")
            df["FTI"] = df["FTI"].astype("float64")
            self.logger.logs("logs", "preprocessing",
                             "correct_data_type completed")

            return df
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in correct_data_type of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e

    def segregate_column_names_for_imputation(self,df):
        self.logger.logs("logs", "preprocessing",
                         "entered into segregate_column_names_for_imputation method of Preprocessing_class")

        try:
            num_cols = [col for col in df.columns if df[col].dtypes != "O"]

            cat_cols = [col for col in df.columns if df[col].dtypes == "O"]
            discreate_col = [col for col in num_cols if df[col].nunique() < 5]
            continuous_col = [col for col in num_cols if col not in discreate_col + ["T3"]]

            self.logger.logs("logs", "preprocessing",
                             "segregate_column_names_for_imputation completed")
            return cat_cols,discreate_col,continuous_col
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in segregate_column_names_for_imputation of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e


    def impute_by_mode(self, df, var):
        self.logger.logs("logs", "preprocessing",
                         "entered into impute_by_mode method of Preprocessing_class")

        """ THIS METHOD IS USED FOR CATEGORYCAL COLS & WHICH IS HAVING MISSING PCT IS LESS THAN 20% """
        try:
            mode = df[var].mode()[0]
            df[var] = df[var].fillna(mode)
            self.logger.logs("logs", "preprocessing",
                             "impute_by_mode completed")
            return df
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in impute_by_mode of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e

    def impute_by_median(self, df, var):
        self.logger.logs("logs", "preprocessing",
                         "entered into impute_by_median method of Preprocessing_class")

        """THIS METHOD IS USED FOR CONTINUOUS COLS & WHICH IS HAVING MISSING_PCT IS LESS THAN 20% """
        try:
            median = df[var].median()
            df[var] = df[var].fillna(median)
            self.logger.logs("logs", "training",
                             "impute_by_median completed")
            return df
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in impute_by_median of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e

    def impute_by_capture_nan_in_continuous_cols(self, df, var):
        self.logger.logs("logs", "preprocessing",
                         "entered into impute_by_capture_nan_in_continuous_cols method of Preprocessing_class")

        """ THIS METHOD IS USED FOR CONTINUOUS COL & WHICH IS HAVING MISSING_PCT IS >= 20% """

        try:
            median_cap = df[var].median()
            df[var + "_nan"] = np.where(df[var].isnull(), 1, 0)
            df[var] = df[var].fillna(median_cap)
            self.logger.logs("logs", "preprocessing",
                             "impute_by_capture_nan_in_continuous_cols completed")
            return df
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in impute_by_capture_nan_in_continuous_cols of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e
    def segragate_colums_for_encoding(self,df):
        self.logger.logs("logs", "preprocessing",
                         "entered into segragate_colums_for_encoding method of Preprocessing_class")

        try:
            num_cols = [col for col in df.columns if df[col].dtypes != "O"]

            cat_cols = [col for col in df.columns if df[col].dtypes == "O"]

            discreate_col = [col for col in num_cols if df[col].nunique() < 15]

            continuous_col = [col for col in num_cols if col not in discreate_col]
            self.logger.logs("logs", "preprocessing",
                             "segragate_colums_for_encoding completed")
            return cat_cols,discreate_col,continuous_col
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in segragate_colums_for_encoding of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e

    def two_cat_list(self,cat_cols_list,df):
        self.logger.logs("logs", "preprocessing",
                         "entered into two_cat_list method of Preprocessing_class")

        try:
            two_cat_lis = []
            for col in cat_cols_list:
                if df[col].nunique() == 2:
                    two_cat_lis.append(col)
            self.logger.logs("logs", "preprocessing",
                             "two_cat_list completed")
            return two_cat_lis
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in two_cat_list of Preprocessing_class ")
            self.logger.logs("logs", "training", "Exception Message : " + str(e))

            raise e

    def one_cat_list(self,cat_cols_list,df):
        self.logger.logs("logs", "preprocessing",
                         "entered into one_cat_list method of Preprocessing_class")

        try:
            one_cat_lis = []
            for col in cat_cols_list:
                if df[col].nunique() == 1:
                    one_cat_lis.append(col)
            self.logger.logs("logs", "preprocessing",
                             "one_cat_list completed")
            return one_cat_lis
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in one_cat_list of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e

    def get_dummy_for_two_cat(self, columns_list, xdata):
        self.logger.logs("logs", "preprocessing",
                         "entered into get_dummy_for_two_cat method of Preprocessing_class")

        try:
            self.new_data = pd.get_dummies(data=xdata, prefix_sep='_', columns=columns_list, drop_first=True)
            self.logger.logs("logs", "preprocessing",
                             "get_dummy_for_two_cat completed")
            return self.new_data
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in get_dummy_for_two_cat of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e


    def get_dummy_for_one_cat(self,columns_list,xdata):
        self.logger.logs("logs", "preprocessing",
                         "entered into get_dummy_for_one_cat method of Preprocessing_class")

        try:
            self.new_data = pd.get_dummies(data=xdata,columns=columns_list)
            self.logger.logs("logs", "training",
                             "get_dummy_for_one_cat completed")
            return self.new_data
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in get_dummy_for_one_cat of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e


    def get_dummy_for_more_than_two_cat(self, columns_list, xdata):
        self.logger.logs("logs", "preprocessing",
                         "entered into get_dummy_for_more_than_two_cat method of Preprocessing_class")

        try:
            self.df = pd.get_dummies(data=xdata, prefix_sep='_', columns=columns_list, drop_first=True)
            self.logger.logs("logs", "preprocessing",
                             "get_dummy_for_more_than_two_cat completed")
            return self.df
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in get_dummy_for_more_than_two_cat of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e

    def encode_class_label_ytrain(self, ytrain):
        self.logger.logs("logs", "preprocessing",
                         "entered into encode_class_label_ytrain method of Preprocessing_class")

        try:
            self.le = LabelEncoder().fit(ytrain)
            self.ytrain = self.le.transform(ytrain)

            self.s3.upload_ml_model(self.region,self.encoder_bucket,self.le,"classencoder")

            self.logger.logs("logs", "preprocessing",
                             "encode_class_label_ytrain completed")
            return self.ytrain
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in encode_class_label_ytrain of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e

    def decode_class_label_ypred(self,ypred):
        self.logger.logs("logs", "preprocessing",
                         "entered into decode_class_label_ypred method of Preprocessing_class")

        try:
            enco = self.s3.read_pickle_obj_from_s3_bucket(self.region, self.encoder_bucket, "classencoder.pickle")
            self.ypred = enco.inverse_transform(ypred)
            self.logger.logs("logs", "preprocessing",
                             "decode_class_label_ypred completed")
            return self.ypred
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in decode_class_label_ypred of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e

    def over_sampling_final_output(self, xtrain, ytrain):
        self.logger.logs("logs", "preprocessing",
                         "entered into over_sampling_final_output method of Preprocessing_class")

        try:
            os = RandomOverSampler()
            xtrain_final, ytrain_final = os.fit_resample(xtrain, ytrain)
            self.logger.logs("logs", "preprocessing",
                             "over_sampling_final_output completed")
            return xtrain_final, ytrain_final
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in over_sampling_final_output of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e

    def chi2_train(self, xtrain, ytrain):
        self.logger.logs("logs", "preprocessing",
                         "entered into chi2_train method of Preprocessing_class")

        try:
            chi = SelectKBest(score_func=chi2, k=12)
            chi.fit(xtrain, ytrain)
            xtrain_chi = chi.transform(xtrain)
            self.s3.upload_ml_model(self.region,self.encoder_bucket,chi,"chi2")

            self.logger.logs("logs", "preprocessing",
                             "chi2_train completed")
            return xtrain_chi
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in chi2_train of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e

    def chi2_test(self,xtest):
        self.logger.logs("logs", "preprocessing",
                         "entered into chi2_test method of Preprocessing_class")

        try:
            chi = self.s3.read_pickle_obj_from_s3_bucket(self.region,self.encoder_bucket,"chi2.pickle")
            xtest_chi = chi.transform(xtest)
            self.logger.logs("logs", "preprocessing",
                             "chi2_test completed")
            return xtest_chi
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in chi2_test of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e

    def saperate_label_and_feature(self,data,label_col_name):
        self.logger.logs("logs", "preprocessing",
                         "entered into saperate_label_and_feature method of Preprocessing_class")

        try:
            x = data.drop(labels=[label_col_name],axis=1)
            y = data[label_col_name]
            self.logger.logs("logs", "preprocessing",
                             "saperate_label_and_feature completed")
            return x,y
        except Exception as e:
            self.logger.logs("logs", "preprocessing",
                             "Exception occured in saperate_label_and_feature of Preprocessing_class ")
            self.logger.logs("logs", "preprocessing", "Exception Message : " + str(e))

            raise e



