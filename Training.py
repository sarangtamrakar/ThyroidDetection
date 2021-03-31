from Application_logging.logging_file import logging_class
from Best_model_finder.tuner_file import Model_finder_class
from DataIngestion.DataLoader_file import data_loader_class
from DataPreprocessing.Preprocessing_file import Preprocessing_class
from sklearn.model_selection import train_test_split
from Fileoperation.FileOpeartion_file import FileOperation_class


class Training_class:
    def __init__(self):
        self.logger = logging_class()
        self.model_finder = Model_finder_class()
        self.data_loader = data_loader_class()
        self.preprocessing = Preprocessing_class()
        self.file_operation = FileOperation_class()


    def Train(self):
        try:
            self.logger.logs("logs","training","TRAINING START!!!")

            # loading the dataset
            data = self.data_loader.training_loader()

            # remove the unnecessory fetures
            col_to_remove = ["TSH_measured","T3_measured","TT4_measured","T4U_measured","FTI_measured","TBG_measured","TBG","hypopituitary"]

            data = self.preprocessing.remove_columns(data,col_to_remove)

            data = self.preprocessing.replace_invalid_values_by_null(data)

            # correct the datatype
            data = self.preprocessing.correct_data_type(data)

            # segregate the columns based on the dtype for imputation
            cat_cols,discreate_col,continuous_col = self.preprocessing.segregate_column_names_for_imputation(data)

            # impute the cat_cols by mode values
            for col in cat_cols:
                data = self.preprocessing.impute_by_mode(data,col)

            # impute continuous col by median
            for col in continuous_col:
                data = self.preprocessing.impute_by_median(data,col)

            # impute by capturing nan method for continuous which is basically missing pct
            # is greater than 20%
            data = self.preprocessing.impute_by_capture_nan_in_continuous_cols(data,"T3")

            # separate the label & feature
            x,y = self.preprocessing.saperate_label_and_feature(data,"Class")

            # segragate the columns based on dtypes for encoding
            cat_cols,discreate_col,continuous_col = self.preprocessing.segragate_colums_for_encoding(data)

            # getting list of cat columns which has only two categories
            two_cat_list = self.preprocessing.two_cat_list(cat_cols,data)

            # getting one cat list which columns has only one categories
            one_cat_list = self.preprocessing.one_cat_list(cat_cols,data)

            # encoding two cat columns
            x = self.preprocessing.get_dummy_for_two_cat(columns_list=two_cat_list, xdata=x)

            if len(one_cat_list) > 0:
                x = self.preprocessing.get_dummy_for_one_cat(columns_list=one_cat_list,xdata=x)
            else:
                pass



            # encoding the columns which has nominal datatype & more than 2 categories
            x = self.preprocessing.get_dummy_for_more_than_two_cat(columns_list=["referral_source"], xdata=x)

            # encoding the class label by label Encoder
            y = self.preprocessing.encode_class_label_ytrain(ytrain=y)

            # handling the imbalanced dataset , applying the over sampling
            x,y = self.preprocessing.over_sampling_final_output(x,y)

            # feature selection by the chi2 test
            x = self.preprocessing.chi2_train(x,y)

            # train test using stratified so that class proportion will be same in train test.
            xtrain,xtest,ytrain,ytest = train_test_split(x,y,test_size=0.25,random_state=22,stratify=y)

            # Hyper parameter tuning & best model getting
            model_name,model=self.model_finder.get_best_model(xtrain,ytrain,xtest,ytest)

            # saving the best model into s3 bucket
            self.file_operation.save_model(model,model_name)

            self.logger.logs("logs","training","Training completed")
        except Exception as e:
            self.logger.logs("logs","training","training Failed")
            self.logger.logs("logs","training","Exception : "+str(e))
            raise e

