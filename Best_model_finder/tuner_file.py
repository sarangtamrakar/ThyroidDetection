from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier,GradientBoostingClassifier
from sklearn.tree import DecisionTreeClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score,f1_score
import pandas as pd
from Application_logging.logging_file import logging_class
from datetime import datetime

class Model_finder_class:
    def __init__(self):
        self.rf = RandomForestClassifier()
        self.gb = GradientBoostingClassifier()
        self.dt = DecisionTreeClassifier()
        self.xgb = XGBClassifier()
        self.logger = logging_class()


    def get_best_rf(self,xtrain,ytrain):
        self.logger.logs("logs","training","entered into get_best_rf method of Model_finder_class")

        try:
            grid = {
                "n_estimators":[50,100,150,200,250,300],
                "criterion":["gini","entropy"],
                "max_depth":[3,5,10,20],
                "max_features":["auto","sqrt","log2"]
            }
            
            self.grid = GridSearchCV(self.rf,grid,scoring="f1_macro",cv=5,verbose=3,n_jobs=-1)
            self.grid.fit(xtrain,ytrain)

            n_estimators = self.grid.best_params_["n_estimators"]
            criterion = self.grid.best_params_["criterion"]
            max_depth = self.grid.best_params_["max_depth"]
            max_features = self.grid.best_params_["max_features"]

            clf = RandomForestClassifier(n_estimators=n_estimators,criterion=criterion,max_depth=max_depth,max_features=max_features)
            clf.fit(xtrain,ytrain)
            self.logger.logs("logs","training","Best param for RandomForestClassifier ===> n_estimators : {},criterion : {}, max_depth :{},max_features : {} ".format(n_estimators,criterion,max_depth,max_features))

            return clf
        except Exception as e:
            self.logger.logs("logs", "training", "Exception occured in the get_best_rf ModelFinder_class")
            self.logger.logs("logs", "training", "Exception Message : " + str(e))
            raise e

    def get_best_gb(self,xtrain,ytrain):
        self.logger.logs("logs", "training", "entered into  get_best_gb method of Model_finder_class")

        try:
            grid = {
                "learning_rate":[0.001,0.01,0.1,1,2,3,4],
                "n_estimators":[50,100,200,300,400],
                "max_depth":[2,3,5,7,9],
                "max_features":["auto","sqrt","log2"]
                }
            self.grid = GridSearchCV(self.gb,grid,scoring="f1_macro",cv=5,verbose=5,n_jobs=-1)
            self.grid.fit(xtrain,ytrain)

            learning_rate = self.grid.best_params_["learning_rate"]
            n_estimators = self.grid.best_params_["n_estimators"]
            max_depth = self.grid.best_params_["max_depth"]
            max_features = self.grid.best_params_["max_features"]

            clf = GradientBoostingClassifier(learning_rate=learning_rate,n_estimators=n_estimators,max_depth=max_depth,max_features=max_features)
            clf.fit(xtrain,ytrain)
            self.logger.logs("logs", "training",
                             "Best param for GradientBoostingClassifier ===> learning_rate : {},n_estimators : {}, max_depth :{},max_features : {} ".format(
                                 learning_rate, n_estimators, max_depth, max_features))

            return clf
        except Exception as e:
            self.logger.logs("logs", "training", "Exception occured in the get_best_gb ModelFinder_class")
            self.logger.logs("logs", "training", "Exception Message : " + str(e))
            raise e

    def get_best_dt(self,xtrain,ytrain):
        self.logger.logs("logs", "training", "entered into get_best_dt method of Model_finder_class")

        try:
            param = {
                "criterion":["gini", "entropy"],
                "max_depth":[2,3,5,7,9],
                "max_features":["auto", "sqrt", "log2"]
            }
            self.grid = GridSearchCV(self.dt,param,scoring="f1_macro",cv=5,verbose=3,n_jobs=-1)
            self.grid.fit(xtrain,ytrain)

            criterion = self.grid.best_params_["criterion"]
            max_depth = self.grid.best_params_["max_depth"]
            max_features = self.grid.best_params_["max_features"]

            clf = DecisionTreeClassifier(criterion=criterion,max_depth=max_depth,max_features=max_features)
            clf.fit(xtrain,ytrain)

            self.logger.logs("logs", "training",
                             "Best param for GradientBoostingClassifier ===> criterion : {},max_depth : {}, max_features :{}".format(
                                 criterion, max_depth, max_depth, max_features))

            return clf
        except Exception as e:
            self.logger.logs("logs", "training", "Exception occured in the get_best_dt ModelFinder_class")
            self.logger.logs("logs", "training", "Exception Message : " + str(e))
            raise e

    def get_best_xgb(self,xtrain,ytrain):
        self.logger.logs("logs", "training", "entered into get_best_xgb method of Model_finder_class")

        try:
            param = {
                "max_depth":[2,3,5,7,9],
                "learning_rate":[0.001,0.01,0.1,1,2,3,4],
                "n_estimators":[50,100,200,300,400]
            }
            self.grid = GridSearchCV(self.xgb,param,scoring="f1_macro",cv=5,verbose=3,n_jobs=-1)
            self.grid.fit(xtrain,ytrain)

            max_depth = self.grid.best_params_["max_depth"]
            learning_rate = self.grid.best_params_["learning_rate"]
            n_estimators = self.grid.best_params_["n_estimators"]

            clf = XGBClassifier(max_depth=max_depth,learning_rate=learning_rate,n_estimators=n_estimators)
            clf.fit(xtrain,ytrain)
            self.logger.logs("logs", "training",
                             "Best param for XGBClassifier ===> max_depth : {},learning_rate : {}, n_estimators :{}".format(
                                 max_depth, learning_rate,n_estimators))

            return clf
        except Exception as e:
            self.logger.logs("logs", "training", "Exception occured in the get_best_xgb ModelFinder_class")
            self.logger.logs("logs", "training", "Exception Message : " + str(e))
            raise e

    def get_best_model(self,xtrain,ytrain,xtest,ytest):
        self.logger.logs("logs", "training", "entered into get_best_model method of Model_finder_class")

        try:
            ytest = pd.Series(ytest)

            # metrics of rf
            rf_clf = self.get_best_rf(xtrain,ytrain)
            ypred_rf = rf_clf.predict(xtest)

            if len(ytest.unique()) == 1:
                score_rf = accuracy_score(ytest,ypred_rf)
            else:
                score_rf = f1_score(ytest,ypred_rf,average="macro")



            # metrics of gb
            gb_clf = self.get_best_gb(xtrain,ytrain)
            ypred_gb = gb_clf.predict(xtest)

            if len(ytest.unique()) == 1:
                score_gb = accuracy_score(ytest,ypred_gb)
            else:
                score_gb = f1_score(ytest,ypred_gb,average="macro")


            # metrics of dt
            dt_clf = self.get_best_dt(xtrain,ytrain)
            ypred_dt = dt_clf.predict(xtest)

            if len(ytest.unique()) == 1:
                score_dt = accuracy_score(ytest,ypred_dt)
            else:
                score_dt = f1_score(ytest,ypred_dt,average="macro")


            # metrics of xgb
            xgb_clf = self.get_best_xgb(xtrain,ytrain)
            ypred_xgb = xgb_clf.predict(xtest)

            if len(ytest.unique()) == 1:
                score_xgb = accuracy_score(ytest,ypred_xgb)
            else:
                score_xgb = f1_score(ytest,ypred_xgb,average="macro")


            self.now = datetime.now()
            self.date = self.now.date()
            self.current_time = self.now.strftime("%H%M%S")

            self.logger.logs("logs","modelmetrics","Date :{} , Time : {}".format(str(self.date),str(self.current_time)))
            self.logger.logs("logs","modelmetrics","RandomForest Score : "+str(score_rf))
            self.logger.logs("logs","modelmetrics","DecisionTree Score : "+str(score_dt))
            self.logger.logs("logs","modelmetrics","Gradient Boost Score : "+str(score_gb))
            self.logger.logs("logs","modelmetrics","XGBoostClassifier Score : "+str(score_xgb))


            if (score_rf > score_dt) & (score_rf > score_gb) & (score_rf > score_xgb):
                return "RandomForest",rf_clf
            elif (score_dt > score_rf) & (score_dt > score_gb) & (score_dt > score_xgb):
                return "DecisionTree",dt_clf
            elif (score_gb > score_rf) & (score_gb > score_dt) & (score_gb > score_xgb):
                return "GradientBoost",gb_clf
            else:
                return "XGBoost",xgb_clf

        except Exception as e:
            self.logger.logs("logs", "training", "Exception occured in the get_best_model ModelFinder_class")
            self.logger.logs("logs","training","Exception Message : "+str(e))
            raise e

