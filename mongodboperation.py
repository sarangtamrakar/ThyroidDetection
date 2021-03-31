import pymongo
import pandas as pd
import json
from s3_operation import s3_bucket_operation


class mongodb:
    def __init__(self):
        # getting userid & passwd from private location..
        with open('private_property.json', 'r') as f:
            dic = json.load(f)
            f.close()

        userid = dic.get('mongo_userid', None)
        passwd = dic.get('mongo_passwd', None)


        self.conn_string = 'mongodb+srv://{}:{}@cluster0.fcuwl.mongodb.net/myFirstDatabase?authSource=admin&connectTimeoutMS=60000'.format(userid,passwd)
        self.s3 = s3_bucket_operation()
        self.region = 'us-east-2'



    def create_conn_to_db(self):
        try:
            conn = pymongo.MongoClient(self.conn_string)
            return conn
        except Exception as e:
            raise e

    def close_conn(self,conn):
        try:
            conn.close()
        except Exception as e:
            raise e


    def create_database(self,database_name,conn):
        try:
            mydb = conn[database_name]
            return mydb
        except Exception as e:
            raise e

    def create_collection(self,db_obj,collection_name):
        try:
            collection = db_obj[collection_name]
            return collection
        except Exception as e:
            raise e

    def list_of_db_in_server(self,conn):
        try:
            list_of_db = conn.list_database_names()
            self.close_conn(conn)
            return list_of_db
        except Exception as e:
            raise e

    def list_of_collection_in_db(self,db_obj,conn_obj):
        try:

            list_of_collection = db_obj.list_collection_names()
            self.close_conn(conn_obj)

            return list_of_collection
        except Exception as e:
            raise e

    def insert_one_record(self,data_dict,db_name,collection_name):
        try:
            conn = self.create_conn_to_db()
            db_obj = self.create_database(db_name,conn)
            col_obj = self.create_collection(db_obj,collection_name)
            col_obj.insert_one(data_dict)
            self.close_conn(conn)
            # print('inserted one sucessfully')
        except Exception as e:
            raise e

    def insert_many_record(self,data_list,db_name,collection_name):
        try:
            conn = self.create_conn_to_db()
            db_obj = self.create_database(db_name, conn)
            col_obj = self.create_collection(db_obj, collection_name)
            col_obj.insert_many(data_list)
            self.close_conn(conn)
            print('inserted many sucessfully')
        except Exception as e:
            raise e

    def insert_dataframe(self,dataframe,collection_name,db_name):
        try:
            record = dataframe.to_dict('records')
            conn = self.create_conn_to_db()
            db_obj = self.create_database(db_name,conn)
            col_obj = self.create_collection(db_obj, collection_name)
            col_obj.insert_many(record)
            self.close_conn(conn)

        except Exception as e:
            raise e

    def read_dataframe(self,collection_name,db_name):
        try:
            conn = self.create_conn_to_db()
            db_obj = self.create_database(db_name, conn)
            col_obj = self.create_collection(db_obj, collection_name)
            df = pd.DataFrame(list(col_obj.find()))
            self.close_conn(conn)

            if '_id' in list(df.columns):
                df = df.drop(columns=['_id'],axis=1)

            return df

        except Exception as e:
            raise e


    def read_dataframe_from_db_and_dump_into_bucket(self,dbname,collection_name,bucket_name,key):
        try:
            dataframe = self.read_dataframe(collection_name,dbname)
            self.s3.upload_dataframe_to_bucket(self.region,bucket_name,key,dataframe)
        except Exception as e:
            raise e




    def dump_all_dataframe_into_mongo_db(self,db_name,collection_name,bucket_name):
        try:
            lis = self.s3.getting_list_of_object_in_bucket(self.region,bucket_name)
            for file in lis:
                df = self.s3.read_csv_obj_from_s3_bucket(self.region,bucket_name,file)
                record = df.to_dict('records')
                conn = self.create_conn_to_db()
                db_obj = conn[db_name]
                col_obj = db_obj[collection_name]
                col_obj.insert_many(record)
                self.close_conn(conn)
        except Exception as e:
            raise e
    def drop_collection_before_prediction(self,dbname,collection_name):
        try:
            conn = self.create_conn_to_db()
            db_obj = self.create_database(dbname,conn)
            col_obj = self.create_collection(db_obj,collection_name)
            col_obj.drop()
            self.close_conn(conn)
        except Exception as e:
            raise e


