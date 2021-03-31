
try:
    import boto3
    import pickle
    import json
    import os
    import sys
    import pandas as pd
    print('importing library sucessfully')
except Exception as e:
    print('exception occured while importing library,Exception :'+str(e))


class s3_bucket_operation:

    def __init__(self):
        # getting access_key & passwd from private location..
        with open("private_property.json", "r") as f:
            dic = json.load(f)
            f.close()

        aws_access_key_id = dic.get('aws_access_key_id', None)
        aws_secret_access_key = dic.get('aws_secret_access_key', None)

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key



    def create_resource_to_s3(self,region_name):

        try:
            re = boto3.resource('s3',region_name=region_name,aws_access_key_id=self.aws_access_key_id,aws_secret_access_key=self.aws_secret_access_key)


            return re
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to create conn to s3,Exception :'+str(e))
            raise e



    def create_bucket(self,region_name,bucket_name):
        try:
            re = self.create_resource_to_s3(region_name)
            response = re.meta.client.create_bucket(Bucket=bucket_name,CreateBucketConfiguration={'LocationConstraint': region_name})
            # self.logger.logs('logs', 's3_bucket', 'we have create the bucket sucessfully')

        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to create the bucket,Exception :'+str(e))
            raise e



    def get_list_of_bucket_in_s3(self,region_name):
        try:
            lis = []
            re = self.create_resource_to_s3(region_name)
            for buck in re.meta.client.list_buckets().get("Buckets",None):
                lis.append(buck.get('Name',None))
            # self.logger.logs('logs','s3_bucket','we have get the list of bucket in s3')
            return lis
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to get the list of bucket,Exception :'+str(e))
            raise e



    def getting_list_of_object_in_bucket(self,region_name,bucket_name):
        try:
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            lis = []
            for i in buck.meta.client.list_objects(Bucket=bucket_name).get('Contents', None):
                lis.append(i.get('Key', None))
            return lis

            # self.logger.logs('logs','s3_bucket','we have get the list of objects in bucket :'+str(bucket_name))
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to get the list of objects in bucket:'+str(bucket_name))
            raise e


    def getting_special_list_of_object_in_bucket(self, region_name, bucket_name):
        try:
            """ this is design for Kmeans clustering model specially 
            works well with if - else condition """
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            try:
                lis = []
                for i in buck.meta.client.list_objects(Bucket=bucket_name).get('Contents', None):
                    lis.append(i.get('Key', None))
                return lis
            except:
                return [",", ","]
        except Exception as e:
            raise e




    def getting_list_of_objs_in_folder_of_bucket(self,region_name,bucket_name,folder_name):
        try:
            re = self.create_resource_to_s3(region_name)
            lis = []
            for i in re.meta.client.list_objects(Bucket=bucket_name,Prefix=folder_name).get('Contents',None):
                lis.append(i.get('Key',None))
            # self.logger.logs('logs','s3_bucket','we have get the list of obj in folder'+str(folder_name)+'of bucket'+str(bucket_name))
            return lis


        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to get the list of bucket in folder'+str(folder_name)+'of bucket'+str(bucket_name))
            raise e



    def put_obj_in_bucket_folder(self,region_name,bucket_name,obj_path,folder_name,ACL='public-read-write'):
        try:
            with open(obj_path,'rb') as f:
                data = f.read()
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            res = buck.meta.client.put_object(ACL=ACL,Body=data,Bucket=bucket_name,Key=str(folder_name+'/'+obj_path))
            # self.logger.logs('logs','s3_bucket','we have put obj :'+str(obj_path)+'response:'+str(res))

        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to put object in bucket'+str(e)+'object'+str(obj_path))
            raise e



    def put_obj_in_bucket(self, region_name, bucket_name, obj_path, ACL='public-read-write'):
        try:
            with open(obj_path, 'rb') as f:
                data = f.read()
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            res = buck.meta.client.put_object(ACL=ACL, Body=data, Bucket=bucket_name,Key=str(obj_path))
            # self.logger.logs('logs', 's3_bucket', 'we have put obj :' + str(obj_path) + 'response:' + str(res))

        except Exception as e:
            # self.logger.logs('logs', 's3_bucket','we have failed to put object in bucket' + str(e) + 'object' + str(obj_path))
            raise e



    def delete_single_obj_in_bucket(self,region_name, bucket_name, obj_id):
        try:
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            res = buck.meta.client.delete_object(Bucket=bucket_name, Key=obj_id)
            # self.logger.logs('logs','s3_bucket','we have deleted the object :'+str(obj_id)+'in bucket'+str(bucket_name))

        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to delete the obj :'+str(obj_id)+'exception :'+str(e))
            raise e



    def delete_particular_folder_in_bucket(self,region_name, bucket_name, folder_name):
        try:
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            res = buck.objects.filter(Prefix=folder_name).delete()
            # self.logger.logs('logs','s3_bucket','we have deleted the complete folder'+str(folder_name)+'in bucket:'+str(bucket_name))

        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to delete complete folder'+str(folder_name)+'in bucekt :'+str(bucket_name))
            raise e



    def delete_all_objects_in_bucket(self,region_name,bucket_name):
        try:
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            res = buck.objects.all().delete()
            # self.logger.logs('logs','s3_bucket','we have deleted the all objects in bucket :'+str(bucket_name))
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to delete all objects in bucket'+str(bucket_name))
            raise e



    def delete_bucket_with_all_objects(self,region_name,bucket_name):
        try:
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            res = buck.objects.all().delete()
            res = buck.delete()
            # self.logger.logs('logs','s3_bucket','we have deleted bucket'+str(bucket_name))
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to delete bucket'+str(bucket_name)+'Exception :'+str(e))

            raise e



    def read_csv_obj_from_s3_bucket(self,region_name, bucket_name, key):
        try:
            re = self.create_resource_to_s3(region_name)
            response = re.meta.client.get_object(Bucket=bucket_name, Key=key)
            data = response.get('Body', None)
            df = pd.read_csv(data)
            # self.logger.logs('logs','s3_bucket','we have read file from s3 ,File:'+str(key))

            return df
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to read csv file '+str(key))
            raise e



    def read_pickle_obj_from_s3_bucket(self,region_name, bucket_name, key):
        try:
            re = self.create_resource_to_s3(region_name)
            response = re.meta.client.get_object(Bucket=bucket_name, Key=key)
            data = response.get('Body', None).read()
            model = pickle.loads(data)
            # self.logger.logs('logs','s3_bucket','we have read the pickle file:'+str(key))

            return model
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to read the pickle file,Exception:'+str(e))
            raise e



    def read_json_obj_from_s3_bucket(self,region_name, bucket_name, key):
        try:
            re = self.create_resource_to_s3(region_name)
            response = re.meta.client.get_object(Bucket=bucket_name, Key=key)
            data = response.get('Body', None)
            file = json.load(data)

            # self.logger.logs("logs","s3_bucket","we have read the json file from s3 , File : "+str(key))

            return file
        except Exception as e:
            # self.logger.logs("logs","s3_bucket","Exception Occured while reading Json File : "+str(key))

            raise e





    def copy_file_to_another_bucket(self,region_name, bucket_from, bucket_to, key):
        try:
            re = boto3.resource('s3', region_name=region_name)
            buck = re.Bucket(bucket_from)
            res = buck.meta.client.copy_object(Bucket=bucket_to,CopySource={'Bucket': bucket_from, 'Key': key}, Key=str(key))
            # self.logger.logs('logs','s3_bucket','we have copy file to'+str(bucket_to))
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to copy file'+str(key)+' bucket_to :'+str(bucket_to))
            raise e


    def copy_file_to_another_bucket_folder(self,region_name, bucket_from, bucket_to, key,folder_name):
        try:
            re = boto3.resource('s3', region_name=region_name)
            buck = re.Bucket(bucket_from)
            res = buck.meta.client.copy_object(Bucket=bucket_to,CopySource={'Bucket': bucket_from, 'Key': key}, Key=str(folder_name+"/"+key))
            # self.logger.logs('logs','s3_bucket','we have copy file to'+str(bucket_to))
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to copy file'+str(key)+' bucket_to :'+str(bucket_to))
            raise e





    def create_folder_in_bucket(self,region_name, bucket_name, folder_name):
        try:
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            buck.meta.client.put_object(Bucket=bucket_name, Key=folder_name + '/')
            # self.logger.logs('logs','s3_bucket','we have create the folder :'+str(folder_name))
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have failed to create the folder'+str(folder_name))
            raise e



    def delete_folder_in_bucket(self,region_name,bucket_name,folder_name):
        try:
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            buck.meta.client.delete_object(Bucket=bucket_name, Key=folder_name + '/')
            # self.logger.logs('logs','s3_bucket', 'we have deleted the folder :' + str(folder_name))
        except Exception as e:
            # self.logger.logs('logs','s3_bucket', 'we have failed to delete the folder :' + str(folder_name))
            raise e


    def upload_dataframe_to_bucket(self,region_name, bucket_name, key, dataframe):
        try:
            data = dataframe.to_csv(encoding='utf-8', header=True, index=None)
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            res = buck.meta.client.put_object(Body=data, Bucket=bucket_name, Key=key)
            # self.logger.logs('logs','s3_bucket','we have upload the dataframe to the bucket : '+str(bucket_name))
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have upload the dataframe to bucket : '+str(bucket_name))
            raise e


    def upload_result_dataframe_to_bucket(self,region_name, bucket_name, key, dataframe):
        try:
            # here mode = Append
            data = dataframe.to_csv(encoding='utf-8', header=True, index=None,mode="a+")
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)
            res = buck.meta.client.put_object(Body=data, Bucket=bucket_name, Key=key)
            # self.logger.logs('logs','s3_bucket','we have upload the dataframe to the bucket : '+str(bucket_name))
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','we have upload the dataframe to bucket : '+str(bucket_name))
            raise e




    def upload_ml_model(self,region_name, bucket_name, model, filename):
        try:
            re = self.create_resource_to_s3(region_name)
            buck = re.Bucket(bucket_name)

            # convert ml model into serialized bytes format..
            data = pickle.dumps(model)

            # filename must be in .pickle extension
            res = buck.meta.client.put_object(Body=data, Bucket=bucket_name, Key=str(filename+'.pickle'))
        except Exception as e:
            # self.logger.logs('logs','s3_bucket','')
            raise e