from flask import Flask,request,Response,jsonify
from flask_cors import CORS,cross_origin
from trainValidation import TrainValidation_class
from Training import Training_class
from predictionvalidation import prediction_Validtion_class
from prediction_file import prediction_class
import sys
import os


# change the recursive limit
print(sys.getrecursionlimit())
sys.setrecursionlimit(10**6)
print(sys.getrecursionlimit())

application = Flask(__name__)
os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')
CORS(application)

@application.route("/train", methods=["POST"])
@cross_origin()
def TrainingRoute():
    try:
        if request.json is not None:
            bucket = request.json["bucket_name"]

            # validate the raw data
            trainvalidation_obj = TrainValidation_class(bucket)
            trainvalidation_obj.validate()
            # training
            training_obj = Training_class()
            training_obj.Train()
            return Response("Training successfull")

    except Exception as e:
        return Response("Training Failed ,Error  :  {}".format(str(e)))


@application.route("/predict", methods=["POST"])
@cross_origin()
def PredictRoute():
    try:
        if request.json is not None:
            bucket = request.json["bucket_name"]

            predictvalObj = prediction_Validtion_class(bucket)
            predictvalObj.Validate_predictions()

            predictionObj = prediction_class()
            result = predictionObj.prediction()
            return jsonify(result)

    except Exception as e:
        return Response("Error Occured : {}".format(str(e)))


# port = os.getenv("PORT",5000)
if __name__ == "__main__":
    #host = '0.0.0.0'
    # port = 5000
    #httpd = simple_server.make_server(host, port,application)
    # print("Serving on %s %d" % (host, port))
    #print("model serving")
    #httpd.serve_forever()
    application.run(debug=True)




