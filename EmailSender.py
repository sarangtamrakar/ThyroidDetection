import json
import smtplib
import ssl
from Application_logging.logging_file import logging_class

class emailSenderClass:
    def __init__(self):
        with open("private_property.json", "r") as f:
            data = json.load(f)

        # define the sender , password & receiver
        sender = data.get("userid")
        password = data.get("password")
        receiver = "sarang.tamrakarsgi15@gmail.com"

        self.sender = sender
        self.password = password
        self.receiver = receiver
        self.logger = logging_class()

    def send_message(self,bucket_name,file_list):
        self.logger.logs("logs","emaillogs","we have entered into send_message method of emailSenderClass")

        try:
            port = 465  # For SSL
            smtp_server = "smtp.gmail.com"

            message = """Subject: BAD FILE INFORMATION OF THYROID DETECTION PROJECT !!!


            bucket location ==> {bucket}
            filename list ===> {filename}
            """.format(bucket = bucket_name,filename=file_list)

            context = ssl.create_default_context()

            self.logger.logs("logs","emaillogs","start sending email")
            with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
                server.login(self.sender, self.password)
                server.sendmail(self.sender, self.receiver, message)
                server.quit()

            self.logger.logs("logs","emaillogs","Email Sent Successfully, Bucket Location : "+str(bucket_name)+" file name list : "+str(file_list))


        except Exception as e:
            self.logger.logs("logs", "emaillogs", "Exception occured into send_message method of emailSenderClass")
            self.logger.logs("logs","emaillogs","Exception Message : "+str(e))

            raise e
