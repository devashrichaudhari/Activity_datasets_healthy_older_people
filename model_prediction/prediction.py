# performing important imports
import pandas as pd
import os
import numpy as np
from application_logging.logger import AppLogger
from prediction_data_validation.prediction_data_validation import PredictionDataValidation
from file_operation.file_handler import FileHandler


class Prediction:
    def __init__(self):
        self.logger = AppLogger()
        self.logger.database.connect_db()
        self.table_name = 'prediction_log'
        self.pred_data_val = PredictionDataValidation()

    def predict(self):
        """
        This function applies prediction on the provided data
        :return: output- Prediction
                 probablity- Probablity of predicted class
        """

        try:
            self.logger.log(self.table_name, 'Start of Prediction', 'Info')
            # initializing FileHandler object
            file_handler = FileHandler(self.table_name, self.logger)
            # getting the data file path
            file = os.listdir('Input_data/')[0]
            # reading data file
            dataframe = pd.read_csv('Input_data/'+file)
            data = dataframe.copy()
            data = np.array(data)
            # loading Random Forest Classifier model
            random_forest_classifier = file_handler.load_model('RandomForestClassifier')
            # predicting
            predicted = random_forest_classifier.predict(data)
            self.logger.log(
                self.table_name,
                'Predction complete!!. Exiting Predict method of Prediction class ',
                'Info')
            # self.logger.database.close_connection()
            return predicted

        except Exception as e:
            self.logger.log(
                self.table_name,
                'Error occured while running the prediction!! Message: ' + str(e),
                'Error')
            self.logger.database.close_connection()
            raise e
