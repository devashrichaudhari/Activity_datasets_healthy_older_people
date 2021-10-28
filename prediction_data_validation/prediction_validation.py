# performing important imports
from application_logging.logger import AppLogger
from prediction_data_validation.prediction_data_validation import PredictionDataValidation


class PredictionValidation:
    def __init__(self):
        self.raw_data = PredictionDataValidation()
        self.logger = AppLogger()
        self.logger.database.connect_db()

    def validation(self):
        """
        Calling the validation
        """

        table_name = 'prediction_log'
        try:
            self.logger.log(table_name, "Validation started for Prediction Data", "Info")
            # validating Datatype
            self.logger.log(table_name, "Starting datatype validation", "Info")
            self.raw_data.validate_data_type()
            self.logger.log(table_name, "Datatype validation complete!!", "Info")
        except Exception as e:
            self.logger.log(table_name, "Error ocurred while performing validation. Error: " + str(e), "Error")
            self.logger.database.close_connection()
            raise e
