# performing important imports
import json
import os
import shutil
import pandas as pd
import numpy as np
from application_logging.logger import AppLogger


class PredictionDataValidation:
    def __init__(self):
        self.logger = AppLogger()
        self.schema = 'prediction_schema.json'
        self.logger.database.connect_db()

    def delete_prediction_files(self):
        """
        Deletes the prediction_logs directory and it's content
        """
        table_name = 'folder_handler'
        try:
            self.logger.log(
                table_name,
                'Entered deletePredictionFiles method of PredictionDataValidation class',
                'Info')
            shutil.rmtree('Input_data/')
            self.logger.log(table_name, 'Input_data deleted.', 'Info')
        except Exception as e:
            self.logger.log(
                table_name,
                'Error occured in deleting folder in deletePredictionFiles method of \
                PredictionDataValidation class. Message: '+str(e),
                'Error')
            self.logger.log(table_name,
                            'Failed to delete folder.',
                            'Error')
            self.logger.database.close_connection()
            raise e

    def create_prediction_files(self, folder_name):
        """
        Creates new directory
        :param folder_name: Name of the folder to create
        """

        table_name = 'folder_handler'
        try:
            self.logger.log(
                table_name,
                'Entered createPredictionFiles method of PredictionDataValidation class',
                'Info')
            os.mkdir(f'{folder_name}/')
            self.logger.log(table_name, 'Input_data created.')
        except Exception as e:
            self.logger.log(table_name,
                            'Error occured in creating folder in createPredictionFiles method of\
                             PredictionDataValidation class. Message: ' + str(e),
                            'Error')
            self.logger.log(table_name,
                            'Failed to create folder.',
                            'Error')
            self.logger.database.close_connection()
            raise e

    def get_schema_values(self):
        """
        Retrives important data from Schema
        :return:
        """

        table_name = 'value_from_schema_log'
        try:

            self.logger.log(table_name, 'Entered getSchemaValue method of PredictionDataValidation class', 'Info')
            with open(self.schema, 'r') as f:
                dic = json.load(f)
                f.close()
            required_columns = dic["RequiredColumns"]

            message = "RequiredColumns: "+str(required_columns)+"\n"
            self.logger.log(table_name, message, 'Info')

        except ValueError as v:
            message = "ValueError:Value not found inside Schema_prediction.json"
            self.logger.log(table_name, message, 'Error')
            self.logger.database.close_connection()
            raise v

        except KeyError as k:
            message = "KeyError:key value error incorrect key passed"
            self.logger.log(table_name, message, 'Error')
            self.logger.database.close_connection()
            raise k

        except Exception as e:
            self.logger.log(table_name, str(e), 'Error')
            self.logger.database.close_connection()
            raise e
        # returning tuple of these 3 values
        return required_columns

    def validate_data_type(self):
        """
        Validate the incoming datatype
        """

        table_name = 'validation_log'
        try:
            self.logger.log(table_name, 'Entered ValidateDataType method of PredictionDataValidation class', 'Info')
            data = pd.read_csv('Input_data/input.csv')
            for i in data.columns:
                if data[[i]].dtypes[0] == np.int64 or data[[i]].dtypes[0] == np.float64:
                    pass
                elif i == 'Region' and data[[i]].dtypes[0] == np.dtype('O'):
                    pass
                else:
                    self.logger.log(table_name, 'Failed valiadtion. Exiting.....', 'Error')
                    raise Exception('Different Datatype found..')
            self.logger.log(
                table_name,
                'Datatype validation complete exiting ValidateDataType method of PredictionDataValidation class',
                'Info')
        except Exception as e:
            self.logger.log(table_name, 'Error occured in Validating datatypes. Message: '+str(e), 'Error')
            self.logger.log(table_name, 'Failed to validate datatype. Exiting.....', 'Error')
            self.logger.database.close_connection()
            raise e
