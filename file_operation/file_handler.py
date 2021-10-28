# performing important imports
import pickle


class FileHandler:
    def __init__(self, table_name, logger_object):
        self.table_name = table_name
        self.logger = logger_object
        self.model_path = 'models/'

    def load_model(self, filename):
        """
        This function helps to load different .sav files
        :param filename: Name of the file to create
        :return: FileObject
        """

        try:
            self.logger.log(self.table_name, 'Entered the loadModel method of FileHandler class', 'Info')
            with open(self.model_path+filename+'.sav', 'rb') as f:
                self.logger.log(
                    self.table_name,
                    filename+' loaded. Exiting loadModel method of FileHandler class',
                    'Info')
                return pickle.load(f)

        except Exception as e:
            self.logger.log(
                self.table_name,
                'Error occured in loadModel method of FileHandler class. Message: '+str(e),
                'Error')
            self.logger.database.close_connection()
            raise e
