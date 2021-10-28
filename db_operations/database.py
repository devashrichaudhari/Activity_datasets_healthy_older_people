from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from configparser import ConfigParser


class DataBase:
    def __init__(self):
        config = ConfigParser()
        config.read('config/config.ini')
        config.sections()
        self.cloud_config = {
            'secure_connect_bundle': config['database']['path']
        }
        self.auth_provider = PlainTextAuthProvider(config['database']['clientid'], config['database']['clientsecret'])
        self.session = None

    def connect_db(self):
        """
        Connecting to the database
        """
        try:
            cluster = Cluster(cloud=self.cloud_config, auth_provider=self.auth_provider)
            self.session = cluster.connect()
        except Exception as e:
            raise e

    def insert_data(self, table_name, date, time, message, level):
        """
        :param table_name: name of the respective table
        :param date: current date
        :param time: current time
        :param message: log message to insert
        :param level: level of severity
                Info, Error, Warning
        """

        try:
            if self.is_connected():
                query = self.session.prepare(
                    f"INSERT INTO log.{table_name}(id, cur_date, cur_time, level, message) VALUES(uuid(),?,?,?,?)")
                self.session.execute(query, [date, time, level, message])
            else:
                raise Exception('Database not connected')
        except Exception as e:
            raise e

    def read_data(self, table_name):
        """
        Read data from database
        :param table_name: name of table to access
        :return: retrieved data
        """

        try:
            if self.is_connected():
                data = self.session.execute(f"select * from log.{table_name}")
            else:
                raise Exception('Database not connected')
            return data
        except Exception as e:
            raise e

    def create_tables(self):
        """
        Create table if don't exist
        """

        try:
            table = self.session.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'log';")
            if table:
                return
            else:
                self.session.execute(
                    "CREATE TABLE log.api_handler(id uuid , \
                    cur_date date, cur_time time, Level text, message varchar, PRIMARY KEY(id)) ;")
                self.session.execute(
                    "CREATE TABLE log.folder_handler(id uuid PRIMARY KEY, \
                    cur_date date, cur_time time, Level text, message varchar);")
                self.session.execute(
                    "CREATE TABLE log.prediction_log(id uuid PRIMARY KEY, \
                    cur_date date, cur_time time, Level text, message varchar);")
                self.session.execute(
                    "CREATE TABLE log.validation_log(id uuid PRIMARY KEY, \
                    cur_date date, cur_time time, Level text, message varchar);")
                self.session.execute(
                    "CREATE TABLE log.value_from_schema_log(id uuid PRIMARY KEY, \
                    cur_date date, cur_time time, Level text, message varchar);")
        except Exception as e:
            raise e

    def is_connected(self):
        """
        Check is database is connected
        :return: True- Connected
                 False- Not Connected
        """

        try:
            if self.session:
                return True
            else:
                return False
        except Exception as e:
            raise e

    def close_connection(self):
        """
        Close database connection
        """

        try:
            if not self.session.is_shutdown:
                self.session.shutdown()
        except Exception as e:
            raise e
