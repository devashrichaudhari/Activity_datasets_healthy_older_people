# performing important imports
import os
import pandas as pd
from flask_cors import cross_origin
from flask import Flask, request, render_template, redirect, url_for
from prediction_data_validation.prediction_validation import PredictionValidation
from prediction_data_validation.prediction_data_validation import PredictionDataValidation
from model_prediction.prediction import Prediction
from application_logging.logger import AppLogger

app = Flask(__name__)
logger = AppLogger()
table_name = 'api_handler'


@app.route("/", methods=["GET"])
@cross_origin()
def home():
    """
    This function initiates the home page
    :return: html
    """

    try:
        logger.database.connect_db()
        if logger.database.is_connected():
            logger.create_table()
        else:
            raise Exception('Database not connected')

        logger.log(table_name, 'Initiating app', 'Info')
        pred_data_val = PredictionDataValidation()
        # deleting Input_data folder
        if os.path.isdir('Input_data/'):
            pred_data_val.delete_prediction_files()

        # creating Input_data folder
        pred_data_val.create_prediction_files('Input_data')
        column_info = pred_data_val.get_schema_values()
        logger.log(table_name, 'App started. Exiting method...', 'Info')
        return render_template('index.html')
    except Exception as e:
        logger.log(
            table_name,
            f'Exception occured in initating or creation/deletion of Input_data directory. Message: {str(e)}',
            'Error')
        message = 'Error :: ' + str(e)
        return render_template('exception.html', exception=message)


@app.route('/input', methods=['POST'])
@cross_origin()
def manual_input():
    """
    This function helps to get all the manual input provided by the user
    :return: html
    """

    try:
        if logger.database.is_connected():
            logger.create_table()
        else:
            logger.database.connect_db()

        logger.log(table_name, 'Getting input from Form', 'Info')
        # getting data
        if request.method == 'POST':
            input_data = []
            pred_data_val = PredictionDataValidation()
            required_columns = pred_data_val.get_schema_values()
            selected = request.form.to_dict(flat=False)
            for i, v in enumerate(selected.keys()):
                input_data.append(selected[v][0])
            pd.DataFrame([input_data], columns=required_columns).to_csv('Input_data/input.csv', index=False)

        return redirect(url_for('predict'))
    except Exception as e:
        logger.log(table_name, f'Error occured in getting input from Form. Message: {str(e)}', 'Error')
        message = 'Error :: ' + str(e)
        logger.database.close_connection()
        return render_template('exception.html', exception=message)


@app.route('/predict', methods=['GET'])
@cross_origin()
def predict():
    """
    This function is the gateway for data prediction
    :return: html
    """

    try:
        if os.path.exists('Input_data/Prediction.csv'):
            return redirect(url_for('home'))

        logger.log(table_name, 'Prediction Initiated..', 'Info')
        pred_val = PredictionValidation()
        # initiating validstion
        pred_val.validation()
        pred = Prediction()
        # calling perdict to perform prediction
        prediction = pred.predict()
        if prediction == 1:
            output = 'Siting on bed'
        elif prediction == 2:
            output = 'Sitting on chair'
        elif prediction == 3:
            output = 'lying'
        else:
            output = 'ambulating'
        logger.log(table_name, 'Prediction for data complete', 'Info')
        return render_template('result.html', result={"output": output})
    except Exception as e:
        logger.log(table_name, f'Error occured in prediction. Message: {str(e)}', 'Error')
        message = 'Error :: '+str(e)
        logger.database.close_connection()
        return render_template('exception.html', exception=message)


@app.route('/getlogs', methods=['GET'])
@cross_origin()
def view_logs():
    """
    Returns Html page for Logs
    :return: html
    """

    try:
        return render_template('logs.html')
    except Exception as e:
        message = 'Error :: ' + str(e)
        logger.database.close_connection()
        return render_template('exception.html', exception=message)


@app.route('/getlogs/<log>', methods=['GET'])
@cross_origin()
def get_logs(log):
    """
    Returns logs for inspection of the system
    :return: html
    """

    try:
        logger.database.connect_db()
        if logger.database.is_connected():
            logger.create_table()
        else:
            raise Exception('Database not connected')
        data = logger.database.read_data(log)
        return render_template('logs.html', logs=data)
    except Exception as e:
        message = 'Error :: ' + str(e)
        logger.database.close_connection()
        return render_template('exception.html', exception=message)


if __name__ == "__main__":
    app.run(debug=True)
