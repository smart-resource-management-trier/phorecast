"""
This module contains the API endpoints for the flask app.
"""
from flask import Blueprint, request

from src.configurable_components.adapter import Session
from src.configurable_components.target_loaders.base_target_loader import Field
from src.database.influx_interface import influx_interface

api = Blueprint('api', __name__)


@api.route('/apiv0/prediction', methods=['GET'])
def get_latest_predictions():
    """
    Returns the prediction run for specified fields if no run is named the latest run is returned

    :param field: The field to get the prediction for (required)
    :param run_id: The run_id to get the prediction for (optional)
    :return: forecast data see documentation

    """

    field = request.args.get('field')
    run_id = request.args.get('run_id')

    if field is None:
        return "No field specified", 400

    if run_id is not None:
        forecast, run_id = influx_interface.get_forecast(run_id=run_id, target=field)
    else:
        forecast, run_id = influx_interface.get_forecast(target=field)

    forecast_dict = forecast.to_dict(orient="dict")[field]

    response_data = {
        "field": field,
        "run_id": run_id,
        "forecast": {str(key): value for key, value in forecast_dict.items()}
    }

    return response_data, 200


@api.route('/apiv0/fields', methods=['GET'])
def get_fields():
    """
    Returns all fields that are stored in the database
    :return: list of fields
    """

    with Session.begin() as session:
        fields = session.query(Field.influx_field).all()

    fields = [f[0] for f in fields]

    if not fields:
        return "No fields found", 404

    return {"fields": fields}, 200
