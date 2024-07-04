"""
main.py Bluprint, handles all system related requests and pages except for authentication
"""

import os

from datetime import datetime, timedelta
from io import BytesIO, StringIO

from sqlalchemy import desc
import pandas as pd
from flask import Blueprint, render_template, redirect, url_for, request, flash, send_file
from flask_login import login_required, current_user

from src.configurable_components import WeatherLoader
from src.configurable_components.adapter import Session
from src.configurable_components.exceptions import ExceptionLog
from src.configurable_components.models.base_model import ModelRun, BaseModel
from src.database.data_validation import DataValidationError
from src.database.influx_interface import influx_interface
from src.engine.event_engine import EventEngine

main = Blueprint('main', __name__)

event_engine = EventEngine()
event_engine.start()


@main.route('/')
def index():
    """Renders index page"""
    if current_user.is_authenticated:
        return redirect(url_for('main.components', component_type="target_loaders"))

    return redirect(url_for('auth.login'))


####################################################################################################
################################# Component Configuration ##########################################
####################################################################################################
@main.route('/components/<component_type>')
@login_required
def components(component_type):
    """
    Renders the loaders configuration page
    :param component_type: can be 'target_loaders' or 'weather_loaders'
    """

    active_components = event_engine.get_active_components(component_type)
    creatable_components = event_engine.get_configurable_options(component_type)

    return render_template('components.html', creatable_components=creatable_components,
                           active_components=active_components, component_type=component_type)


@main.route('/create/<component_type>/<table_name>', methods=['GET', 'POST'])
@login_required
def create_object(component_type, table_name):
    """
    Renders the create object page which can be used to create new objects (site is generic for all
    types of objects)
    :param component_type: type of object see configurable_components
    :param table_name: specific table name of the object
    """

    form = event_engine.get_form_for_component(component_type, table_name)
    if form.validate_on_submit():
        event_engine.create_object_async(component_type, table_name, form)
        return redirect(url_for('main.components', component_type=component_type))

    return render_template('create_component.html', form=form, table_name=table_name)


@main.route('/delete/<component_type>/<table_name>/<component_id>', methods=['GET'])
@login_required
def delete_object(component_type, table_name, component_id):  # pylint: disable=unused-argument
    """
    Deletes an object from the database
    :param component_type: type of object. (not used right now.)
    :param table_name: name of table.
    :param component_id: id of the object to be deleted
    """
    event_engine.delete_object_async(component_type, component_id)
    flash('Item deleted successfully!', 'success')
    return redirect(url_for('main.components', component_type=component_type))


@main.route('/update/<component_type>/<table_name>/<component_id>', methods=['GET', 'POST'])
@login_required
def update_object(component_type, table_name, component_id):
    """
    Updates an object in the database
    :param component_type: type of object. (not used right now.)
    :param table_name: name of the table.
    :param component_id: id of the object to be updated
    """
    form = event_engine.get_form_for_component(component_type, table_name, component_id)
    if form.validate_on_submit():
        event_engine.update_object_async(component_type, table_name, form)
        return redirect(url_for('main.components', component_type=component_type))

    return render_template('create_component.html', form=form, table_name=table_name)


@main.route('/model-view/<component_id>', methods=['GET'])
@login_required
def model_view(component_id):
    """
    Renders the model view page
    :param component_id: id of the model
    :return: a page with all runs of the model
    """
    # check for runs of the model
    with Session.begin() as session:
        model_name = session.query(BaseModel.name).filter_by(id=component_id).first()[0]

        runs_raw = session.query(ModelRun.ts_start, ModelRun.loss, ModelRun.id,
                                 ModelRun.path).filter_by(model_id=component_id).all()
    runs = []

    # process the run data
    for r in runs_raw:
        # fetch all image files
        image_files = [f for f in os.listdir(r[3]) if
                       f.endswith('.png') or f.endswith('.jpg') or f.endswith('.jpeg')]

        # create dict for image paths (name,path)
        paths = {(f.split(".")[0].replace("_", " ").capitalize(),
                  url_for("main.load_run_image", run_id=r[2], image=f))
                 for f in image_files}

        runs.append({
            "id": r[2],
            "ts": r[0].strftime("%Y-%m-%d %H:%M:%S"),
            "score": f"{r[1]:.5f}",
            "image_paths": paths
        })
    return render_template('model.html', model_name=model_name, runs=runs,
                           component_id=component_id)


@main.route('/load-run-image/<run_id>/<image>', methods=['GET'])
@login_required
def load_run_image(run_id, image):
    """
    Loads an image from the model run folder
    :param run_id: id of the run (needed for db lookup)
    :param image: image name
    :return: image file
    """
    with Session.begin() as session:
        path = session.query(ModelRun.path).filter_by(id=run_id).first()

    return send_file(os.path.join(path[0], image), as_attachment=False)


@main.route('/train-model/<component_id>', methods=['GET'])
@login_required
def train_model(component_id):
    """
    Trains a model
    :param component_id: id of the model
    :return: model view page
    """
    try:
        event_engine._train_model(component_id)
    except RuntimeError:
        flash("Model is already running", 'warning')
    return redirect(url_for('main.model_view', component_id=component_id))


@main.route('/errors', methods=['GET'])
@login_required
def errors():
    """Renders the error page with the last 50 errors"""

    with Session.begin() as session:
        last_errors = session.query(ExceptionLog).order_by(desc(ExceptionLog.timestamp)).limit(50)
        return render_template('errors.html', errors=last_errors)


####################################################################################################
##################################### Data Management ##############################################
####################################################################################################
@main.route('/restore', methods=['POST'])
@login_required
def restore():
    """
    Restores the database from a backup file
    :param loader_type: type of data to restore, can be 'target_loaders' or 'weather_loaders'
    :param file: file to restore has to be csv
    """
    loader_type = request.form.get('loader_type')
    file = request.files['file']
    string_buffer = StringIO(file.read().decode('utf-8'))
    df = pd.read_csv(string_buffer, index_col=0, parse_dates=True)

    if loader_type == 'target_loaders':
        try:
            influx_interface.write_pv_data(df, -1)
        except DataValidationError:
            flash("Could not restore data, data may be corrupted", 'danger')
            return redirect(url_for('main.backup'))
        flash("Data restored successfully", 'success')

    if loader_type == 'weather_loaders':
        loader_id = request.form.get('weather_loader')
        with Session.begin() as session:
            loader = session.query(WeatherLoader).filter_by(id=loader_id).first()
            if loader.MODEL != df['model'].iloc[0]:
                flash(f"Model did not match: {loader.MODEL} != {df['model'].iloc[0]} tried "
                      f"to restore anyways", 'warning')

            dfs = [group for name, group in df.groupby('cell_id')]
            dfs = sorted(dfs, key=lambda x: x['cell_id'].iloc[0])

            if len(loader.cells) != len(dfs):
                flash(f"Number of cells does not match could not restore: cells in backup: "
                      f"{dfs}, cells in loader: {loader.cells}", 'danger')
                redirect(url_for('main.backup'))
            # This method is not optimal, but it is the easiest way to ensure that the cells are
            # correctly assigned to the loader in the future use the exact position/and member of
            # cell.
            cells = sorted(loader.cells, key=lambda x: x.id)
            faulty = []
            for i, cell in enumerate(cells):
                df_cell = dfs[i].copy()

                for run, run_df in df_cell.groupby('run'):
                    write_df = run_df.drop(columns=['run', 'model', 'cell_id', 'loader_id'],
                                           errors='ignore')
                    try:
                        influx_interface.write_weather_forecast(write_df,
                                                                model=loader.MODEL, run=run,
                                                                loader_id=loader_id,
                                                                cell_id=cell.id)
                    except DataValidationError:
                        faulty.append(run)
            if len(faulty) > 10:
                flash(f"Could not restore {len(faulty)} runs, data may be corrupted ", 'warning')
            elif len(faulty) == 0:
                flash("All runs were restored successfully", 'success')
            else:
                flash(f"Could not restore runs: {faulty}", 'warning')

    return redirect(url_for('main.backup'))


@main.route('/backup', methods=['GET', 'POST'])
@login_required
def backup():
    """
    GET: Renders the backup page
    POST: Creates a backup (csv) of the selected data and returns the file
    """
    if request.method == 'GET':
        with Session.begin() as session:
            loaders = session.query(WeatherLoader.name, WeatherLoader.id).all()

        return render_template('backup.html', weather_loaders=loaders)

    time = datetime.now()

    loader_type = request.form.get('loader_type')
    if loader_type == 'target_loaders':
        data = influx_interface.get_pv_data(time - timedelta(days=365 * 3), time)

    elif loader_type.startswith('weather_loaders'):
        loader_id = loader_type.split(':')[1]
        data = influx_interface.get_weather_forecasts(loader_id=loader_id, keep_metadata=True)
    else:
        return redirect(url_for('main.backup'))

    buffer = BytesIO()
    data.to_csv(buffer, encoding='utf-8')
    buffer.seek(0)

    return send_file(buffer, as_attachment=True,
                     download_name=f'backup_{loader_type}_{time.strftime("%Y-%m-%d")}.csv',
                     mimetype='text/csv')


@main.route('/delete-data', methods=['GET'])
@login_required
def delete_data():
    """
    Deletes all data of a certain type
    :return: backup page
    """

    data_type = request.args.get('data_type')
    influx_interface.delete_measures(data_type)
    return redirect(url_for('main.backup'))
