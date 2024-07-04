"""
The event engine is responsible for executing the data loaders and the model pipeline.
It also stores their configuration and can update them.
"""

from datetime import datetime
from threading import Thread, Lock
from time import sleep

from flask_wtf import FlaskForm

from src.configurable_components import configurable_components, base_classes
from src.configurable_components.adapter import Session
from src.configurable_components.target_loaders.base_target_loader import TargetLoader
from src.configurable_components.weather_loaders.base_weather_loader import WeatherLoader
from src.database.data_classes import ComponentInfo
from src.utils.logging import get_default_logger

logger = get_default_logger(__name__)


class EventEngine(Thread):
    """
    The event engine is responsible for executing components in set intervals.
    """

    def __init__(self, session_factory: Session = Session, interval: int = 600):
        """
        The event engine is responsible for executing the data loaders and the model pipeline in
        set intervals.
        :param session_factory: session factory for the database
        :param interval: interval in seconds in which the event engine should execute the data
        loaders, models and evals
        """

        super().__init__(daemon=True, name="EventEngine")
        self.interval = interval

        self._write_session = session_factory()
        self._session_lock: Lock = Lock()

        self._model_execution_thread: Thread = None
        self._model_session = session_factory()
        self._model_lock = Lock()

        self._loader_session = session_factory()
        self._loader_lock = Lock()

        self._read_session = session_factory()
        # Database connector

    def _get_session_and_lock(self, type_name: str) -> (Session, Lock):
        """
        Returns the session and lock for the given type, makes code more readable
        :param type_name: type name of the object
        :return: the correct (session,lock) pair
        """
        if type_name == "models":
            return self._model_session, self._model_lock
        if type_name in ["target_loaders", "weather_loaders"]:
            return self._loader_session, self._loader_lock

        raise ValueError(f"Unknown type_name {type_name}")

    def get_configurable_options(self, type_name: str) -> [str]:
        """
        Returns the configurable options for the event engine
        :return: list of stings of Type names
        """
        return list(configurable_components[type_name].keys())

    def get_active_components(self, type_name: str) -> [ComponentInfo]:
        """
        Returns the active components of the given type
        :param type_name: name of type
        :return: list of ComponentInfo objects
        """
        current_components = []
        # no lock since there is no write operation
        with self._read_session.begin():
            object_type = base_classes[type_name]
            for result in self._read_session.query(object_type).all():
                current_components.append(result.get_component_info())
        return current_components

    def get_form_for_component(self, component_type: str, table_name: str,
                               component_id: int = None) -> FlaskForm:
        """
        Returns the form for the object
        :param component_id: if set the form will be pre filled with the object data
        :param table_name: __table_name__  of the object
        :param component_type: type of the object can be: target_loader, weather_loader, models,
        evals
        :return: form for the object
        """
        # pylint: disable=inconsistent-return-statements
        if component_id is None:
            form: FlaskForm = configurable_components[component_type][table_name].get_form()
            return form

        with self._read_session.begin():
            obj = self._read_session.query(base_classes[component_type]).filter_by(
                id=component_id).first()
            form: FlaskForm = \
                configurable_components[component_type][table_name].get_form(obj=obj)
            form.id = obj.id  # add id to form
        return form

    def delete_object_async(self, component_type: str, component_id: int):
        """ See delete_object """
        t = Thread(target=self._delete_object, args=(component_type, component_id))
        t.start()

    def _delete_object(self, component_type: str, component_id: int):
        """
        Deletes a component from the database.
        :param component_type: type of component can be: target_loader, weather_loader, models,
        evals
        :param component_id: id of the component to delete
        :return: None
        """
        session, lock = self._get_session_and_lock(component_type)

        with lock, session.begin():
            obj = session.query(base_classes[component_type]).filter_by(
                id=component_id).first()
            session.delete(obj)
            session.commit()
            logger.info(f"Successfully deleted {component_type} {component_id}")

    def create_object_async(self, component_type, table_name, form_data):
        """ See create_object """
        t = Thread(target=self._create_object, args=(component_type, table_name, form_data))
        t.start()

    def _create_object(self, component_type: str, table_name: str, form_data: FlaskForm):
        """
        Creates a new object and saves it to the database
        :param component_type: type of component, can be: target_loader, weather_loader, models,
        evals
        :param table_name: table name of the object to create
        :param form_data: WTForm to create the object
        :return: None
        """
        session, lock = self._get_session_and_lock(component_type)
        with lock, session.begin():
            obj = configurable_components[component_type][table_name].from_form(form_data)
            session.add(obj)
            session.commit()
            logger.info(f"Successfully created {component_type} {table_name}")

    def update_object_async(self, component_type, table_name, form_data):
        """ See update_object """
        t = Thread(target=self._update_object, args=(component_type, table_name, form_data))
        t.start()

    def _update_object(self, component_type: str, table_name: str, form_data: FlaskForm):
        """
        Updates a loader object and saves the changes to the config
        :param component_type: type of the object can be: target_loader, weather_loader, models,
        evals
        :param table_name: name of the table
        :param form_data: form data to update the object
        :return: None
        """
        session, lock = self._get_session_and_lock(component_type)
        with lock:
            # first delete the old object
            with session.begin():
                obj = session.query(base_classes[component_type]).filter_by(
                    id=form_data.id).first()
                session.delete(obj)

            # create the new object
            with session.begin():
                obj = configurable_components[component_type][table_name].from_form(form_data)
                session.add(obj)
                session.commit()
                logger.info(f"Successfully updated {component_type} {table_name}")

    def _run_loaders(self):
        """
        executes all loaders in parallel
        """

        with self._loader_lock, self._loader_session.begin():
            # get all loaders
            loaders = []
            for loader_type in [TargetLoader, WeatherLoader]:
                loaders.extend(self._loader_session.query(loader_type).all())

            if not loaders:  # exit early if no loaders are present
                logger.info("No loaders to execute!")
                return

            logger.info(
                f"Executing weather and target loaders:\n"
                f"Loaders: {[t.__tablename__ for t in loaders]}")

            # create the Threads objects and give them a name
            loader_threads = \
                [Thread(target=loader.run,
                        daemon=True,
                        name=f"{loader.name} ({loader.__tablename__})") for loader in loaders]

            # If thread should live longer than the timeout, it will throw an exception when
            # accessing its attributes.
            for t in loader_threads:
                t.start()
            for t in loader_threads:
                t.join(self.interval // 2)

                # Check if thread is still alive
                if t.is_alive():
                    logger.warning(f"Loader thread {t.name}) is still alive after timeout, "
                                   f"will crash soon")
            # finish the transaction
            self._loader_session.commit()

    def _run_models(self):
        """
        Executes all models in the database as a separate thread
        :return: None
        """
        with self._model_lock, self._model_session.begin():
            models = self._model_session.query(base_classes["models"]).all()

            # Create Thread objects and give them a name
            model_threads = [Thread(target=model.execute,
                                    daemon=True,
                                    name=f"{model.name} ({model.__tablename__})")
                             for model in models]

            logger.info(f"Executing Models: {[m.name for m in models]}")

            # see _run_loaders for explanation
            for t in model_threads:
                t.start()
            for t in model_threads:
                t.join(3600)
                if t.is_alive():
                    logger.warning(f"Model thread {t.name}) is still alive after timeout, "
                                   f"will crash soon")

            self._model_session.commit()

    def _train_model(self, component_id: int):
        """
        Trains a single model
        :param component_id: id of the component
        :return: None
        """
        if self._model_lock.locked():
            raise RuntimeError("Model is already training")

        with self._model_lock, self._model_session.begin():
            model = self._model_session.query(base_classes["models"]).filter_by(
                id=component_id).first()

            model.train()
            self._model_session.commit()

    def run(self):
        """
        Main Loop, executes the data loaders and the model pipeline in set intervals
        """
        logger.info("Starting Event Engine loop")

        while True:
            start_time = datetime.now()

            # Run loader (blocking)
            self._run_loaders()

            # run models (non blocking)
            if self._model_execution_thread is None or not self._model_execution_thread.is_alive():
                self._model_execution_thread = Thread(target=self._run_models, daemon=True)
                self._model_execution_thread.start()

            duration = datetime.now() - start_time
            sleep_time = self.interval - duration.seconds
            if sleep_time > 0:
                sleep(sleep_time)
