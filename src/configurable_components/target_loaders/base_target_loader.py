"""
Module for base loader class. Contains the base class for all Target Loaders, the form for the
Loader, and a dummy loader for testing.
"""
import time
from typing import List

import pandas as pd
from flask_wtf import FlaskForm
from sqlalchemy import String, ForeignKey, Integer
from sqlalchemy.orm import mapped_column, Mapped, relationship
from wtforms import ValidationError, StringField, validators, IntegerField

from src.configurable_components.adapter import Session, ComponentInterface
from src.configurable_components.exceptions import ComponentError, log_component_error, \
    log_uncaught_error
from src.utils.general import Base
from src.utils.logging import get_default_logger

logger = get_default_logger(__name__)


def field_name_not_existing(form, field):  # pylint: disable=unused-argument
    """
    Custom validator for checking if field name already exists in database
    :param form: form object
    :param field: field object
    """
    with Session.begin() as session:
        field_names = [f.influx_field for f in session.query(Field).all()]
        if field.data in field_names:
            raise ValidationError(f"Field name cant be the same as an existing field name:"
                                  f"{field_names}")


class Field(Base):
    """
    The field class is a mapping to keep track of target variables. It is assigned by a loader and
    connected to a loader.
    :param external_field: The external field is the name of the field in the external source, could
        be anything to identify the variable consistently.
    :param influx_field: The influx field is the name of the field in the influx database, it is
        stored to, the uniqueness ensures that influx entries are not overwritten.
    """

    # relationships
    __tablename__ = 'field'
    loader_id: Mapped[int] = mapped_column(ForeignKey("target_loader.id"), nullable=True)
    loader: Mapped["TargetLoader"] = relationship(back_populates="fields")

    # columns
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    external_field: Mapped[str] = mapped_column(String(100), nullable=True)
    influx_field: Mapped[str] = mapped_column(String(100), unique=True)


class TargetLoaderForm(FlaskForm):
    """
    Base form for all TargetLoaders
    """
    name = StringField('Name of the Target Loader',
                       validators=[validators.DataRequired()])


class TargetLoader(Base, ComponentInterface):
    """
    Base class for all Target Loaders which retrieve data from an external source and stores it in
    the internal influx database. A target is a variable that is to be predicted by the model.
    The fields list contains information about the fields that the loader stores in the database.
    """

    FORM = None
    __tablename__ = 'target_loader'
    __mapper_args__ = {
        "polymorphic_identity": "target_loader",
        "polymorphic_on": "type",
    }
    fields: Mapped[List["Field"]] = relationship(back_populates="loader",
                                                 cascade="all, delete")

    def run(self):
        """
        Main run method executable as thread, which calls the pre_execute, execute and post_execute
        methods in order, and catches any exceptions that might occur. It also logs the error in the
        database.
        """

        error = None
        try:
            self._pre_execute()
            self._execute()

        except ComponentError as e:
            logger.error(f"ComponentError in Target Loader execution/pre_execution "
                         f"({self.name}): {e}")
            log_component_error(e)
            error = type(e).__name__

        except Exception as e:  # pylint: disable=broad-except
            logger.error(f"Uncaught error in Target Loader execution/pre_execution "
                         f"({self.name}): ({type(e)})")
            logger.exception(e)
            log_uncaught_error(e, self)
            error = type(e).__name__

        finally:
            try:
                self._post_execute()
            except ComponentError as e:
                logger.error(f"ComponentError in Target Loader post_execution ({self.name}): {e}")
                log_component_error(e)
                error = type(e).__name__
            except Exception as e:  # pylint: disable=broad-except
                logger.error(
                    f"Uncaught error in Target Loader post_execution ({self.name}): ({type(e)})")
                logger.exception(e)
                log_uncaught_error(e, self)
                error = type(e).__name__

        self.error = error
        if self.error is None:
            self.last_execution = pd.Timestamp.utcnow()

    def _execute(self):
        """
        Method to be implemented by the subclass, which contains the actual execution logic
        - e.g. fetching data from an API
        - pre-processing the data
        - storing the data in the database
        :return: None
        """

    def _pre_execute(self):
        """
        Method to be implemented by the subclass, which contains the pre-execution logic
        - e.g. checking if the API is reachable
        - checking if the database connection is working
        - logging in
        :return: None
        """

    def _post_execute(self):
        """
        Method to be implemented by the subclass, which contains the post-execution logic
        - e.g. logging out
        - closing the database connection
        :return: None
        """


class DummyTargetLoaderForm(TargetLoaderForm):
    """
    Form for the DummyLoader
    """
    execution_time = IntegerField('Execution Time', default=30,
                                  validators=[validators.NumberRange(min=1)])

    field_name = StringField('Influx Field Name',
                             validators=[validators.DataRequired(),
                                         field_name_not_existing])


class DummyTargetLoader(TargetLoader):
    """
    Dummy loader for testing purposes
    """
    FORM = DummyTargetLoaderForm
    __tablename__ = 'dummy_target_loader'
    __mapper_args__ = {"polymorphic_identity": "dummy_target_loader"}
    id: Mapped[int] = mapped_column(ForeignKey("target_loader.id"), primary_key=True)
    execution_time: Mapped[int] = mapped_column(Integer)

    def _execute(self):
        logger.info("Dummy loader executed")
        time.sleep(self.execution_time * 0.2)

    def _pre_execute(self):
        logger.info("Dummy loader pre executed")
        time.sleep(self.execution_time * 0.6)

    def _post_execute(self):
        logger.info("Dummy loader post executed")
        time.sleep(self.execution_time * 0.2)

    @classmethod
    def from_form(cls, form: DummyTargetLoaderForm):
        obj = cls(name=form.name.data,
                  execution_time=form.execution_time.data,
                  fields=[Field(influx_field=form.field_name.data)])
        return obj
