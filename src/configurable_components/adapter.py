"""
This module contains the Session factory class which should be used for all metadata operations.
"""
from datetime import datetime

from flask_wtf import FlaskForm
from sqlalchemy import create_engine, DateTime, String
from sqlalchemy.orm import sessionmaker, mapped_column, Mapped

from src.database.data_classes import ComponentInfo
from src.utils.static import event_config_db_file


class ComponentInterface:
    """
    Interface for the components in the database. It contains the default columns for all components
    as well as the get_component_info method which returns a ComponentInfo object.
    """

    # allow unmapped columns (sqlalchemy ignores fields which have nor Mapped type nor Column type)
    __allow_unmapped__ = True
    # default id and name columns for all components
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100))

    # default type column for all components to distinguish between different object in db
    type: Mapped[str] = mapped_column(String(100))

    # info columns
    error: Mapped[str] = mapped_column(String(100), default=None, nullable=True)
    last_execution: Mapped[datetime] = mapped_column(DateTime, nullable=True)

    FORM: FlaskForm = None

    def get_component_info(self) -> ComponentInfo:
        """
        Get the component info object for the component.
        :return: a ComponentInfo object
        """

        return ComponentInfo(
            id=self.id,
            name=self.name,
            type=self.type,
            last_execution=self.last_execution if self.last_execution is not None
            else "No last execution",
            status="Active" if self.error is None else self.error,
        )

    @classmethod
    def get_form(cls, obj: "ComponentInterface" = None) -> FlaskForm:
        """
        Get the form for the object, if obj is given the form is filled with the object's data.
        (prefill only works on matching field names.)

        Has to be overwritten if the form has select fields with non-static values to be filled on
        runtime.

        :param obj: object to be used to prefill form.
        :return: Flask form for the object
        """
        if obj:
            return cls.FORM(obj=obj)  # pylint: disable=not-callable
        return cls.FORM()  # pylint: disable=not-callable

    @classmethod
    def from_form(cls, form: FlaskForm) -> "ComponentInterface":
        """
        Create a new object from the form data, default implementation to be overwritten if needed:
        In this impl. the form data is keyword matched with the objects, that only works if the
        Object has the exact named fields as the form.

        If this cant be done e.g. if a field has to be created at runtime or from additional data,
        this method has to be overwritten.

        :param form: Filled and validated WTForm
        :return: New object created from the form data
        """

        form_data = {key: value for key, value in form.data.items() if
                     key not in ["csrf_token", "submit", "id"]}
        obj = cls(**form_data)
        return obj


engine = create_engine(f'sqlite:///{event_config_db_file}', echo=False)

Session = sessionmaker(engine, autobegin=False)
