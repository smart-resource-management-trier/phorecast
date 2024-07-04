"""
This module contains the ExceptionLog class and the ComponentError exception.
The ExceptionLog class is used to log exceptions that occur in the application. The ComponentError
exception is used to re-raise exceptions that occur in the configurable components of the
application.
"""

import traceback

import pandas as pd
from sqlalchemy import Integer, Column, DateTime, String, Text

from src.configurable_components.adapter import Session
from src.utils.general import Base


class ExceptionLog(Base):
    """
    This class represents the exception_logs table in the database.
    """
    __tablename__ = 'exception_logs'
    id = Column(Integer, primary_key=True, autoincrement=True)

    table_name = Column(String(100))
    name = Column(String(100))
    error_message = Column(Text)
    timestamp = Column(DateTime, default=pd.Timestamp.utcnow)
    stack_trace = Column(Text)


class ComponentError(Exception):
    """
    This class represents a custom exception for handling errors related to components.
    It inherits from the built-in Exception class.
    """

    def __init__(self, message: str, component: object):
        """
        @param message: The error message to be passed to the Exception class.
        @type message: str
        @param component: The component that caused the error.
        @type component: object (should be a SQLAlchemy model and have name column)
        """
        super().__init__(message)
        try:
            self.table_name = component.__tablename__
            self.name = component.name
        except AttributeError:
            self.table_name = None
            self.name = None

    @property
    def traceback(self) -> str:
        """
        This property returns the traceback of the error.
        @return: A string message describing the error. If the error has a cause, the message
        includes the type and message of the cause.
        @rtype: str
        """
        if not self.__cause__:
            message = "This error has been raised by itself"
        else:
            cause = self.__cause__
            message = "This error has been caught, it has been caused by:\n"
            message += f"Error type: {type(cause).__name__}\n Message: {str(cause)}"
        return message


def log_uncaught_error(exception: Exception, component: any):
    """
    This function logs an error that has not been caught by the application.
    It formats the error message and stack trace, and then calls the log_error function to log
    the error.

    @param exception: The uncaught exception.
    @param component: The component that caused the error.
    """
    message = "This error has not been caught by the application: \n"
    message += f"Error type: {type(exception).__name__}\n Message: {str(exception)}"
    traceback_formatted = "".join(traceback.format_tb(exception.__traceback__))
    log_error(message, traceback_formatted, component.__tablename__, component.name)


def log_component_error(exception: ComponentError):
    """
        This function logs an error that has been raised by a component.
        It formats the error message and stack trace, and then calls the log_error function to log
        the error.

        @param exception: The ComponentError exception.
    """
    log_error(str(exception), exception.traceback, exception.table_name, exception.name)


def log_error(message: str, stack_trace: str, table_name: str, name: str):
    """
    This function logs an error to the database. It creates a new instance of the ExceptionLog
    class, populates it with the provided error details, and commits it to the database.

    @param message: The error message.
    @param stack_trace: The stack trace of the error.
    @param table_name: The name of the table related to the error.
    @param name: The name of the component related to the error.
    """
    with Session.begin() as session:
        new_log = ExceptionLog(
            error_message=message,
            stack_trace=stack_trace,
            table_name=table_name,
            name=name
        )
        session.add(new_log)
        session.commit()
