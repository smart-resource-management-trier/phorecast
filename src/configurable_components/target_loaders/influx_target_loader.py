"""
This module contains the InfluxTargetLoader class, which is used to load data from an external
 InfluxDB
"""
import pandas as pd
from influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.exceptions import InfluxDBError
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column
from wtforms import StringField, validators, PasswordField, TextAreaField

from src.configurable_components.exceptions import ComponentError
from src.configurable_components.target_loaders.base_target_loader import TargetLoaderForm, \
    TargetLoader, field_name_not_existing, Field
from src.database.data_validation import DataValidationError
from src.database.influx_interface import influx_interface
from src.utils.logging import get_default_logger

logger = get_default_logger(__name__)


class InfluxTargetLoaderForm(TargetLoaderForm):
    """
    This class represents a form for the InfluxTargetLoader. It inherits from the
    TargetLoaderForm class. It contains several fields that are used to configure the
    InfluxTargetLoader.
    """

    influx_url = StringField('Influx URL (without http/https)',
                             validators=[validators.DataRequired()])
    token = PasswordField('Token for InfluxDB access (only read access needed)',
                          validators=[validators.DataRequired()])
    org = StringField('Organization to access the InfluxDB',
                      validators=[validators.DataRequired()])
    query = TextAreaField(
        'Influx Query which returns the wanted data.'
        'The time range can be left out since it will be replaced at runtime.\n '
        'An appropriate aggregation function will be set if not defined.\n',
        validators=[validators.DataRequired()])

    field_name = StringField(
        'The field name of the target of the source database, that should be returned by the query',
        validators=[validators.DataRequired()])

    # Check if field exists in database
    field_name_intern = StringField('The field returned in the query should be stored as:',
                                    validators=[validators.DataRequired(),
                                                field_name_not_existing])


class InfluxTargetLoader(TargetLoader):
    """
    This class represents a loader for InfluxDB targets. It inherits from the TargetLoader class.
    It contains several fields and methods that are used to interact with an InfluxDB database.
    """

    FORM = InfluxTargetLoaderForm
    __tablename__ = 'influx_target_loader'
    __mapper_args__ = {"polymorphic_identity": "influx_target_loader"}
    id: Mapped[int] = mapped_column(ForeignKey("target_loader.id"), primary_key=True)

    # login fields
    influx_url: Mapped[str] = mapped_column(String(300))
    token: Mapped[str] = mapped_column(String(100))
    org: Mapped[str] = mapped_column(String(100))

    # query fields
    query: Mapped[str] = mapped_column(String(1000))
    field_name: Mapped[str] = mapped_column(String(100))

    # fields needed between methods
    query_api: QueryApi = None
    influx_client: InfluxDBClient = None

    def _pre_execute(self):
        """
        This method is used to establish a connection to the InfluxDB server. It initializes the
        InfluxDBClient and QueryApi instances and checks the connection by pinging the server.
        If the connection cannot be established, it raises a ComponentError.
        """

        try:
            self.influx_client = InfluxDBClient(url=self.influx_url, token=self.token, org=self.org)
            self.influx_client.ping()
            self.query_api = self.influx_client.query_api()
        except (InfluxDBError, ConnectionRefusedError) as e:
            raise ComponentError("Influx DB client was not able to connect to the "
                                 "database, please check your server config and credentials",
                                 self) from e

    def _execute(self):
        """
        This method is used to execute a query on the InfluxDB server. It first checks if the query
        contains the necessary placeholders for the time range. Then, it executes the query and
        checks the returned data. If the data is not in the correct format, it raises a
        ComponentError. Finally, it writes the data to the database.
        """
        _, ts = influx_interface.get_last_entry_of_pv_measurement(self.fields[0].influx_field)

        if ts is None:
            ts = "-3y"

        query_array = self.query.split("\n")

        if len(query_array) <= 1:
            raise ComponentError("The query should not contain multiple lines", self)

        if "from(bucket:" not in query_array[0].replace(" ", ""):
            raise ComponentError("The query should start with a from statement", self)

        # replace the range statement if it is already in the query
        query_array = [s for s in query_array if "|>range(" not in s.replace(" ", "")]

        query_array.insert(1, f"  |> range(start: {ts})")

        # add aggregation and pivot if not already in the query
        if not any("|>aggregateWindow(" in s.replace(" ", "") for s in query_array):
            query_array.append('  |> aggregateWindow(every: 1h,fn: mean,createEmpty: false, '
                               'timeSrc: "_stop",timeDst: "_time")')

        if not any("|>pivot(" in s.replace(" ", "") for s in query_array):
            query_array.append('  |> pivot(rowKey:["_time"],columnKey: ["_field"],valueColumn: '
                               '"_value")')

        query = "\n".join(query_array)
        # query the data from the external InfluxDB
        try:
            data = self.query_api.query_data_frame(query, data_frame_index=["_time"], org=self.org)
        except InfluxDBError as e:
            raise ComponentError("Error while querying the InfluxDB, please check your query",
                                 self) from e

        # check if the data is in the correct format
        if isinstance(data, list):
            raise ComponentError(
                "Multiple tables found for the given query, should be only one!", self)

        if len(data) == 0:
            raise ComponentError("No data returned from the query", self)

        if self.field_name not in data.columns:
            raise ComponentError(
                "The requested field is not in the data, make sure the table is pivoted"
                " correctly, so that one column is the field name, also make sure the correct"
                "field is retrieved!", self)

        # Only keep the field that is needed
        data[self.fields[0].influx_field] = data[self.field_name]
        data = data[[self.fields[0].influx_field]]
        data.index = pd.to_datetime(data.index)

        try:
            influx_interface.write_pv_data(data, self.id)
        except DataValidationError as e:
            raise ComponentError("Data validation error while writing data to the database",
                                 self) from e
        logger.info(f"Written {len(data)} hours from {data.index.min()} to {data.index.max()} "
                    "to the database")

    def _post_execute(self):
        """
        This method is used to close the connection to the InfluxDB server. It calls the close
        method on the InfluxDBClient instance.
        """

        self.influx_client.close()

    @classmethod
    def from_form(cls, form: InfluxTargetLoaderForm):
        obj = cls(name=form.name.data,
                  influx_url=form.influx_url.data,
                  token=form.token.data,
                  org=form.org.data,
                  query=form.query.data,
                  field_name=form.field_name.data,
                  fields=[Field(influx_field=form.field_name_intern.data)])
        return obj
