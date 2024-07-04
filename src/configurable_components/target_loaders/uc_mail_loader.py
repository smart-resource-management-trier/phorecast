"""
Module of specific loader class
"""

import email
import imaplib
import socket
from datetime import datetime, timedelta

import pandas as pd
from lxml import etree
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import mapped_column, Mapped, reconstructor
from wtforms import StringField, PasswordField
from wtforms import validators

from src.configurable_components.exceptions import ComponentError
from src.configurable_components.target_loaders.base_target_loader import TargetLoader, Field, \
    field_name_not_existing, TargetLoaderForm
from src.database.influx_interface import influx_interface
from src.utils.logging import get_default_logger

logger = get_default_logger(__name__)


class UCmailLoaderForm(TargetLoaderForm):
    """
    This class represents a form for the UCmailLoader.
    """

    imap_url = StringField('IMAP URL of the EMail server',
                           validators=[validators.DataRequired()])
    username = StringField('Username for the EMail server',
                           validators=[validators.DataRequired()])
    password = PasswordField('Password for the EMail server',
                             validators=[validators.DataRequired()])
    key_from = StringField('KEY to look for in "sender" field',
                           validators=[validators.DataRequired()])

    # Check if field exists in database
    field_name = StringField('Influx Field Name',
                             validators=[validators.DataRequired(),
                                         field_name_not_existing])


# TODO MH: Diesen Loader würde ich ehrlich gesagt aus der Github-Veröffentlichung entfernen.
#  Das ist ja extrem spezifisch. Möglicherweise könnte man einen solchen Loader aber als
#  Beispiel in der Dokumentation aufnehmen, um die Flexibilität des Systems zu zeigen.
# JG: ja, ich denke das wäre ein schönes Beispiel zum Anlegen eines neuen Loaders
# PH: Ja auf jeden fall. Er ist eigentlich nur drin weil er für das deployment das wir haben
# notwendig ist. Ich nehm ihn raus sobald der influx loader fertig ist und wir den nutzen können
# also für das ucb Deployment.
class UCmailLoader(TargetLoader):
    """
    This class represents a loader for the uc mail server. It inherits from the TargetLoader class.
    """

    FORM = UCmailLoaderForm
    __tablename__ = 'uc_mail_target_loader'
    __mapper_args__ = {"polymorphic_identity": "uc_mail_target_loader"}
    id: Mapped[int] = mapped_column(ForeignKey("target_loader.id"), primary_key=True)

    imap_url: Mapped[str] = mapped_column(String(300))
    username: Mapped[str] = mapped_column(String(100))
    password: Mapped[str] = mapped_column(String(100))
    key_from: Mapped[str] = mapped_column(String(100))

    @reconstructor
    def __init__(self, **kwargs):
        """
            TargetLoader for the uc mail server
            :param imap_url: url of the imap server
            :param username: username of the imap server
            :param password: password of the imap server
            :param key_in_sender: filter by this key in the sender email address
        """
        super().__init__(**kwargs)
        self.mail = None

    @classmethod
    def from_form(cls, form: UCmailLoaderForm):
        obj = cls(name=form.name.data, imap_url=form.imap_url.data, username=form.username.data,
                  password=form.password.data, key_from=form.key_from.data,
                  fields=[Field(influx_field=form.field_name.data)])
        return obj

    def _pre_execute(self):
        """
        Logs into the mail server
        """
        try:
            self.mail = imaplib.IMAP4_SSL(self.imap_url)
        except socket.gaierror as e:
            raise ComponentError(f"Could not connect to mail server: {self.imap_url}"
                                 " check if the url is correct", self) from e

        try:
            self.mail.login(self.username, self.password)
        except imaplib.IMAP4.error as e:
            raise ComponentError("Could log into mail server, check your credentials!",
                                 self) from e

        self.mail.select('inbox')
        logger.debug(f"logged in to mail server: {self.imap_url}")

    def _post_execute(self):
        """
        Logs out of the mail server
        """
        if self.mail is not None:
            self.mail.close()
            self.mail.logout()
            self.mail = None
            logger.debug(f"logged out of mail server: {self.imap_url}")

    def _execute(self):
        """
        Loads the emails from the mail server, parses them and writes them to the database
        """
        # Read all emails from the last day
        field_name = self.fields[0].influx_field
        _, last_value_ts = influx_interface.get_last_entry_of_pv_measurement(field_name)
        if last_value_ts is not None:
            time_since = last_value_ts.strftime("%d-%b-%Y")
        else:
            time_since = (datetime.now() - timedelta(days=365)).strftime("%d-%b-%Y")
        _, data = self.mail.search(None, f'(SINCE "{time_since}")')
        email_ids = data[0].split()
        df_list = []
        logger.debug(f"loaded {len(email_ids)} emails from {time_since} from {self.imap_url}")

        # iterate over all emails
        for num in email_ids:
            # fetch the email message by ID and parse it
            _, data = self.mail.fetch(num, '(RFC822)')
            msg = email.message_from_bytes(data[0][1])
            index = (msg['from']).find('<')
            email_from = (msg['from'])[0:index]

            if self.key_from not in email_from:
                continue

            # extract attachment from email
            for part in msg.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                file_name = part.get_filename()
                if bool(file_name):
                    payload = part.get_payload(decode=True)
                    df = parse_uc_xml_to_df(payload)
                    if df is None or df.empty or len(df) == 0:
                        continue
                    df.set_index("timestamp", inplace=True)
                    df[field_name] = df['Target']
                    df.drop(columns=['Target'], inplace=True)
                    df_list.append(df)

        complete_df = pd.concat(df_list)
        complete_df[field_name] = complete_df[field_name].diff()
        complete_df = complete_df.resample('h', label='left', closed='right').sum()
        complete_df.dropna(inplace=True)
        influx_interface.write_pv_data(complete_df, loader_id=self.id)
        logger.info(f"Emails from {self.imap_url} loaded, parsed and written to database")


def parse_uc_xml_to_df(xml_str: str) -> pd.DataFrame:
    """
    Parses a uc xml string to a dataframe
    :param xml_str: xml string to parse
    :return:
    """
    tree = etree.fromstring(xml_str)
    tp_id = '017_EMANGKO__34SAN09BX01'
    tp_elements = tree.find(f'.//TP[@ID="{tp_id}"]')
    values = []
    timestamps = []

    for tp in tp_elements:
        timestamp = None
        value = None
        try:
            timestamp = tp.find('DATETIME').text
        except AttributeError:
            logger.error("Could not find key timestamp in uc xml")
        try:
            value = tp.find('EE').text
        except AttributeError:
            logger.error(f"Could not find key 'EE' uc xml with timestamp {timestamp}")

        if timestamp is None or value is None:
            continue

        timestamps.append(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ"))
        values.append(float(value))

    df = pd.DataFrame({'timestamp': timestamps, 'Target': values})
    return df
