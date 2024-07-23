import os
import time
import unittest

from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.configurable_components import Base
from src.configurable_components.target_loaders.base_target_loader import DummyTargetLoaderForm, \
    TargetLoader, DummyTargetLoader
from src.engine.event_engine import EventEngine
from src.configurable_components import target_loaders

class TestEventEngine(unittest.TestCase):
    DB_PATH = 'test.db'

    def setUp(self):
        target_loaders['dummy_target_loader'] = DummyTargetLoader
        try:
            os.remove(self.DB_PATH)
        except Exception:
            pass

        self.engine = create_engine('sqlite:///' + self.DB_PATH)

        # Create all tables
        Base.metadata.create_all(self.engine)

        # Session factory
        self.session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

        self.app = Flask(__name__)
        self.app.config['TESTING'] = True
        self.app.config['WTF_CSRF_ENABLED'] = False  # Optionally disable CSRF for testing

        self.app_context = self.app.app_context()
        self.app_context.push()

        # Create a new session for each test
        self.session = self.session_factory()
        self.event_engine = EventEngine(session_factory=self.session_factory, interval=10)
        self.event_engine.start()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()
        try:
            os.remove(self.DB_PATH)
        except Exception:
            pass

    def test_add_dummy_target_loader(self):
        form_data = DummyTargetLoaderForm(name="Test Loader", field_name="Testfield123", execution_time=2)
        self.event_engine._create_object('target_loaders', 'dummy_target_loader', form_data)

        added_loader = self.session.query(DummyTargetLoader).filter_by(name="Test Loader").first()
        loader_id = added_loader.id
        self.assertIsNotNone(added_loader)
        self.assertEqual(added_loader.fields[0].influx_field, "Testfield123")
        self.session.query(TargetLoader.last_execution).filter_by(id=loader_id).first()

        for i in range(30):
            time.sleep(1)
            if (self.session.query(TargetLoader.last_execution).filter_by(id=loader_id).first()[0]
                    is not None):
                break

        ts = self.session.query(TargetLoader.last_execution).filter_by(id=loader_id).first()[0]
        self.assertIsNotNone(ts)

    def test_update_dummy_target_loader(self):
        # Assuming there's a mechanism to update loaders via `update_object`
        # First, add a loader to update
        form_data = DummyTargetLoaderForm(name="Updatable Loader", field_name="TestField123",
                                          execution_time=2)
        self.event_engine._create_object('target_loaders', 'dummy_target_loader', form_data)


        loader_id = self.session.query(DummyTargetLoader).filter_by(
            name="Updatable Loader").first().id

        new_form = self.event_engine.get_form_for_component('target_loaders', 'dummy_target_loader',
                                                            loader_id)

        for k, v in form_data.data.items():
            if k != "csrf_token" and k != "field_name":
                print(k)
                self.assertEqual(v, new_form.data[k])

        new_form.field_name.data = "Updated Test String"

        # Update the loader
        self.event_engine._update_object('target_loaders', 'dummy_target_loader', new_form)

        updated_loader = self.session.query(DummyTargetLoader).filter_by(
            name="Updatable Loader").first()
        self.assertEqual(updated_loader.fields[0].influx_field, "Updated Test String")

    def test_delete_dummy_target_loader(self):
        # First, add a loader to delete
        loader_to_delete_form = DummyTargetLoaderForm(name="Deletable Loader", field_name="Test String",
                                                      execution_time=5)
        self.event_engine._create_object('target_loaders', 'dummy_target_loader',
                                              loader_to_delete_form)


        loader_to_delete = self.session.query(DummyTargetLoader).filter_by(
            name="Deletable Loader").first()
        self.event_engine._delete_object('target_loaders', loader_to_delete.id)


        deleted_loader = self.session.query(DummyTargetLoader).filter_by(
            name="Deletable Loader").first()
        self.assertIsNone(deleted_loader)


if __name__ == '__main__':
    unittest.main()
