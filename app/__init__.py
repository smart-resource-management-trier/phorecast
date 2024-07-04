"""
This file contains the Flask application factory. It initializes the Flask application.
"""

import logging
import os
import warnings
import secrets

from flask import Flask
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy
from werkzeug.middleware import proxy_fix
from werkzeug.security import generate_password_hash

from src.utils.static import user_data_db_file
# flake8: noqa
# init SQLAlchemy so we can use it later in our models

app = Flask(__name__)

# get secret key from environment or generate a new one
if 'FLASK_SECRET_KEY' in os.environ:
    app.config['SECRET_KEY'] = os.environ['FLASK_SECRET_KEY']
else:
    app.config['SECRET_KEY'] = secrets.token_hex(16)

app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{user_data_db_file}'
app.logger.disabled = True
log = logging.getLogger('werkzeug')
log.disabled = True

if 'SCRIPT_NAME' in os.environ:
    app.wsgi_app = proxy_fix.ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

if app.debug is True:  # if started with flask in debug mode
    os.environ["DOCKER_INFLUXDB_INIT_HOST"] = "localhost"
else:
    # disable warnings in production mode
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)
db = SQLAlchemy(app)

login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.init_app(app)


@login_manager.user_loader
def load_user(user_id):
    """Returns the requested user object."""
    # since the user_id is just the primary key of our user table, use it in the query for the user
    return User.query.get(int(user_id))


# pylint: disable=wrong-import-position, cyclic-import
# blueprint for auth routes in our app
from .auth import auth as auth_blueprint

app.register_blueprint(auth_blueprint)

# blueprint for non-auth parts of app
from .main import main as main_blueprint

app.register_blueprint(main_blueprint)

# blueprint for api parts of app
from .api import api as api_blueprint

app.register_blueprint(api_blueprint)

# Create all database tables

from .models import User

with app.app_context():
    db.create_all()

    if not db.session.query(User.query.exists()).scalar():
        new_user = User(username='admin', password=generate_password_hash('admin', method='scrypt'))

        # add the new user to the database
        db.session.add(new_user)
        db.session.commit()
