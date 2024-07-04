"""
models.py Database models
"""
from flask_login import UserMixin

from . import db




class User(db.Model, UserMixin):
    """
    User database model
    """

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True)
    password = db.Column(db.String(100))
