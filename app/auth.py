"""
auth.py Bluprint, handles user authentication and everything related to it
"""
from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash

from . import db
from .models import User

auth = Blueprint('auth', __name__)


@auth.route('/login')
def login():
    """
    Renders the login page
    """
    return render_template('login.html')


@auth.route('/login', methods=['POST'])
def login_post():
    """
    Handles the login post request
    :return: redirect to profile page
    """

    username = request.form.get('username')
    password = request.form.get('password')
    remember = request.form.get('remember', False)

    user = User.query.filter_by(username=username).first()

    # check if user actually exists
    # take the user supplied password, hash it, and compare it to the hashed password in database
    if not user or not check_password_hash(user.password, password):
        flash('Please check your login details and try again.', 'danger')
        return redirect(url_for('auth.login'))
        # if user doesn't exist or password is wrong, reload the page

    # if the above check passes, then we know the user has the right credentials
    login_user(user, remember=remember)
    return redirect(url_for('auth.profile'))


@auth.route('/signup')
@login_required
def signup():
    """
    Renders the signup page
    """
    return render_template('signup.html')


@auth.route('/signup', methods=['POST'])
@login_required
def signup_post():
    """
    Handles the signup post request
    :return: login page
    """

    username = request.form.get('username')
    password = request.form.get('password')

    # if this returns a user, then the email already exists in database
    user = User.query.filter_by(
        username=username).first()

    if user:  # if a user is found, we want to redirect back to signup page so user can try again
        flash('User name already exists', 'danger')
        return redirect(url_for('auth.signup'))

    # create new user with the form data. Hash the password so plaintext version isn't saved.
    new_user = User(username=username, password=generate_password_hash(password, method='scrypt'))

    # add the new user to the database
    db.session.add(new_user)
    db.session.commit()

    return redirect(url_for('auth.login'))


@auth.route('/logout')
@login_required
def logout():
    """
    Handles the logout request
    :return: login page
    """
    logout_user()
    return redirect(url_for('auth.login'))


@auth.route('/profile')
@login_required
def profile():
    """
    Renders the profile page
    """
    return render_template('profile.html', name=current_user.username)


@auth.route('/change-password', methods=['POST'])
@login_required
def change_password():
    """
    Handles the change password request
    :return: profile page
    """
    password_1 = request.form.get('new_password_1')
    password_2 = request.form.get('new_password_2')
    old_password = request.form.get('old_password')

    # password checks
    form_faulty = False
    if not check_password_hash(current_user.password, old_password):
        flash('Old password is incorrect', 'danger')
        form_faulty = True

    if password_2 != password_1:
        flash('New passwords do not match', 'danger')
        form_faulty = True

    # change password in db when not faulty
    if not form_faulty:
        current_user.password = generate_password_hash(password_1, method='scrypt')
        db.session.commit()

    return redirect(url_for('auth.profile'))


@auth.route('/change-username', methods=['POST'])
@login_required
def change_username():
    """
    Handles the change username request
    :return: profile page
    """

    username = request.form.get('new_username', default=None)

    form_faulty = False

    if username is None or username.strip() == '':
        flash('Username cannot be empty', 'danger')
        form_faulty = True

    user = User.query.filter_by(username=username).first()
    if user:
        flash('Username already exists', 'danger')
        form_faulty = True

    if not form_faulty:
        current_user.username = username
        db.session.commit()

    return redirect(url_for('auth.profile'))
