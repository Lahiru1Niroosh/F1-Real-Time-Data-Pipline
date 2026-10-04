import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Persist dashboard definitions and Superset settings.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

APP_NAME = "F1 Analytics"
WTF_CSRF_ENABLED = True