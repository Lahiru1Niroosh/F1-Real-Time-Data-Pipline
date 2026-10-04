import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Persist dashboard definitions and Superset settings.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

APP_NAME = "F1 Analytics"
WTF_CSRF_ENABLED = True

from copy import deepcopy
from superset.config import TALISMAN_CONFIG as DEFAULT_TALISMAN_CONFIG

TALISMAN_CONFIG = deepcopy(DEFAULT_TALISMAN_CONFIG)
TALISMAN_CONFIG["content_security_policy"]["img-src"].append(
    "https://upload.wikimedia.org"
)

TALISMAN_CONFIG["content_security_policy"]["img-src"].append(
    "https://media.formula1.com"
)