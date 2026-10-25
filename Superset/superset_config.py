
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    "sqlite:////app/superset_home/superset.db"
)

WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False

FEATURE_FLAGS = {}
