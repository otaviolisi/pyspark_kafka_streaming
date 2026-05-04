import pyodbc
from config import settings


def get_sql_server_connection():
    connection_string = (
        f"DRIVER={{{settings.SQL_SERVER_DRIVER}}};"
        f"SERVER={settings.SQL_SERVER_HOST},{settings.SQL_SERVER_PORT};"
        f"DATABASE={settings.SQL_SERVER_DATABASE};"
        f"UID={settings.SQL_SERVER_USER};"
        f"PWD={settings.SQL_SERVER_PASSWORD};"
        f"TrustServerCertificate={settings.SQL_SERVER_TRUST_CERTIFICATE};"
    )

    return pyodbc.connect(connection_string)