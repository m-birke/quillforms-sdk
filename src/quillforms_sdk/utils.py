from sqlalchemy import Engine, create_engine, text


def create_mysql_engine(host: str, database: str, username: str, password: str, cacert: str = "") -> Engine:
    """Creates a SQLAlchemy engine for MySQL connection using provided credentials

    Enables SSL connection if cacert is provided, otherwise tries to connect with SSL enabled but without certificate verification

    :param host: MySQL host address
    :param database: MySQL database name
    :param username: MySQL username
    :param password: MySQL password
    :param cacert: Path to CA certificate for SSL connection
    """
    connection_string = f"mysql+mysqlconnector://{username}:{password}@{host}/{database}"
    if cacert:
        connection_string += f"?ssl_ca={cacert}&ssl_verify_cert=false"
    else:
        connection_string += "?enable_ssl"
    return create_engine(connection_string)


def test_connection(**db_creds) -> tuple[bool, str]:
    """Creates a test connection and executes a simple query to verify connectivity

    :param db_creds: kv-pairs containing all parameters defined in the function signature of create_mysql_engine
    """
    engine = create_mysql_engine(**db_creds)

    try:
        with engine.connect() as connection:
            _ = connection.execute(text("SELECT 1"))
        return True, "success"

    except Exception as e:
        msg = f"Failed with exception: {e}"
        return False, msg
