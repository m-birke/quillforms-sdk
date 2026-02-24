from sqlalchemy import Engine, create_engine, text

_QUILLFORMS_ENTRIES_TABLE_NAME = "wp_quillforms_entries"  # make use of IDE search and replace feature to rename
_QUILLFORMS_RECORDS_TABLE_NAME = "wp_quillforms_entry_records"  # make use of IDE search and replace feature to rename


def pseudonymize_form_data(form_data: dict) -> dict:
    """Pseudonymizes form data by extracting only form_id and response_id, returns empty values if form_data is empty or keys are missing

    :param form_data: dict containing form data with keys "form_id" and "response_id" as minimum
    """

    pseudonymized_form_data = {"form_id": "", "response_id": ""}

    if not form_data:
        return pseudonymized_form_data

    pseudonymized_form_data["form_id"] = form_data.get("form_id", "")
    pseudonymized_form_data["response_id"] = form_data.get("response_id", "")

    return pseudonymized_form_data


def get_quillforms_response(
    db_creds: dict,
    form_id: int,
    response_id: int,
    add_to_meta: list[tuple[str, int]],
    expected_record_count_range: tuple[int, int] | None = None,
) -> tuple[dict, list]:
    """Queries single form response with metadata and form records per ID

    Extracts additional values from response records and adds it to metadata coming from query_quillform_response_from_id

    :param db_creds: kv-pairs containing all parameters defined in the function signature of create_mysql_engine
    :param form_id: ID of the form to query
    :param response_id: ID of the form response to query
    :param add_to_meta: list of tuples (keyname, index in records) to be added to metadata
    :return: Tuple of metadata dict, records list
    """
    engine = create_mysql_engine(**db_creds)
    meta = query_quillforms_response_from_id(engine=engine, form_id=form_id, response_id=response_id)

    if not meta:
        return {}, []

    records = query_quillforms_response_records(
        engine=engine,
        form_id=form_id,
        entries_id=meta["response_id"],
        expected_record_count_range=expected_record_count_range,
    )
    for keyname, index in add_to_meta:
        if index < len(records):
            meta[keyname] = records[index]
        else:
            msg = f"quillforms response id {response_id}: Cannot add {keyname} to metadata, index {index} is out of range for records with length {len(records)}"
            raise IndexError(msg)

    return meta, records


def get_latest_quillforms_response(
    db_creds: dict,
    form_id: int,
    mail_addr: str,
    add_to_meta: list[tuple[str, int]],
    expected_record_count_range: tuple[int, int] | None = None,
) -> tuple[dict, list]:
    """Queries single form response with given mail address and latest date_created

    Extracts additional values from response records and adds it to metadata coming from query_quillform_latest_response_meta_from_mail_addr

    :return: Tuple of metadata dict, records list
    """
    engine = create_mysql_engine(**db_creds)
    meta = query_quillforms_latest_response_meta_from_mail_addr(engine=engine, form_id=form_id, mail_addr=mail_addr)

    if not meta:
        return {}, []

    records = query_quillforms_response_records(
        engine=engine,
        form_id=form_id,
        entries_id=meta["response_id"],
        expected_record_count_range=expected_record_count_range,
    )

    if not records:
        return meta, []

    for keyname, index in add_to_meta:
        if index < len(records):
            meta[keyname] = records[index]
        else:
            msg = f"quillforms response id {meta['response_id']}: Cannot add {keyname} to metadata, index {index} is out of range for records with length {len(records)}"
            raise IndexError(msg)

    return meta, records


def get_quillforms_record_value(db_creds: dict, form_id: int, response_id: int, record_id: str) -> str:
    """Gets a specific record ID (answer of a single question of a form)

    :param db_creds: kv-pairs containing all parameters defined in the function signature of create_mysql_engine
    :param form_id: ID of the form to query
    :param response_id: ID of the form response to query
    """
    engine = create_mysql_engine(**db_creds)
    return query_quillforms_record_id(engine=engine, form_id=form_id, response_id=response_id, record_id=record_id)


def query_quillforms_response_records(
    engine: Engine, form_id: int, entries_id: int, expected_record_count_range: tuple[int, int] | None = None
) -> list:
    """Queries the actual form response data (records)

    expected_record_count_range can be used to validate the number of records returned, which can be useful to detect changes in the form structure
    (eg. new questions added) that would change the number of records and potentially break the mapping of record index to question

    :param engine: SQLAlchemy engine to use for the query
    :param form_id: ID of the form to query
    :param entries_id: ID of the form response to query
    :param expected_record_count_range: optional tuple of (min, max) expected record count, raises ValueError if actual record count is not in range
    """
    query = text(
        "SELECT record_value FROM wp_quillforms_entry_records WHERE entry_id = :entries_id AND form_id = :form_id"
    )

    with engine.connect() as connection:
        records = [row[0] for row in connection.execute(query, {"entries_id": entries_id, "form_id": form_id})]

    if expected_record_count_range and len(records) not in range(*expected_record_count_range):
        msg = f"quillforms response id {entries_id}: Expected record count in range {expected_record_count_range}, but got {len(records)} records"
        raise ValueError(msg)

    return records


def query_quillforms_response_from_id(engine: Engine, form_id: int, response_id: int) -> dict:
    """Queries single quillforms response metadata, decides for latest date_created

    returns metadata dict with:

    * form_id
    * response_id
    * submitted_at
    * mail_addr
    * system: quillforms
    """
    query = text(
        "SELECT wp_quillforms_entries.ID, wp_quillforms_entries.form_id, date_created, record_value "
        "FROM wp_quillforms_entries INNER JOIN wp_quillforms_entry_records "
        "ON wp_quillforms_entries.ID = wp_quillforms_entry_records.entry_id "
        "WHERE record_value LIKE '%@%.%' "
        "AND wp_quillforms_entries.form_id = :form_id "
        "AND wp_quillforms_entries.ID = :response_id"
    )

    with engine.connect() as connection:
        result_proxy = connection.execute(query, {"form_id": form_id, "response_id": response_id})

    for resp_id, queried_form_id, date_created, queried_mail_addr in result_proxy:
        return {
            "form_id": queried_form_id,
            "response_id": resp_id,
            "submitted_at": date_created.isoformat() + "Z",
            "mail_addr": queried_mail_addr,
            "system": "quillforms",
        }

    return {}


def query_quillforms_latest_response_meta_from_mail_addr(engine: Engine, form_id: int, mail_addr: str) -> dict:
    """Queries single quillforms response metadata based on mail address, decides for latest date_created

    returns dict with:

    * form_id
    * response_id
    * submitted_at
    * mail_addr
    * system: quillforms
    """
    query = text(
        "SELECT wp_quillforms_entries.ID, wp_quillforms_entries.form_id, date_created, record_value "
        "FROM wp_quillforms_entries INNER JOIN wp_quillforms_entry_records "
        "ON wp_quillforms_entries.ID = wp_quillforms_entry_records.entry_id "
        "WHERE record_value = :mail_addr "
        "AND wp_quillforms_entries.form_id = :form_id "
        "ORDER BY date_created DESC "
        "LIMIT 1"
    )

    with engine.connect() as connection:
        result_proxy = connection.execute(query, {"form_id": form_id, "mail_addr": mail_addr})

    for response_id, queried_form_id, date_created, queried_mail_addr in result_proxy:
        return {
            "form_id": queried_form_id,
            "response_id": response_id,
            "submitted_at": date_created.isoformat() + "Z",
            "mail_addr": queried_mail_addr,
            "system": "quillforms",
        }

    return {}


def query_quillforms_record_id(engine: Engine, form_id: int, response_id: int, record_id: str) -> str:
    """Queries a specific record ID (answer of a single question of a form)

    :param engine: SQLAlchemy engine to use for the query
    :param form_id: ID of the form to query
    :param response_id: ID of the form response to query
    :param record_id: ID of the specific record to query (eg. 'ep0bh45p6')
    """

    query = text(
        "SELECT record_value "
        "FROM wp_quillforms_entry_records "
        "WHERE form_id = :form_id "
        "AND entry_id = :response_id "
        "AND record_id = :record_id"
    )

    with engine.connect() as connection:
        result_proxy = connection.execute(
            query, {"form_id": form_id, "response_id": response_id, "record_id": record_id}
        )

    for record_value in result_proxy:
        # result proxy returns a tuple with single entry
        return record_value[0]

    return ""


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
