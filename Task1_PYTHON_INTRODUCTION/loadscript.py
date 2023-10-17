import json
from configuration import ConfigurationManager
import pymysql.cursors


data_path = 'json_data/'
files = {
    'rooms': 'rooms.json',
    'students': 'students.json'
}


def validate_string(val: int | str) -> bytes | int | None:
    """
    Validates and converts a value to the appropriate type.
        Parameters:
            val (int | str): The value to be validated and converted.
        Returns:
            bytes | int | None: The converted value if it is not None. If the value is an integer,
                                it is converted to bytes using UTF-8 encoding and returned.
                                If the value is a string, it is returned as is. If the value is None, None is returned.

    """
    if val is not None:
        if type(val) is int:
            return str(val).encode('utf-8')
        else:
            return val
    else:
        return None


def load_rooms(db_connection: pymysql.Connection):
    """
    Loads room data from a JSON file and inserts it into the database.
        Parameters:
            db_connection (pymysql.Connection): The database connection object.
    """
    rooms_file_path = data_path + files['rooms']
    with open(rooms_file_path, 'r') as rooms_file:
        rooms_json_obj = json.loads(rooms_file.read())

    cursor = db_connection.cursor()
    for i, item in enumerate(rooms_json_obj):
        id = validate_string(item.get("id", None))
        name = validate_string(item.get("name", None))
        cursor.execute("insert into room (id, name) values (%s, %s)", (id, name))
    db_connection.commit()


def load_students(db_connection: pymysql.Connection):
    """
    Loads student data from a JSON file and inserts it into the database.
        Parameters:
            db_connection (pymysql.Connection): The database connection object.
    """
    students_file_path = data_path + files['students']

    with open(students_file_path, 'r') as students_file:
        students_json_obj = json.loads(students_file.read())

    cursor = db_connection.cursor()
    for i, item in enumerate(students_json_obj):
        id = validate_string(item.get("id", None))
        name = validate_string(item.get("name", None))
        sex = validate_string(item.get("sex", None))
        room_id = validate_string(item.get("room", None))
        birthday = validate_string(item.get("birthday", None)).split('T')[0]

        cursor.execute("insert into student (id, name, sex, room_id, birthday) values (%s, %s, %s, %s, %s)",
                       (id, name, sex, room_id, birthday))
    db_connection.commit()


if __name__ == '__main__':
    with pymysql.connect(**ConfigurationManager.get_configuration('db_configs/mysql_config.ini', 'dev'),
                         cursorclass=pymysql.cursors.DictCursor) as connection:
        load_rooms(connection)
        load_students(connection)
