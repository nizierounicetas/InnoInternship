from pandas import DataFrame
from pymysql.cursors import DictCursor


class ConversionLayer:
    """ Layer for conversion SQL query results into XML and JSON formats. """

    @classmethod
    def convert_to_xml(cls, cursor: DictCursor, file_path: str | None = None) -> str | None:
        """
        Class method (facade) to convert query result into xml format.
            Parameters:
                cursor (DictCursor): Select-query cursor.
                file_path (str | None): Optional file path to save the XML output.
            Returns:
            str | None: The XML string if no file path is provided, otherwise None.
            """
        return DataFrame(cursor.fetchall()).to_xml(path_or_buffer=file_path, pretty_print=True, index=False,
                                                   row_name='room', root_name='rooms')

    @classmethod
    def convert_to_json(cls, cursor: DictCursor, file_path: str | None = None) -> str | None:
        """"
        Class method (facade) to convert query result into xml format.
            Parameters:
                cursor (DictCursor): Select-query cursor.
                file_path (str | None): Optional file path to save the JSON output.
            Returns:
            str | None: The JSON string if no file path is provided, otherwise None.
            """
        return DataFrame(cursor.fetchall()).to_json(path_or_buf=file_path, indent=4, index=False, orient='records')
