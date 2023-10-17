import configparser

import pymysql
import pymysql.cursors


class DatabaseLayer:
    """ Layer responsible for interaction with database """

    __ROOMS_WITH_OCCUPANCY_QUERY = """select room.*, count(student.id) as students_count
                                            from room left join student
                                            on room.id = student.room_id
                                            group by room.id"""

    _ROOMS_WITH_SMALLEST_AVERAGE_AGE_QUERY = """select room.* from room inner join student
                                                    on room.id  = student.room_id
                                                    group by room.id
                                                    order by avg(datediff(now(), student.birthday))
                                                    limit %s"""

    __ROOMS_WITH_BIGGEST_AGE_DIFFERENCE_QUERY = """select room.* from room inner join student
                                                    on room.id  = student.room_id
                                                    group by room.id
                                                    order by avg(datediff(now(), student.birthday))
                                                    limit %s"""

    __ROOMS_WITH_MIXED_GENDERS_QUERY = """select r.* from room r
                                        where (
                                            select count(*) from
                                            (select distinct sex from student where room_id = r.id) as s
                                        ) > 1"""  # = 2?

    def __init__(self, configuration: configparser.SectionProxy):
        """
        Parameters:
            configuration (configParser.SectionProxy): configuration object for database
        """
        self.connection: pymysql.Connection = pymysql.connect(**configuration, cursorclass=pymysql.cursors.DictCursor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection.open:
            self.connection.close()

    def __del__(self):
        if self.connection.open:
            self.connection.close()

    def __execute_select_query(self, query: str, *args) -> pymysql.cursors.DictCursor:
        """
        Hidden method encapsulating select query
            Parameters:
                query (str): SQL query to execute.
                args: Possible query arguments.
            Returns:
                pymysql.cursors.DictCursor: Cursor for the query.
        """
        cursor = self.connection.cursor()
        cursor.execute(query, args)
        return cursor

    def get_rooms_with_occupancy(self) -> pymysql.cursors.DictCursor:
        """
        Query executor to get rooms and their occupancy.
            Returns:
                pymysql.cursors.DictCursor: The select-cursor for the query.
        """
        return self.__execute_select_query(self.__ROOMS_WITH_OCCUPANCY_QUERY)

    def get_rooms_with_smallest_average_age(self, limit: int = 5) -> pymysql.cursors.DictCursor:
        """
        Query executor to get rooms with the smallest average age
            Parameters:
                limit (int): Limit value for records count.
            Returns:
                pymysql.cursors.DictCursor: The select-cursor for the query.
        """
        return self.__execute_select_query(self._ROOMS_WITH_SMALLEST_AVERAGE_AGE_QUERY, limit)

    def get_rooms_with_biggest_age_difference(self, limit: int = 5) -> pymysql.cursors.DictCursor:
        """
        Query executor to get rooms with the biggest age difference.
            Parameters:
                limit (int): Limit value for records count.
            Returns:
                pymysql.cursors.DictCursor: The select-cursor for the query.
        """
        return self.__execute_select_query(self.__ROOMS_WITH_BIGGEST_AGE_DIFFERENCE_QUERY, limit)

    def get_rooms_with_mixed_genders(self) -> pymysql.cursors.DictCursor:
        """
        Query executor to get rooms with male and female students living together.
            Returns:
                pymysql.cursors.DictCursor: The select-cursor for the query.
        """
        return self.__execute_select_query(self.__ROOMS_WITH_MIXED_GENDERS_QUERY)
