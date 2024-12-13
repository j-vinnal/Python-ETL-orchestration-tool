import textwrap
import pyodbc


class Database:
    """
    A context manager for handling the database connection using pyodbc.

    This class helps manage a connection to a SQL Server database. It establishes
    the connection upon entering the context and ensures the connection is closed
    upon exiting the context.

    Attributes:
        server (str): The name or IP address of the SQL Server.
        database (str): The name of the database to connect to.
        conn (pyodbc.Connection or None): The pyodbc connection instance.
    """

    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.conn = None

    def __enter__(self):
        self.conn = pyodbc.connect(
            'Driver={SQL Server};SERVER=' + self.server + ';DATABASE=' + self.database + ';Trusted_Connection=yes;')
        self.conn.autocommit = True

    def __exit__(self, *args):
        if self.conn:
            self.conn.close()
            self.conn = None


def create_query_string(sql_full_path):
    """"
    Simple function to read sql queries from .sql files

    :param sql_full_path: sql query path
    :return query string
    """
    with open(sql_full_path, 'r') as f_in:
        lines = f_in.read()

        # remove any common leading whitespace from every line
        query_string = textwrap.dedent("""{}""".format(lines))

    return query_string
