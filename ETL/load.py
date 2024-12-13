class Load:
    """
     Load object to keep specific data load variables
    """

    def __init__(self, database, target_schema, target_table, initial=False):
        self.database = database
        self.target_schema = target_schema
        self.target_table = target_table
        self.initial = initial
        if initial:
            print("Starting initial {}.{} load...".format(target_schema, target_table))
        else:
            print("Starting incremental {}.{} load...".format(target_schema, target_table))


def prepare_target_table(load, ddl):
    """
    Simple function to execute sql queries. In this case DDL, execute only when initial load

    :param load: Load object to keep specific data load variables
    :param ddl: Target table DDL SQL script
    """
    if load.initial:
        with load.database:
            load.database.conn.cursor().execute(ddl)


def load_data(load, sql):
    """
    Simple function to execute sql queries. In this case data transformation.
    Print affected rows count, based on temp sub table

    :param load: Load object to keep specific data load variables
    :param sql: Data transformation query
    """
    with load.database:
        load.database.conn.cursor().execute(sql)
    print("Affected {} rows".format(count_rows(load)))


def count_rows(load):
    """
    Simple function to count affected rows count, based on temp sub table {target_schema}.Temp{target_table}

    :param load: Load object to keep specific data load variables
    """
    with load.database:
        count = load.database.conn.cursor().execute('SELECT COUNT(*) FROM {}.Temp{}'.
                                                    format(load.target_schema, load.target_table))
        count = [row[0] for row in list(count.fetchall())]
        return count[0]
