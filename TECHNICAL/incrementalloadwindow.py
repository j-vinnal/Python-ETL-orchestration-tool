def start_log_incrementalloadwindow(load):
    """
    Create Technical schema and Technical.IncrementalLoadWindow if needed

    If initial load then insert "load start" entry to Technical.IncrementalLoadWindow table
    If incremental load then update entry.

    :param load: Load object to keep specific load variables
    """
    create_technical_schema = 'IF NOT EXISTS(SELECT * FROM sys.schemas WHERE name = \'Technical\') ' \
                              'EXEC(\'CREATE SCHEMA Technical AUTHORIZATION db_owner\');'

    create_incrementalloadwindow = 'IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ' \
                                   'ON t.schema_id = s.schema_id ' \
                                   'WHERE t.name = \'IncrementalLoadWindow\' AND s.name = \'Technical\') ' \
                                   'CREATE TABLE Technical.IncrementalLoadWindow (' \
                                   'TableSchema VARCHAR(4000)' \
                                   ', TableName VARCHAR(4000)' \
                                   ', LastLoadedTS DATETIME ' \
                                   ', LastExecStartTS DATETIME' \
                                   ', LastExecEndTS DATETIME' \
                                   ', CurrentLoadedTS DATETIME' \
                                   ', CurrentExecStartTS DATETIME' \
                                   ', InsertTS DATETIME DEFAULT CURRENT_TIMESTAMP' \
                                   ', UpdateTS DATETIME DEFAULT CURRENT_TIMESTAMP' \
                                   ');'
    with load.database:
        load.database.conn.cursor().execute(create_technical_schema)
        load.database.conn.cursor().execute(create_incrementalloadwindow)
    if load.initial:
        init_log_incrementalloadwindow(load)
    else:
        log_incrementalloadwindow(load)


def end_log_incrementalloadwindow(load):
    """
    Update current entry from Technical.IncrementalLoadWindow at the end of loading
    Set LastLoadedTS and LastExecStartTS to load end timestamp

    :param load: Load object to keep specific load variables
    """
    log_sql = 'UPDATE Technical.IncrementalLoadWindow ' \
              'SET LastLoadedTS = CurrentLoadedTS' \
              ', LastExecStartTS = CurrentExecStartTS' \
              ', LastExecEndTS = CURRENT_TIMESTAMP' \
              ', CurrentLoadedTS = CAST(NULL AS DATETIME)' \
              ', CurrentExecStartTS = CAST(NULL AS DATETIME)' \
              ', UpdateTS = CURRENT_TIMESTAMP ' \
              'WHERE TableSchema = \'{}\' ' \
              'AND TableName = \'{}\' '.format(load.target_schema, load.target_table)
    with load.database:
        load.database.conn.cursor().execute(log_sql)


def init_log_incrementalloadwindow(load):
    """
    Insert "load start" entry to Technical.IncrementalLoadWindow table

    :param load: Load object to keep specific load variables
    """
    init_sql = 'DELETE FROM Technical.IncrementalLoadWindow WHERE 1 = 1 ' \
               'AND TableSchema = \'{}\' AND TableName = \'{}\';'.format(
        load.target_schema, load.target_table)

    init_log_sql = 'INSERT INTO Technical.IncrementalLoadWindow	' \
                   '(    TableSchema    ' \
                   ', TableName    ' \
                   ', LastLoadedTS    ' \
                   ', LastExecStartTS    ' \
                   ', LastExecEndTS    ' \
                   ', CurrentLoadedTS    ' \
                   ', CurrentExecStartTS    ' \
                   ', InsertTS    ' \
                   ', UpdateTS   ' \
                   ')' \
                   'SELECT ' \
                   '  \'{}\' AS TableSchema    ' \
                   ', \'{}\' AS TableName    ' \
                   ', CAST(NULL AS TIMESTAMP) AS LastLoadedTS    ' \
                   ', CAST(NULL AS TIMESTAMP) AS LastExecStartTS    ' \
                   ', CAST(NULL AS TIMESTAMP) AS LastExecEndTS    ' \
                   ', CURRENT_TIMESTAMP AS CurrentLoadedTS    ' \
                   ', CURRENT_TIMESTAMP AS CurrentExecStartTS    ' \
                   ', CURRENT_TIMESTAMP AS InsertTS    ' \
                   ', CURRENT_TIMESTAMP AS UpdateTS'.format(load.target_schema, load.target_table)
    with load.database:
        load.database.conn.cursor().execute(init_sql)
        load.database.conn.cursor().execute(init_log_sql)


def log_incrementalloadwindow(load):
    """
    Update current entry from Technical.IncrementalLoadWindow at the beginning of loading
    Set CurrentLoadedTS and CurrentExecStartTS to load beginning timestamp

    :param load: Load object to keep specific load variables
    """
    log_sql = 'UPDATE Technical.IncrementalLoadWindow ' \
              'SET   CurrentLoadedTS = CURRENT_TIMESTAMP' \
              ', CurrentExecStartTS = CURRENT_TIMESTAMP' \
              ', UpdateTS = CURRENT_TIMESTAMP ' \
              'WHERE TableSchema = \'{}\' ' \
              'AND TableName = \'{}\' '.format(load.target_schema, load.target_table)
    with load.database:
        load.database.conn.cursor().execute(log_sql)
