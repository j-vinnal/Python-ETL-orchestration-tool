from DB.database import Database, create_query_string
from ETL.load import Load, prepare_target_table, load_data
from TECHNICAL.incrementalloadwindow import end_log_incrementalloadwindow, start_log_incrementalloadwindow

# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = 'localhost,1433'
db_name = 'AdventureWorks2019'
db = Database(server, db_name)


def load_FactSales(initial=False):
    """
    Data flow to load Fact.Sales table

    :param initial: True if initial full load (first run have to be initial), False if incremental load
    """
    target_table = 'Sales'
    target_schema = 'Fact'

    # Load object to keep specific load variables
    load = Load(db, target_schema, target_table, initial)

    # Incremental window start
    start_log_incrementalloadwindow(load)

    # Create Fact table if needed
    prepare_target_table(load, create_query_string('DDL/Fact.Sales.sql'))

    # Load data to target
    load_data(load, create_query_string('ETL/TRANSFORM/Sales.Fact/transform_Sales.Fact.sql'))

    # Incremental window end
    end_log_incrementalloadwindow(load)


def load_DimCustomer(initial=False):
    """
    Data flow to load Dim.Customer table

    :param initial: True if initial full load (first run have to be initial), False if incremental load
    """
    target_table = 'Customer'
    target_schema = 'Dim'

    load = Load(db, target_schema, target_table, initial)

    # Incremental window start
    start_log_incrementalloadwindow(load)

    # Create Dim table if needed
    prepare_target_table(load, create_query_string('DDL/Dim.Customer.sql'))

    # Load data to target
    load_data(load, create_query_string('ETL/TRANSFORM/Dim.Customer/transform_Dim.Customer.sql'))

    # Incremental window end
    end_log_incrementalloadwindow(load)


if __name__ == "__main__":
    # First run initial = True, after that set parameters to False
    load_FactSales(True)
    load_DimCustomer(True)
