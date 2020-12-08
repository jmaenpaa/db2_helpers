#!/usr/bin/env python3
"""Export/Import tables to/from CSV file(s)

   Allows for the external editing of the data using spreadsheet
   and other tools. Tables should have previously been unloaded
   using the db_export.py program and the file name must
   match the table name.

   The table(s) must already exist in the database to use this program.
   Existing rows are deleted before importing from the file.
   If referential integrity is in place, the deletion of parent rows
   will likely fail.

   This program does not support large data types and
   should not be used for columns containing JSON or XML
   documents.

   Files to be imported must be stored in <location>/<environment>/<database>/<schema>-<table>.csv

"""
# TODO: Avoid replacing actual data with empty file (add --force) ???
# TODO: Do we need to handle "not null" fields that are zero-length strings?

import sys
import csv
from pathlib import Path
import click
import ibm_db
from db2_helpers import db_connect, db_connected, db_disconnect, db_load_settings, db_error, table_list

# --------------------------------------------------
# Defaults for this program
# --------------------------------------------------
_default_database = "sample"
_default_hostname = "localhost"
_default_environment = "dev"
_default_schema = "db2inst1"
_default_file_location = "./db"
_default_folder_mask = 0o775

# --------------------------------------------------
# Global variables for use across functions
# --------------------------------------------------
_hdbc = None
_sqlerror = None
_sqlcode = None
_sqlstate = None
_headers_expected = True
_write_headers = True
_folder = Path()


# --------------------------------------------------
# db_export command
# --------------------------------------------------

# noinspection PyBroadException
@click.command()
@click.option("--database", "-D", help="Database Name", default=_default_database,
              envvar="DB_DATABASE", show_default=True)
@click.option("--hostname", "--host", "-H", help="Database Host Name", default=_default_hostname,
              envvar="DB_HOSTNAME", show_default=True)
@click.option("--environment", "-E", help="Environment (dev/test/prod)", default=_default_environment,
              envvar="DB_ENVIRONMENT", show_default=True)
@click.option("--schema", "-S", help="Schema", default=_default_schema, envvar="DB_SCHEMA", show_default=True)
@click.option("--location", "-L", help="Directory location of files",
              default=_default_file_location, show_default=True)
@click.option("--headers/--no-headers", help="Write header row to output",
              is_flag=True, default=True, show_default=True)
@click.option("--all-tables", "--all", "-A", help="All tables in schema", is_flag=True, default=False)
@click.option("--table", "-T", help="table name")
@click.option("--password", "--pwd", "-P", help="Pass phrase for secret key", default=None)
def db_export(database, hostname, environment, schema, location, headers, all_tables, table, password):
    """DB Export from Table to CSV file

    Export tables from a Db2 database to CSV file(s).

    Allows for the external editing of the data using spreadsheet
    and other tools. Tables would be loaded back into Db2 using
    the db_import.py program.

    This program does not support large data types and
    should not be used for columns containing JSON or XML
    documents.
    """

    global _hdbc, _sqlerror, _sqlcode, _sqlstate
    global _folder, _write_headers
    global _default_file_location, _default_folder_mask

    # --------------------------------------------------
    # Initialization

    _folder = Path(location, environment, database.lower())
    if not _folder.exists():
        try:
            _folder.mkdir(_default_folder_mask, True, True)
        except OSError:
            print("Unable to create directory", _folder)
            sys.exit(1)

    _write_headers = headers

    db_load_settings(database, hostname, environment, password)
    _hdbc = db_connect()

    if not db_connected():
        print("Database connection failed, quitting.")
        sys.exit(1)

    # --------------------------------------------------
    # Main

    if all_tables and table:
        print("Specify either --all or --table, not both")
        sys.exit(1)

    if all_tables:
        export_list = table_list(schema, False)
        for table_name in export_list:
            export_table(schema, table_name)
    elif table:
        export_list = table_list(schema, True)
        if table.upper() in export_list:
            export_table(schema, table)
        else:
            print("Table", table, "not found in schema", schema)
            sys.exit(1)
    else:
        print("Either --all or --table option required, use --help for usage")

    # Clean up
    db_disconnect()

# --------------------------------------------------
# db_import command
# --------------------------------------------------


# noinspection PyBroadException
@click.command()
@click.option("--database", "-D", help="Database Name", default=_default_database,
              envvar="DB_DATABASE", show_default=True)
@click.option("--hostname", "--host", "-H", help="Database Host Name", default=_default_hostname,
              envvar="DB_HOSTNAME", show_default=True)
@click.option("--environment", "-E", help="Environment (dev/test/prod)", default=_default_environment,
              envvar="DB_ENVIRONMENT", show_default=True)
@click.option("--schema", "-S", help="Schema", default=_default_schema, envvar="DB_SCHEMA", show_default=True)
@click.option("--location", "-L", help="Directory location of files", default=_default_file_location, show_default=True)
@click.option("--headers/--no-headers", help="Files contain header row", is_flag=True, default=True, show_default=True)
@click.option("--all-tables", "--all", "-A", help="All tables in schema", is_flag=True, default=False)
@click.option("--table", "-T", help="table name")
@click.option("--password", "--pwd", "-P", help="Pass phrase for secret key", default=None)
def db_import(database, hostname, environment, schema, location, headers, all_tables, table, password):
    """DB Import into tables from CSV file(s)

       Import tables into a Db2 database from CSV file(s).

       Allows for the external editing of the data using spreadsheet
       and other tools. Tables should have previously been unloaded
       using the db_export.py program.

       The table(s) must already exist in the database to use this program.

        This program does not support large data types and
        should not be used for columns containing JSON or XML
        documents.
    """

    global _hdbc, _sqlerror, _sqlcode, _sqlstate
    global _folder, _headers_expected
    global _default_file_location

    # --------------------------------------------------
    # Initialization

    _folder = Path(location, environment, database.lower())
    if not _folder.exists():
        print("Unable to use directory", _folder)
        sys.exit(1)

    _headers_expected = headers

    db_load_settings(database, hostname, environment, password)
    _hdbc = db_connect()

    if not db_connected():
        print("Database connection failed, quitting.")
        sys.exit(1)

    # --------------------------------------------------
    # Main

    if all_tables and table:
        print("Specify either --all or --table, not both")
        sys.exit(1)

    import_list = table_list(schema)
    if all_tables:
        for table_name in import_list:
            import_table(schema, table_name)
    elif table:
        if table.upper() in import_list:
            import_table(schema, table)
        else:
            print("Table", table, "not found in schema", schema)
            sys.exit(1)
    else:
        print("Either --all or --table option required, use --help for usage")

    # Clean up
    db_disconnect()

# --------------------------------------------------
# Functions
# --------------------------------------------------


# noinspection PyBroadException
def get_index(schema, tbname):
    """Get index for table"""

    global _hdbc

    index_fields = ""

    try:
        ixstmt = ibm_db.primary_keys(_hdbc, None, schema, tbname)

        if ixstmt:
            data_row = ibm_db.fetch_tuple(ixstmt)
            delimiter = ""
            while data_row:
                index_fields = index_fields + delimiter + data_row[3]
                data_row = ibm_db.fetch_assoc(ixstmt)
                delimiter = ","

        ibm_db.free_stmt(ixstmt)

    except Exception as err:
        print(err)
        db_error(False)
        return None

    return index_fields


# noinspection PyBroadException
def export_table(schema, tbname):
    """Export the specified table to a CSV file"""

    global _folder, _write_headers
    global _hdbc

    index_columns = get_index(schema, tbname)

    if index_columns:
        sql_text = "select * from " \
                   + schema.upper() + "." + tbname.upper() \
                   + " order by " \
                   + index_columns + ";"
    else:
        sql_text = "select * from " \
                   + schema.upper() + "." + tbname.upper() \
                   + " order by 1;"

    filecsv = Path(_folder, str(schema.lower() + "-" + tbname.lower() + ".csv"))
    filetmp = Path(_folder, str("tmp_" + schema.lower() + "-" + tbname.lower() + ".csv"))
    count_table_rows = 0
    count_records = 0

    try:
        stmt = ibm_db.prepare(_hdbc, sql_text)
        tbcolumns = get_columns(stmt)

        if ibm_db.execute(stmt):
            data_row = ibm_db.fetch_assoc(stmt)
            # with open(filetmp, "w", newline="\r\n",encoding="utf-8-sig") as fileout:

            with open(filetmp, "w") as fileout:
                writer = csv.DictWriter(fileout, tbcolumns)
                # writer = csv.DictWriter(fileout, tbcolumns,
                # delimiter=",", quotechar=""", quoting=csv.QUOTE_NONNUMERIC)
                if _write_headers:
                    writer.writeheader()
                    count_records += 1
                while data_row:
                    writer.writerow(data_row)
                    count_table_rows += 1
                    count_records += 1
                    data_row = ibm_db.fetch_assoc(stmt)

        ibm_db.free_stmt(stmt)

        print("Table:", tbname, "Rows:", count_table_rows)
        filecsv.unlink(True)
        filetmp.rename(filecsv)

        return True

    except Exception as err:
        print(err)
        db_error(False)
        return False


# noinspection PyBroadException
def get_columns(stmt):
    """Get column information for prepared SELECT statement"""

    columns = []
    colcount = 0
    try:
        colname = ibm_db.field_name(stmt, colcount)
        while colname:
            columns.append(colname)
            colcount += 1
            colname = ibm_db.field_name(stmt, colcount)
        return columns

    except Exception as err:
        print(err)
        db_error(False)
        return None


# noinspection PyBroadException
def import_table(schema, tbname):
    """Import specified table from a CSV file"""

    global _folder, _headers_expected
    global _hdbc

    sqltxt = "select * from " \
             + schema.upper() + "." + tbname.upper() \
             + ";"
    filecsv = Path(_folder, str(schema.lower() + "-" + tbname.lower() + ".csv"))
    count_table_rows = 0
    count_records = 0

    print("Table:", tbname, "File:", filecsv)

    if not filecsv.is_file():
        print("File", filecsv, "does not exist, bypassing table")
        return

    try:
        stmt = ibm_db.prepare(_hdbc, sqltxt)
        tbcolumns = get_columns(stmt)
        ibm_db.free_stmt(stmt)

    except Exception as err:
        print(err)
        db_error(False)
        return False

    try:
        sqltxt = "delete from " + schema + "." + tbname + ";"
        ibm_db.exec_immediate(_hdbc, sqltxt)

    except Exception:
        db_error(False)
        return False

    first = True
    with open(filecsv, "r", newline="\r\n", encoding="utf-8-sig") as filein:
        reader = csv.reader(filein)
        for row in reader:
            count_records += 1
            if first:
                first = False
                # TODO:  Check first row for headers, if present make sure they match the column list
                #        If the list is "short" then adjust our inserts appropriately
                #        If not headers, handle as a data row
                inserttxt = "INSERT INTO " + schema + "." + tbname + " ("
                valuestxt = ") VALUES("
                colcomma = ""
                for col in row:
                    inserttxt = inserttxt + colcomma + col
                    valuestxt = valuestxt + colcomma + "?"
                    colcomma = ","
                sqltxt = inserttxt + valuestxt + ");"
                try:
                    stmt = ibm_db.prepare(_hdbc, sqltxt)
                except Exception as err:
                    # print("Error on Prepare")
                    # print("Message",ibm_db.stmt_errormsg())
                    print(err)
                    db_error(False)
                    return False
            else:
                try:
                    variables = []
                    variable_count = 0
                    for col in row:
                        if col == "":
                            variables.append(None)
                        else:
                            variables.append(col)
                        ibm_db.bind_param(stmt, variable_count + 1, variables[variable_count])
                        variable_count += 1
                except Exception as err:
                    # print("Error on execution of bind_param")
                    # print("Message",ibm_db.stmt_errormsg())
                    print(err)
                    db_error(False)
                    return False

                try:
                    ibm_db.execute(stmt)
                    count_table_rows += 1
                except Exception as err:
                    # print("Error on execution of Insert")
                    # print("Message",ibm_db.stmt_errormsg())
                    print(err)
                    db_error(False)
                    return False

    try:
        ibm_db.free_stmt(stmt)
    except Exception:
        print("error on ibm_db.free_stmt)")

    print("Table:", tbname, "Rows:", count_table_rows, "Columns:", len(tbcolumns))

    return True
