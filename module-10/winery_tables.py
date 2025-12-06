# CSD 310: Database Development and Use
# Modue 10: Milestone #2
# Team Members:
#   Jeffrey Crossdale
#   Isaac Ellingson
#   Patrice Moracchini
#   Jose Velazquez Saenz
# 12/6/2025

import mysql.connector # to connect
from mysql.connector import errorcode

import dotenv # to use .env file
from dotenv import dotenv_values

secrets = dotenv_values(".env")

""" database config object """
db_config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True #not in .env file
}

""" Given fetched cursor output from a query, print the data table it represents. The cursor MUST be in
dictionary mode (dictionary = True) for this function to work!"""
def print_table(table):
    # The hardest thing here is deciding on automatic column display widths.
    if (len(table) < 1): return
    columnWidths = {}
    columnOrder = []
    first_rec = table[0]
    for key in first_rec.keys():
        columnWidths[key] = len(key)
        columnOrder.append(key)

    for rec in table:
        for key in rec.keys():
            valueWidth = len(str(rec[key]))
            columnWidths[key] = max(columnWidths[key], valueWidth)

    # At this point, columnWidths should have meaningful values. Hopefully.
    # So we proceed to print out the table:

    header = ""
    for column in columnOrder:
        header += column.ljust(columnWidths[column])
        header += " "
    print(header)
    separator = ""
    for column in columnOrder:
        separator += "".ljust(columnWidths[column], "-")
        separator += " "
    print(separator)
    for row in table:
        rowStr = ""
        for column in columnOrder:
            rowStr += str(row[column]).ljust(columnWidths[column])
            rowStr += " "
        print(rowStr)




""" Given a cursor and a table name, fetches the data from the table and prints the table. DO NOT ever let user input
anywhere near this function!!! Because we cannot parameterize a table name, we're forced to concatenate.

The cursor MUST be in dictionary mode for this function to work."""
def fetch_and_print(cursor, tableName):
    cursor.execute("SELECT * FROM " + tableName + ";")
    values = cursor.fetchall()
    print(tableName)
    print_table(values)




## Main program ##
try:
    db = mysql.connector.connect(**db_config)

    # This kind of cursor gives us named keys in a dictionary
    # instead of numbered tuples
    cursor = db.cursor( buffered=True, dictionary=True )

    print("Employees and Hours:")
    print("====================")

    print()
    fetch_and_print(cursor, "Employees")

    print()
    fetch_and_print(cursor, "TimecardLines")

    print()
    input("Press enter to continue...")
    print()


    print("Supplies and Receiving:")
    print("=======================")

    print()
    fetch_and_print(cursor, "Supplies")

    print()
    fetch_and_print(cursor, "Suppliers")

    print()
    fetch_and_print(cursor, "SupplyOrders")

    print()
    fetch_and_print(cursor, "SupplyOrderLines")

    print()
    input("Press enter to continue...")
    print()


    print("Orders and Shipping:")
    print("====================")

    print()
    fetch_and_print(cursor, "Wines")

    print()
    fetch_and_print(cursor, "Distributors")

    print()
    fetch_and_print(cursor, "ProductOrders")

    print()
    fetch_and_print(cursor, "ProductOrderLines")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("  The supplied username or password are invalid")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("  The specified database does not exist")

    else:
        print(err)

finally:
    db.close()
