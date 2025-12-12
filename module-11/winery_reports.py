# CSD 310: Database Development and Use
# Modue 11.1: Milestone #3
# Team Members:
#   Jeffrey Crossdale
#   Isaac Ellingson
#   Patrice Moracchini
#   Jose Velazquez Saenz
# 12/12/2025

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


""" Given fetched cursor output from a query, and a series of header columns to compare and redact,
print the data table it represents.
This version will prevent re-preinting a heading in ordered data, helping the user see the natural
categorization of the data.
The cursor MUST be in dictionary mode (dictionary = True) for this function to work!"""
def print_table_ordered(table, headings):
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
    priorHeader = None
    for row in table:
        curHeader = row[headings[0]]
        printHeadings = curHeader != priorHeader
        rowStr = ""
        for column in columnOrder:
            if (not column in headings) or printHeadings:
                rowStr += str(row[column]).ljust(columnWidths[column])
            else:
                rowStr += "".ljust(columnWidths[column])
            rowStr += " "
        print(rowStr)

        priorHeader = curHeader


## Main program ##
try:
    db = mysql.connector.connect(**db_config)

    # This kind of cursor gives us named keys in a dictionary
    # instead of numbered tuples
    cursor = db.cursor( buffered=True, dictionary=True )

    print("Quarterly Hours by Employee")
    print("===========================")

    cursor.execute("SELECT * FROM QuarterlyHoursByEmployee;")
    values = cursor.fetchall()
    print_table(values)

    print()
    print()

    print("Monthly Delivery Timeliness by Vendor")
    print("=====================================")

    cursor.execute("SELECT * FROM DeliveryReport;")
    values = cursor.fetchall()
    print_table(values)

    print()
    print()

    print("Wines by Distributor")
    print("====================")

    cursor.execute("SELECT * FROM WinesByDistributor;")
    values = cursor.fetchall()
    print_table_ordered(values, ("DistributorId", "DistributorName")) # TODO: redact repeated headers with a specialized method

    print()
    print()

    print("Wines Selling < 20 Units")
    print("========================")

    cursor.execute("CALL UnderperformingWines(20);")
    values = cursor.fetchall()
    print_table(values)


except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("  The supplied username or password are invalid")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("  The specified database does not exist")

    else:
        print(err)

finally:
    db.close()
