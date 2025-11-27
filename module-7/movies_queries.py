# Jose Velazquez Saenz
# Assignment 7.2 Table Queries



import mysql.connector # to connect
from mysql.connector import errorcode

import dotenv # to use .env file
from dotenv import dotenv_values

#using our .env file
secrets = dotenv_values("code.env")

""" database config object """
config = {
    "user": secrets['USER'],
    "password": secrets['PASSWORD'],
    "host": secrets['HOST'],
    "database": secrets['DATABASE'],
    "raise_on_warnings": True #not in .env file
}

try:
    """ try/catch block for handling potential MySQL database errors """ 

    db = mysql.connector.connect(**config) # connect to the movies database 
    
    # output the connection status 
    print("\n  Database user {} connected to MySQL on host {} with " \
                "database {}".format(config["user"], config["host"], config["database"]))

    
    cursor = db.cursor() # Create a cursor object
    
    # Select all records in the studio table
    query_studio = "SELECT * FROM studio;"
    cursor.execute(query_studio)
    studio_results = cursor.fetchall()
    
    print("\n--- Displaying STUDIO Records ---")
    for row in studio_results:
        print(row)

    # Select all records in the genre table
    query_genre = "SELECT * FROM genre;"
    cursor.execute(query_genre)
    genre_results = cursor.fetchall()
    
    print("\n--- Displaying GENRE Records ---")
    for row in genre_results:
        print(row)

    # Query movies with runtime less than 120 minutes
    query_movies = "SELECT film_name, film_runtime FROM film WHERE film_runtime < 120;"
    cursor.execute(query_movies) 
    movies_results = cursor.fetchall()
    
    print("\n--- Displaying MOVIES with RUNTIME less than 120 minutes ---")
    for row in movies_results:
        print(row)

    # Film names and their directors grouped by director
    query_directors = """SELECT film_director, COUNT(film_name) AS number_of_films 
                        FROM film 
                        GROUP BY film_director;"""
    cursor.execute(query_directors)
    directors_results = cursor.fetchall()
    
    print("\n--- Displaying NUMBER of FILMS by DIRECTOR ---")
    for row in directors_results:
        director, film=row
        print(f"Director: {director} - Number of Films: {film}")
        print(row)
    
    cursor.close() # Close the cursor
    
    input("\n\n  Press any key to continue...")

except mysql.connector.Error as err:
    """ on error code """

    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("  The supplied username or password are invalid")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("  The specified database does not exist")

    else:
        print(err)

finally:
    """ close the connection to MySQL """

    db.close()