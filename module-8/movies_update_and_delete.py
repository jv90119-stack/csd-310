# Jose Velazquez Saenz
# Assignment 8.2 Updates and Deletes


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
    
    cursor = db.cursor() # cursor for MySQL queries

    def show_films(cursor, title):
        
        cursor.execute("SELECT film_name as Name, film_director as Director, genre_name as Genre, " \
        "studio_name as 'Studio Name' from film inner join genre on " \
        "film.genre_id = genre.genre_id inner join studio on film.studio_id = studio.studio_id;") 
        
        films = cursor.fetchall()

        print("\n  -- {} --".format(title))
        
        for film in films:
            print("Film Name: {}\n"
                  "Director: {}\n"
                  "Genre Name: {}\n"
                  "Studio Name: {}\n".format(film[0],film[1], film[2], film[3]))
    
    show_films(cursor, "DISPLAYING FILMS")
    cursor.execute("Insert into film (film_name, film_releaseDate, film_runtime, film_director, " \
    "genre_id, studio_id) values ('The Martian', 2015, 144, 'Ridley Scott', 2, 1);")
    show_films(cursor, "DISPLAYING FILMS AFTER INSERT")
    cursor.execute("UPDATE film SET genre_id = 1 WHERE film_name = 'Alien';")
    show_films(cursor, "DISPLAYING FILMS AFTER UPDATE - Change Alien Genre to Horror")
    cursor.execute("DELETE FROM film WHERE film_name = 'Gladiator';")
    show_films(cursor, "DISPLAYING FILMS AFTER DELETE") 
    


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