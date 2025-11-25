""" import statements """
import mysql.connector # to connect
from mysql.connector import errorcode
from mysql.connector import MySQLConnection

import dotenv # to use .env file
import os
from dotenv import dotenv_values
def show_films(cursor, title):
    # Method to exucute an inner join on all tables
    #     iterate over the dataset and output the results to the terminal window

    cursor.execute("select film_name as Name, film_director as Director, genre_name as Genre, studio_name as 'Studio Name' " \
    "from film INNER JOIN genre ON film.genre_id = genre.genre_id " \
    "INNER JOIN studio ON film.studio_id = studio.studio_id")

    films = cursor.fetchall()

    print("\n-- {} --".format(title))

    for film in films:
        print("Film Name: {}\nDirector: {}\nGenre Name ID: {}\nStudio Name: {}\n".format(film[0], film[1], film[2], film[3]))

def GetDatabaseConnection() -> MySQLConnection | None :
    #Was having issues with relative path settings when running locally.
    # Did this to figure out what was wrong
    current_directory = os.getcwd()
    #using our .env file
    secrets = dotenv_values(current_directory + "\\.env")

    """ database config object """
    config = {
        "user": secrets["USER"],
        "password": secrets["PASSWORD"],
        "host": secrets["HOST"],
        "database": secrets["DATABASE"],
        "raise_on_warnings": True #not in .env file
    }

    try:
        """ try/catch block for handling potential MySQL database errors """ 

        #db = mysql.connector.connect(**config) # connect to the movies database 
        db = MySQLConnection(**config)
        if db is not None:
            return db
    except mysql.connector.Error as err:
        """ on error code """

        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("  The supplied username or password are invalid")

        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("  The specified database does not exist")

        else:
            print(err)
    return None


def InsertNewFilm(conn, title, year, length, director, genre_id, studio_id):
    
    """ Insert a new film into the film table
    """
    mycursor = conn.cursor()

    sql = "INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, genre_id, studio_id) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (title, year, length, director, genre_id, studio_id)

    mycursor.execute(sql, val)

    conn.commit()

def UpdateFilmGenre(conn, film_id, new_genre_id):
    """ Update a film's genre in the film table
    """
    mycursor = conn.cursor()

    sql = "UPDATE film SET genre_id = %s WHERE film_id = %s"
    val = (new_genre_id, film_id)

    mycursor.execute(sql, val)

    conn.commit()

def DeleteFilm(conn, film_id):
    """ Delete a film from the film table
    """
    mycursor = conn.cursor()

    sql = "DELETE FROM film WHERE film_id = %s"
    val = (film_id,)

    mycursor.execute(sql, val)

    conn.commit()

def main():
    conn = GetDatabaseConnection()
    if conn is not None:
        print("Successfully connected to the database.")
    else:
        print("Failed to connect to the database.")
        return
    
    show_films(conn.cursor(), "DISPLAYING FILMS")

    InsertNewFilm(conn,"Garfield","2020","125","Peter Rida Michail",3,1)

    show_films(conn.cursor(), "DISPLAYING FILMS AFTER INSERT")

    UpdateFilmGenre(conn, 2, 1)

    show_films(conn.cursor(), "DISPLAYING FILMS AFTER UPDATE - Changed Alien to Horror")

    DeleteFilm(conn, 1)

    show_films(conn.cursor(), "DISPLAYING FILMS AFTER DELETE")
if __name__ == "__main__":
    main()