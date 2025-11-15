""" import statements """
import mysql.connector # to connect
from mysql.connector import errorcode
from mysql.connector import MySQLConnection

import dotenv # to use .env file
import os
from dotenv import dotenv_values

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

def RunFirstQuery(conn: MySQLConnection):
    """ Run the first query to fetch all studios from the database
    """
    mycursor = conn.cursor()

    mycursor.execute("SELECT * FROM movies.studio")

    myresult = mycursor.fetchall()

    print("-- DISPLAYING Studio RECORDS --")
    for row in myresult:
        print("Studio ID:", row[0])
        print("Studio Name:", row[1])
        print()

def RunSecondQuery(conn: MySQLConnection):
    """ Run the Second query to fetch all genres from the database
    """
    mycursor = conn.cursor()

    mycursor.execute("SELECT * FROM movies.genre")

    myresult = mycursor.fetchall()

    print("-- DISPLAYING Genre RECORDS --")
    for row in myresult:
        print("Genre ID:", row[0])
        print("Genre Name:", row[1])
        print()

def RunThirdQuery(conn: MySQLConnection):
    """ Run the Third query to fetch all films from the database 
        less than 2 hours in length
    """
    mycursor = conn.cursor()

    mycursor.execute("SELECT film_name, film_runtime FROM movies.film where film_runtime < 120")

    myresult = mycursor.fetchall()

    print("-- DISPLAYING Short Film RECORDS --")
    for row in myresult:
        print("Film Name:", row[0])
        print("Runtime:", row[1])
        print()

def RunFourthQuery(conn: MySQLConnection):
    """ Run the Fourth query to fetch all films names and directors sorted by director
    """
    mycursor = conn.cursor()

    mycursor.execute("Select film_name, film_director from movies.film order by film_director")

    myresult = mycursor.fetchall()

    print("-- DISPLAYING Director RECORDS in Order --")
    for row in myresult:
        print("Film Name:", row[0])
        print("Director:", row[1])
        print()
    

def main():
    conn = GetDatabaseConnection()
    if conn is not None:
        print("Successfully connected to the database.")
    else:
        print("Failed to connect to the database.")
        return

    RunFirstQuery(conn)
    print()
    RunSecondQuery(conn)
    print()
    RunThirdQuery(conn)
    print()
    RunFourthQuery(conn)
if __name__ == "__main__":
    main()