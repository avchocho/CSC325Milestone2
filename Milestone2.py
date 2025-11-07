#import pymysql,json,os
import pymysql
import json
import os


# Set this to "GCP" or "AWS" (or use an env var: DB_PLATFORM=GCP/AWS)
PLATFORM = os.getenv("DB_PLATFORM", "AWS").upper()

def getconn():
        return pymysql.connect(
            #RDS enpoint = host
            host="csc325termproject-db.cjww46guy0g5.us-east-2.rds.amazonaws.com",
            port=3306,                                       # MySQL default port
            user="admin",
            password="Milestone2",
            #database=None                            # or None if you CREATE first
        )

def setup_db(cur):
  # Set up db
    cur.execute('CREATE DATABASE IF NOT EXISTS disneyplus_db')
    cur.execute('USE disneyplus_db')

    cur.execute('DROP TABLE IF EXISTS ShowDirector;') 
    cur.execute('DROP TABLE IF EXISTS ShowActor;') 
    cur.execute('DROP TABLE IF EXISTS ShowCountry;') 
    cur.execute('DROP TABLE IF EXISTS ShowGenre;') 
    cur.execute('DROP TABLE IF EXISTS Director;')    
    cur.execute('DROP TABLE IF EXISTS Actor;')
    cur.execute('DROP TABLE IF EXISTS Country;') 
    cur.execute('DROP TABLE IF EXISTS Genre;') 
    cur.execute('DROP TABLE IF EXISTS `Show`;')
    
    # Entity tables
    cur.execute('''CREATE TABLE `Show` (
            show_id     VARCHAR(20) NOT NULL PRIMARY KEY,
            title       VARCHAR(300),
            show_type        VARCHAR(20),
            release_year INT(4),
            rating      VARCHAR(20),
            duration    VARCHAR(30),
            date_added  VARCHAR(70),
            description VARCHAR(1000);
        ''');

    cur.execute('''CREATE TABLE Director (
            id          INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            dname       VARCHAR(100);
        ''');

    cur.execute('''CREATE TABLE Actor (
            id          INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            aname       VARCHAR(100);
        ''');

    cur.execute('''CREATE TABLE Country (
            id          INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            cname       VARCHAR(100);
        ''');

    cur.execute('''CREATE TABLE Genre (
            id          INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            gname       VARCHAR(100);
        ''');

    # Joined Tables
    cur.execute('''CREATE TABLE ShowDirector (
            showID      VARCHAR(20) NOT NULL,
            director_id INT NOT NULL,

            FOREIGN KEY(showID) REFERENCES `Show`(show_id),
            FOREIGN KEY(director_id) REFERENCES Director(id),
            PRIMARY KEY (showID, director_id)
        );
        ''')
    
    cur.execute('''CREATE TABLE ShowActor (
            showID      VARCHAR(20) NOT NULL,
            actor_id    INT NOT NULL,
            FOREIGN KEY(showID) REFERENCES `Show`(show_id),
            FOREIGN KEY(actor_id) REFERENCES Actor(id),
            PRIMARY KEY (showID, actor_id)
        );
        ''')

    cur.execute('''CREATE TABLE ShowCountry (
            showID      VARCHAR(20) NOT NULL,
            country_id  INT NOT NULL,
            FOREIGN KEY(showID) REFERENCES `Show`(show_id),
            FOREIGN KEY(country_id) REFERENCES Country(id),
            PRIMARY KEY (showID, country_id)
        );
        ''')
    
    cur.execute('''CREATE TABLE ShowGenre (
            showID     VARCHAR(20) NOT NULL,
            genre_id   INT NOT NULL,
            FOREIGN KEY(showID) REFERENCES `Show`(show_id),
            FOREIGN KEY(genre_id) REFERENCES Genre(id),
            PRIMARY KEY (showID, genre_id)
        );
        ''')

# required function 2: parse the input file
def parse_input_file():
      fname = 'disneyplus_subset.json'
      str_data = open(fname).read()
      json_data = json.loads(str_data)
      return json_data
      
# required function 2: upload data to database
def insert_data(cur):
    cur.execute('USE disneyplus_db')

    json_data = parse_input_file()

    #fname = 'disneyplus_subset.json'
    #Data structure as follows:
    #   [
    #   [ "Charley", "si110", 1 ],
    #   [ "Mea", "si110", 0 ],
    # open the file and read 
    #str_data = open(fname).read()
    # load the data in a json object
    #json_data = json.loads(str_data)


    def split(s):
        return [p.strip() for p in (s or "").split(",") if p and p.strip()]


    #json data is loaded in a pyton list
    #loops thru each record and extracts its attributes
    for entry in json_data:

        show_id = entry[0]
        title = entry[1]
        show_type = entry[2]
        release_year = entry[3]
        rating = entry[4]
        duration = entry[5]
        date_added = entry[6]
        description = entry[7]
        directors = entry[8]
        actors = entry[9]
        countries = entry[10]
        genres = entry[11]
        

        print(show_id)
        print(title)

        # INSERT OR IGNORE satisfies the uniqueness constraint. the inserted data will be ignored if we try to add duplicates.
        # works as both insert and update
        cur.execute('''INSERT IGNORE INTO `Show` (show_id, title, show_type, release_year, rating, duration, date_added, description)  
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s )''', (show_id, title, show_type, release_year, rating, duration, date_added, description) )
        

        # same technique is used to insert the title
        for d in split(directors):
                cur.execute('INSERT IGNORE INTO Director (dname) VALUES ( %s )', ( d,))
                cur.execute('SELECT id FROM Director WHERE dname = %s ', (d,))
                director_id = cur.fetchone()[0]
                cur.execute('INSERT IGNORE INTO ShowDirector (showID, director_id) VALUES (%s,%s)', (show_id, director_id))

        for a in split(actors):
                cur.execute('INSERT IGNORE INTO Actor (aname) VALUES ( %s )', ( a,))
                cur.execute('SELECT id FROM Actor WHERE aname = %s ', (a,))
                actor_id = cur.fetchone()[0]
                cur.execute('INSERT IGNORE INTO ShowActor (showID, actor_id) VALUES (%s,%s)', (show_id, actor_id))

        for c in split(countries):
                cur.execute('INSERT IGNORE INTO Country (cname) VALUES ( %s )', ( c,))
                cur.execute('SELECT id FROM Country WHERE cname = %s ', (c,))
                country_id = cur.fetchone()[0]
                cur.execute('INSERT IGNORE INTO ShowCountry (showID, country_id) VALUES (%s,%s)', (show_id, country_id))

        for g in split(genres):
                cur.execute('INSERT IGNORE INTO Genre (gname) VALUES ( %s )', ( g,))
                cur.execute('SELECT id FROM Genre WHERE gname = %s ', (g,))
                genre_id = cur.fetchone()[0]
                cur.execute('INSERT IGNORE INTO ShowGenre (showID, genre_id) VALUES (%s,%s)', (show_id, genre_id))

cnx = getconn() 
cur = cnx.cursor()
print("Starting Setup...")
setup_db(cur)
print("Finished Setup.")
print("Starting Insert...")
insert_data(cur)
print("Finished Insert.")
cur.close()
cnx.commit()
cnx.close()
print("FINISHED")
