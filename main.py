import random
import psycopg2
from configparser import ConfigParser


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def connect():
    """ Connect to the PostgreSQL database server """
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_table(conn, tname):
    commands = [
        f"""
        create table {tname} (
            id integer primary key,
            num_id integer,
            player varchar(100),
            game   varchar(100),
            score  integer
        )
        """
    ]

    try:
        cur = conn.cursor()

        for command in commands:
            cur.execute(command)

        cur.close()

        conn.commit()
        print(f"created table {tname}")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.close()
        conn.rollback()


def insert_data(conn, tname):
    sql_stmt = f"""
        INSERT INTO {tname} (id, num_id, player, game, score) 
        VALUES (%s, %s, %s, %s, %s)
            """

    cursor = conn.cursor()
    id_pk = 1000
    while True:
        try:
            for items in generate_data():
                try:
                    id_pk = id_pk + 1
                    record_item = (id_pk, items[0], items[1], items[2], items[3])
                    cursor.execute(sql_stmt, record_item)
                    conn.commit()
                    print('inserted record')
                except(Exception, psycopg2.Error) as error:
                    print(f"Failed to insert record into {tname} table with {error}")
                    conn.rollback()
        except KeyboardInterrupt:
            print("Terminated the program")
            cursor.close()
            conn.rollback()
            exit(-1)


def generate_data():
    player = random.choice(players)
    game = random.choice(games)
    id_num = random.randint(1, 20)

    segment_1 = player + "_" + game + "_" + str(random.randint(1, 10))
    segment_2 = game + "_" + player + "_" + str(random.randint(1, 10))
    score = random.randint(33, 99)
    yield id_num, segment_1, segment_2, score


if __name__ == "__main__":
    players = ["Player-0"] * 2 + ["Player-1"] * 2 + ["Player-2"] * 2 + ["Player-3"] * 2 + ["Player-4"] * 2
    games = ["Game-1"] * 3 + ["Game-2"] * 3 + ["Game-3"] * 3
    table_name = "games"
    db_connection = connect()
    create_table(db_connection, table_name)
    insert_data(db_connection, table_name)
