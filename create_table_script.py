import configparser
import psycopg2


def main():
    """
    Ccnnection string to configure into the warehouse cluster
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print(conn)
    cur = conn.cursor()
    
    cur.execute(open("create_tables.sql", "r").read())
    conn.commit()
    print(conn)
    conn.close()


if __name__ == "__main__":
    main()