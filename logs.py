import datetime
import time
import psycopg2

DBNAME = "news"
TOP_THREE_ARTICLES_QUERY = """SELECT title, COUNT(log.id) AS count
    FROM log JOIN articles ON log.path LIKE '%' || articles.slug
    WHERE path != '/'
    GROUP BY path, title
    ORDER BY count DESC LIMIT 3;"""
MOST_POPULAR_AUTHORS_QUERY = """SELECT aa.name, sum(log.hits) AS views
    FROM (
        SELECT name, slug
        FROM articles
        INNER JOIN authors
        ON articles.author=authors.id
    ) AS aa
    LEFT JOIN (
        SELECT path, count(id) AS hits
        FROM log
        GROUP BY path
    ) AS log
    ON log.path LIKE '%' || aa.slug
    GROUP BY aa.name
    ORDER BY views DESC;"""
DAYS_WITH_LOTS_OF_ERRORS_QUERY = """SELECT *
    FROM(
        SELECT hdate, enum, hnum
        FROM(
            SELECT date(time) AS edate, count(*) AS enum
            FROM log
            WHERE status='404 NOT FOUND'
            GROUP BY edate
        ) AS errors
        LEFT JOIN(
            SELECT date(time) AS hdate, count(*) AS hnum
            FROM log
            WHERE status='200 OK'
            GROUP BY hdate
        ) AS hits
        ON hdate=edate
        GROUP BY hdate, enum, hnum
    ) as joined
    WHERE enum / hnum > 0.01;"""


def execute_query(cursor, query):
    """Take in a query and a cursor and execute it. Exists to reduce two lines
    of boilerplate to one.
    """
    cursor.execute(query)
    return cursor.fetchall()


def get_top_three_articles(cursor):
    """Retrieve the top 3 articles based on views from the database and list
    them in descending order.
    """
    raw_data = execute_query(cursor, TOP_THREE_ARTICLES_QUERY)
    data = ['"{}" - {} Views'.format(data_point[0], data_point[1])
            for data_point in raw_data]
    return data


def get_most_popular_authors(cursor):
    """Retrieve a list of authors, combine their views across articles, and
    rank them from most to least popular.
    """
    raw_data = execute_query(cursor, MOST_POPULAR_AUTHORS_QUERY)
    data = ["{} - {} Views".format(data_point[0], data_point[1])
            for data_point in raw_data]
    return data


def get_days_with_more_than_one_percent_errors(cursor):
    """Retrieve a list of all days where the number of errors logged were greater
    than one percent of the number of times the site was accessed. Output will
    be formatted as January 1, 2017 - 1% Errors
    """
    raw_data = execute_query(cursor, DAYS_WITH_LOTS_OF_ERRORS_QUERY)
    data = ["{} - {}% errors".format(
        data_point[0].strftime("%B %d, %Y"),
        round(data_point[1] / data_point[2] * 100, 1)) for data_point in
        raw_data]
    return data


def main():
    """Program entry point. Will create connection and cursor to pass to data
    retrieval function and then print the results.
    """
    conn = psycopg2.connect(dbname=DBNAME)
    cursor = conn.cursor()

    # Create tuples of headings and data sets to DRY out the output
    headings_and_data = [
        ("Top three articles:", get_top_three_articles(cursor)),
        ("Most Popular Authors:", get_most_popular_authors(cursor)),
        ("Days where errors exceeded 1%:",
         get_days_with_more_than_one_percent_errors(cursor))]

    conn.close()

    for heading_and_data in headings_and_data:  # For each tuple
        print(heading_and_data[0])  # Print heading
        for data_point in heading_and_data[1]:  # For each piece of data
            print(data_point)  # Print data
        print("")  # Blank line for seperating data groups


main()
