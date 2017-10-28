#!/usr/bin/env python3

import datetime
import time
import psycopg2

DBNAME = "news"
TOP_THREE_ARTICLES_QUERY = """SELECT title, COUNT(log.id) AS count
    FROM log JOIN articles ON log.path='/article/' || articles.slug
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
    ON log.path='/article/' || aa.slug
    GROUP BY aa.name
    ORDER BY views DESC;"""
DAYS_WITH_LOTS_OF_ERRORS_QUERY = """SELECT *
    FROM(
        SELECT hdate, (enum::float / enum::float) as err_percent
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
    WHERE err_percent > 0.01;"""


def execute_query(query):
    """Take in a query and a cursor and execute it. Exists to reduce two lines
    of boilerplate to one.
    """
    conn = psycopg2.connect(dbname=DBNAME)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results


def get_top_three_articles():
    """Retrieve the top 3 articles based on views from the database and list
    them in descending order.
    """
    raw_data = execute_query(TOP_THREE_ARTICLES_QUERY)
    data = ['"{}" - {} Views'.format(title, num_views) for title, num_views in
            raw_data]
    return data


def get_most_popular_authors():
    """Retrieve a list of authors, combine their views across articles, and
    rank them from most to least popular.
    """
    raw_data = execute_query(MOST_POPULAR_AUTHORS_QUERY)
    data = ["{} - {} Views".format(author, num_views) for author, num_views in
            raw_data]
    return data


def get_days_with_more_than_one_percent_errors():
    """Retrieve a list of all days where the number of errors logged were greater
    than one percent of the number of times the site was accessed. Output will
    be formatted as January 1, 2017 - 1.5% Errors
    """
    raw_data = execute_query(DAYS_WITH_LOTS_OF_ERRORS_QUERY)
    data = ["{0:%B %d, %Y} - {1:}% errors".format(time, round(percent, 1))
            for time, percent in raw_data]
    return data


def main():
    """Program entry point. Will create connection and cursor to pass to data
    retrieval function and then print the results.
    """

    # Create tuples of headings and data sets to DRY out the output
    headings_and_data = [
        ("Top three articles:", get_top_three_articles()),
        ("Most Popular Authors:", get_most_popular_authors()),
        ("Days where errors exceeded 1%:",
         get_days_with_more_than_one_percent_errors())]

    for heading_and_data in headings_and_data:  # For each tuple
        print(heading_and_data[0])  # Print heading
        for data_point in heading_and_data[1]:  # For each piece of data
            print(data_point)  # Print data
        print("")  # Blank line for seperating data groups


# Prevent execution if imported
if __name__ == '__main__':
    main()
