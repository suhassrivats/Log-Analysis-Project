#!/usr/bin/env python

import psycopg2


def connect():
    """Connect to the PostgreSQL database. Returns a database connection."""
    try:
        db = psycopg2.connect("dbname=news")
        cursor = db.cursor()
        return db, cursor
    except:
        print 'Database Connection Failed'


def popular_articles():
    """Print most popular three articles
        Q: What are the most popular three articles of all time?
        A: Number of times the link for a particular article is used.
            More the number, popular the article.
    """
    db, cursor = connect()
    query = ("select title, count(title) as views from articles,log"
             " where log.path = concat('/article/',articles.slug)"
             " group by title order by views desc limit 3;")
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        db.close()
    except Exception as e:
        raise e

    # Print most popular articles
    print "\nPopular Articles:"
    for i in range(0, len(result), 1):
        print "\"" + result[i][0] + "\" - " + str(result[i][1]) + " views"


def popular_authors():
    """Print most popular authors
    Q: Who are the most popular article authors of all time?
    A: That is, when you sum up all of the articles each author has written,
        which authors get the most page views? Present this as a sorted list
        with the most popular author at the top.
    """
    db, cursor = connect()
    query = ("select au.name, count(ar.author) as views from authors as au, "
             "articles as ar, log where ar.author=au.id and "
             "log.path=concat('/article/',ar.slug) group by au.name order by "
             "views desc;")
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        db.close()
    except Exception as e:
        raise e

    # Print most popular authors
    print "\nPopular Authors:"
    for i in range(0, len(result), 1):
        print "\"" + result[i][0] + "\" - " + str(result[i][1]) + " views"


def http_request_errors():
    """
    Q: On which days did more than 1% of requests lead to errors?
    A:
    """
    db, cursor = connect()
    query = (
        "select day, perc from ("
        "select day, round((sum(requests)/(select count(*) from log where "
        "substring(cast(log.time as text), 0, 11) = day) * 100), 2) as "
        "perc from (select substring(cast(log.time as text), 0, 11) as day, "
        "count(*) as requests from log where status like '%404%' group by day)"
        "as log_percentage group by day order by perc desc) as final_query "
        "where perc >= 1")
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        db.close()
    except Exception as e:
        raise e

    # Print requests that lead to more than 1% error
    print '\nDays with more than 1% of errors:'
    for i in range(0, len(result), 1):
        print str(result[i][0]) + " - "+str(round(result[i][1], 2))+'% errors'


def main():
    popular_articles()
    popular_authors()
    http_request_errors()


if __name__ == '__main__':
    main()
