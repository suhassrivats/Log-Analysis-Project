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
    except Exception as e:
        raise e

    result = cursor.fetchall()
    db.close()
    print result


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
    except Exception as e:
        raise e

    result = cursor.fetchall()
    db.close()
    print result


def http_request_errors():
    """
    Q: On which days did more than 1% of requests lead to errors?
    A:
    """
    db, cursor = connect()
    query = ("SELECT au.name, count(log.status) as count FROM authors as au, "
             "articles as ar, log WHERE ar.author = au.id and "
             "log.status = '200 OK' and log.path like '%' || ar.slug || '%' "
             "GROUP BY au.name ORDER BY count desc;")

    try:
        cursor.execute(query)
    except Exception as e:
        raise e

    result = cursor.fetchall()
    db.close()
    print result


def main():
    popular_articles()
    popular_authors()
    http_request_errors()


if __name__ == '__main__':
    main()
