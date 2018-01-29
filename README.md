# Log-Analysis-Udacity-Project
An internal reporting tool that uses information of large database of a web server and draw business conclusions from that information.
(Project from [Full Stack Web Development Nanodegree](https://in.udacity.com/course/full-stack-web-developer-nanodegree--nd004/))

## Introduction
This is a python module that uses information of large database of a web server and draw business conclusions from that information. The database contains newspaper articles, as well as the web server log for the site. The log has a database row for each time a reader loaded a web page. The database includes three tables:
* The **authors** table includes information about the authors of articles.
* The **articles** table includes the articles themselves.
* The **log** table includes one entry for each time a user has accessed the site.

#### The project drives following conclusions:
* Most popular three articles of all time.
* Most popular article authors of all time.
* Days on which more than 1% of requests lead to errors.

### Functions in log.py:
* **connect():** Connects to the PostgreSQL database and returns a database connection.
* **popular_articles():** Prints most popular three articles of all time.
* **popular_authors():** Prints most popular article authors of all time.
* **http_request_errors():** On which days did more than 1% of requests lead to errors?

## Instructions
* <h4>Install <a href="https://www.vagrantup.com/">Vagrant</a> and <a href="https://www.virtualbox.org/wiki/Downloads">VirtualBox.</a></h4>
* <h4>Clone the repository to your local machine:</h4>
  <pre>git clone https://github.com/visheshbanga/Log-Analysis-Udacity-Project</pre>
* <h4>Start the virtual machine</h4>
  From your terminal, inside the project directory, run the command `vagrant up`. This will cause Vagrant to download the Linux           operating   system and install it.
  When vagrant up is finished running, you will get your shell prompt back. At this point, you can run `vagrant ssh` to log in to your     newly installed Linux VM!
* <h4>Download the <a href="https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip">data</a></h4>
  You will need to unzip this file after downloading it. The file inside is called newsdata.sql. Put this file into the vagrant           directory, which is shared with your virtual machine.
* <h4>Setup Database</h4>
  To load the database use the following command:
  <pre>psql -d news -f newsdata.sql;</pre>
* <h4>Make Views</h4>
  Make views by running respective queries on command line or uncomment code written in python module.
* <h4>Run Module</h4>
  <pre>python log.py</pre>