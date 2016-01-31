DROP TABLE task21;
DROP TABLE task23;
DROP TABLE task24;
DROP TABLE task31;
DROP TABLE task32;

CREATE TABLE task21 (airport text, airline text, percent double, PRIMARY KEY(airport, airline));
CREATE TABLE task23 (origin text, destination text, airline text, percent double, PRIMARY KEY(origin, destination, airline));
CREATE TABLE task24 (origin text, destination text, percent double, PRIMARY KEY(origin, destination));
CREATE TABLE task31 ( airport text, flights int, PRIMARY KEY(airport));
CREATE TABLE task32 ( origin text, intermediate text, destination text, flightDate text, firstFlight text, secondFlight text,
    PRIMARY KEY(origin, intermediate, destination, flightDate));


CREATE TABLE keytable (key varchar, PRIMARY KEY(key));