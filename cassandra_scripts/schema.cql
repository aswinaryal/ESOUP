CREATE KEYSPACE IF NOT EXISTS stackoverflow WITH replication = {'class': 'SimpleStrategy','replication_factor':'3'};

USE stackoverflow;

CREATE TABLE IF NOT EXISTS trendingtags(
tagtitle TEXT,
total TEXT,
PRIMARY KEY (tagtitle)
);

CREATE TABLE IF NOT EXISTS realtimeposts(
link TEXT,
title TEXT,
PRIMARY KEY (link)
);

CREATE TABLE IF NOT EXISTS toptagsforuser(
UserId int,DisplayName text, Reputation int, Upvotes int, Downvotes int, tagname text, cnt int
PRIMARY KEY (UserId)
);
