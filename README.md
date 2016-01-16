###Introduction
Sql-for-Elasticsearch (sql4es) is a jdbc 4.1 driver for Elasticsearch implementing the majority of the JDBC interfaces:
 - Connection
 - Statement
 - PreparedStatment (just builds a query using the provided objects)
 - ResultSet
 - DataBase- and ResultSetMetadata
 - Batch
  
Simply said it translates sql statements to their ES counterparts, executes them and returns the proper results. The following sql statements are supported:
- SELECT: fetches documents (with or without scoring) or aggregations from elasticsearch
	- FUNCTIONS (count, min, max, sum, avg)
	- DISTINCT
	- WHERE (=, >, >=, <, <=, LIKE, AND, OR)
	- GROUP BY
	- HAVING
	- ORDER BY 
	- LIMIT
- CREATE TABLE (AS): creates an index/type and optionally indexes the result of a query into it
- CREATE VIEW: creates an alias, optionally with a filter
- INSERT INTO (VALUES | SELECT): inserts documents into an index/type; either provided values or results of a query  
- DELETE FROM: removes documents
- USE: selects an index as its active scope
- EXPLAIN: returns the Elasticsearch query performed for a SELECT statement

###Usage
The sql4es driver can be used by adding the jar file to the tool/application used and load the driver with name 'nl.am.sql4es.jdbc.ESDriver'. The driver expects an URL with the following format: jdbc:sql4es://host:port/index?params. Additional hosts and other properties can be set through the URL's parameters.

###Concepts
Since elasticsearch is a nosql database it does not contain the exact relational objects most people are familiar with (like databases, tables and records). ES does however have a similar hierarchy of objects (index, type and document). The conceptual mapping used by sql4es is the following:
 - Database = Index
 - Table = Type
 - Record = document
 - Column = Field
 - View = Alias (this does not fit from a hierarchical perspective but does match given the purpose of views / aliases)

###QUERIES
This section describes how SQL is interpreted and converted into SE statements. The presto parser is used to parse SQL statements, please see the syntax definition on https://prestodb.io/docs/current/sql.html

####SELECT
Basic syntax: SELECT [field (AS alias)] FROM [types] WHERE [condition] GROUP BY [fields] HAVING [condition] ORDER BY [fields] [ASC|DESC] LIMIT [number]

Fields (also in objects) can be addressed by using their hierarchical names in dotted notation like: nesteddoc.otherdoc.field. It is possible to specify the root of an object in order to fetch all its fields A query like SELECT nesteddoc ... will fetch all fields present in nesteddoc. SELECT \* ... will simply return all fields from documents matching the query. 

By default the query is executed as a filter which means elasticsearch does not scores the results and they are returned in an arbitrary order. Add '\_score' as one of the selected columns in order to change this behavior and request scoring. By default results are returned sorted on \_score DESC (can be changed to ORDER BY _scofe ASC). Ordering on another field will disable scoring. 

Filtering on a textual field will depend on the analysis performed on the specific field. Lets take the snippet='This is a silly example :)' and assume it was not analyzed. Filtering on snippet = 'silly' will not get any results, however snippet LIKE 'This%' will! If the field was analyzed it will work differently, snippet = 'silly' will fetch the row while snippet = ':)' will not (if punctuation was removed during analysis). Hence, filtering on textual fields totally depends on the analysis performed on that field. Under the hood a couple of simple rules are used to determine what type of query should be use:
 - a single word will be put in a TermQuery
 - multiple words will be put in a MatchPhraseQuery
 - presence of wildcards (% and \_) will trigger the use of a WildcardQuery (% is replaced with * and \_ with ?)
This also means that the following queries will be exactly the same: *snippet LIKE '%something'* equals *snippet = '%something'* 

By default results are presented in lateral view. Fields of nested objects are 'exploded' and added to the row. Arrays of objects are exploded as well and will be presented as if they were joined with the parent document. Also see: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView. Note, that although objects are exploded, arrays with primitives are not! They are put in a java.sql.Array implementation supported by JDBC.

Sql4es supports calculated fields like avg(field)/100 and sum(field)/count(1). Valid operations are: *, /, +, - and % (modulo). Note that within the current implementation these calculations are performed within the driver.

Only types part of the active index or alias can be addressed in the FROM clause. An alias must be created if types from different indices must be accessed in a single query (see CREATE VIEW for alias creation). The query cache is the only exception to this rule. When the query cache identifier (default 'query_cache') is used within FROM it indicates the use of the query cache. Whenever present a query is fetched from the cache rather than executed which minimizes query time in case of time consuming queries.
As long as the data does not change the query 'SELECT DISTINCT something FROM type' will be the same as 'SELECT DISTINCT something FROM type, query_cache' (the second will be faster)

Notes:
- limit only works on non aggregating queries. Any 'limits' on an aggregation will be omitted 
- calculations on fields are currently performed within the driver
- having (filtering on aggregated results) is currently performed within the driver
- sorting of aggregated results are currently performed within the driver

####USE
Sql4es works from the scope of a single index/alias. By default this is the index/alias specified within the URL used to get the connection. It is possible to change the index/alias scope by executing:
USE [index / alias] (throws exception if index/alias does not exist)
All subsequent statements will be executed from the specified index/alias. This action only influences the driver and has no effect on Elasticsearch

####CREATE
Sql4es supports creation of indices (create table) and aliases (create view). These statements require knowledge of ES mechanics like mappings, type definitions and aliases.

CREATE TABLE [(index.)type] ([field] "[field definition]" (, [field2])...) WITH ([[property="value"] (, property2=...) )

This creates a mapping for [type] within the currently active index or in the index specified using dot notation. Whenever dotnotation is used it is assumed the part before the first dot refers to the index. If the index specified already exists it just adds the type to this index.

The field definition is the json definition put in the mapping without quoting the json elements! A string type can be defined as follows: *CREATE TABLE mytype (stringField "type:string, index:analyzed, analyzer:dutch")*
Any mapping elements, like templates, can be set using the WITH clause: WITH (dynamic_templates="[{default_mapping: {match: *,match_mapping_type: string, "mapping: {type: string, index: not_analyzed	}}}])
All these json parts will be quoted properly and mashed together into a mapping request

An empty index can be created using CREATE TABLE [index] (_id "type:string"). The _id field is omitted because it is a standard ES field.

CREATE TABLE [(index.)type] AS SELECT ...

Creates a new index/type based on the results of a SELECT statement. The new fieldnames are taken from the SELECT, it is possible to use column-aliases to influence the fieldnames. For example CREATE TABLE myaverage AS SELECT avg(somefield) AS average will result in a new type myaverage within the currently active index with a single Double field called 'average'.
Note that this is a two step process taking place at the driver. First the index/type is created and then the query is executed and results are written (in bulk) to the new type.

CREATE VIEW [alias] AS SELECT * FROM [index1] (, [index2])... (WHERE [condition])
Creates a new ES alias containing the specified indexes or adds the indexes to an existing alias. The optional WHERE clause adds a filter on the index-alias pairs specified. See https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html for information on aliases

####Update
Describes inserting and deleting data through sql

INSERT INTO [(index.)type] ([field1], [field2]...) VALUES ([value1], [value2], ...), ([value1], ...), ... 
Adds one or more documents to the specified type within the index. Fields must be defined and the number of values must match the number of fields defined. It is possible to add multiple documents within a single INSERT statement

INSERT INTO  [(index.)type] SELECT ...
Adds all of the results from the SELECT statement to the specified type within the index. Fieldnames to insert are taken from the result (i.e. column aliases can be used).
Note that, similar to the 'CREATE TABLE .. AS SELECT' the results are pulled into the driver and then indexed (using Bulk).

DELETE FROM [type] (WHERE [condition])
Deletes all documents from the specified type that meet the condition. If no WHERE clause is specified all documents will be removed.
As Elasticsearch can only delete documents based on their \_id this statement is executed in two steps. First collect all _id's from documents that meet the condition, secondly delete those documents using the bulk API 

###CONFIGURATION
It is possible to set parameters through the provided url. All parameters are exposed to elastic search as well which means that is is possible to set Client parameters. The following driver specific parameters can be set:
 - fetch.size (int default 10000): maximum number of results to fetch in a single request (10000 is elasticsearch's maximum)
 - scroll.timeout.sec (int, default 60): the time a scroll id remains valid and 'getMoreResults()' can be called
 - query.timeout.ms (int, default 10000): the timeout set on a query.
 - default.row.length (int, default 250): the initial number of columns created for results. Increase this property only when results do not fit.
 - query.cache.table (string, default 'query_cache'): the fictional table name used to indicate a query & result must be cached
 - result.nested.lateral (boolean, default true): specifies weather nested results must be exploded (the default) or not.