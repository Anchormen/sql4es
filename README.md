### Introduction sql4es

Sql-for-Elasticsearch (sql4es) is a jdbc 4.1 driver for Elasticsearch 2.X implementing the majority of the JDBC interfaces: Connection, Statement, PreparedStatment, ResultSet, Batch and DataBase- /  ResultSetMetadata.

Simply said it translates SQL statements to their Elasticsearch counterparts, executes them and returns the result in a ResultSet implementation. The following sql statements are supported:

- SELECT: fetches documents (with or without scoring) or aggregations from elasticsearch
  * FUNCTIONS (count, min, max, sum, avg)
  * DISTINCT
  * WHERE (=, >, >=, <, <=, <>, IN, LIKE, AND, OR)
  * GROUP BY
  * HAVING
  * ORDER BY
  * LIMIT (without offset, offsets are not supported by sql4es)
- CREATE TABLE (AS) creates an index/type and optionally indexes the result of a query into it
- CREATE VIEW (AS): creates an alias, optionally with a filter
- DROP TABLE/VIEW removes an index or alias
- INSERT INTO (VALUES | SELECT): inserts documents into an index/type; either provided values or results of a query 
- DELETE FROM (WHERE): removes documents
- USE: selects an index as its active 
- EXPLAIN SELECT: returns the Elasticsearch query performed

The driver can be used from code or applications able to load the jdbc driver. It has been used succesfully with [sqlWorkbench/J](http://www.sql-workbench.net/) and [Squirrel](http://squirrel-sql.sourceforge.net/) on an Elasticsearch 2.1 cluster. 

**Not supported (yet)**

* Offsets are not supported (OFFSET or LIMIT offset, number)
* Fields with type 'nested' are not supported because this type requires different methods to query and retrieve data.
* Parent child relationships are not supported. It is currently not possible to index or retrieve fields of this type.

### Usage

The sql4es driver can be used by adding the jar file to the tool/application used and load the driver with name '***nl.anchormen.sql4es.jdbc.ESDriver***'. The driver expects an URL with the following format: ***jdbc:sql4es://host:port/index?params***. Additional hosts and other properties can be set through the URL's parameters, also see the Configuration section.

``` java
// register the driver and get a connection for index 'myidx'
Class.forName("nl.anchormen.sql4es.jdbc.ESDriver");
Connection con = DriverManager.getConnection("jdbc:sql4es://localhost:9300/myidx?");
Statement st = con.createStatement();
// execute a query on mytype within myidx
ResultSet rs = st.executeQuery("SELECT * FROM mytype WHERE somting >= 42");
ResultSetMetaData rsmd = rs.getrs.getMetaData();
int nrCols = rsmd.getColumnCount();
// get other column information like type
while(rs.next){
	for(int i=1; i<=nrCols; i++){
  		System.out.println(rs.getObject(i));
	}
}
rs.close();
con.close();
```

### Concepts

Since elasticsearch is a nosql database it does not contain the exact relational objects most people are familiar with (like databases, tables and records). Elasticsearch does however have a similar hierarchy of objects (index, type and document). The conceptual mapping used by sql4es is the following:

- Database = Index
- Table = Type
- Record = document
- Column = Field
- View = Alias (this does not fit from a hierarchical perspective but does match given the purpose of views / aliases)

Elasticsearch responses, both search results and aggregations, are put into a ResultSet implementation. Any nested objects are 'exploded' into a lateral view by default; this means that nested objects are treated as joined tables which are put inside the he same row (see [this page](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView) for explanation). It is possible to represent nested objects as a nested ResultSet, see the Configuration section. Note, that although objects are exploded, arrays with primitives are not! They are put in a java.sql.Array implementation supported by JDBC.

Sql4es works from an active index wich means that it resolves references to types from this index. If for example *myIndex* is currently active the query *SELECT * FROM sometype* will only return any results if sometype is part of myindex. Executing a SELECT on a type that does not exist within an index will return an empty result. It is possible to change the active index by executing *USE [otherIndex]* as described below. 

### QUERIES

This section describes how SQL is interpreted and converted into SE statements. The presto parser is used to parse SQL statements, please see the syntax definition on the [presto website](https://prestodb.io/docs/current/sql.html).

#### SELECT

``` sql
/* basic syntax */
SELECT [field (AS alias)] FROM [types] WHERE [condition] GROUP BY [fields] HAVING [condition] ORDER BY [field (ASC|DESC)] LIMIT [number]
```

- fields (AS alias): defines the fields to be retrieved from elasticsearch and put in the ResultSet. It is possible to use * to indicate all fields should be retrieved (including _id and _type). Fields can be addressed by their name, nested fields can be addressed  using their hierarchical names in dotted notation like: *nesteddoc.otherdoc.field*. Using a star will simply fetch all fields, also nested ones, present in a document. It is possible to specify the root of an object in order to fetch all its fields. A query like *SELECT nesteddoc FROM type* will fetch all fields present in nesteddoc. As a result it might return hundreds of columns if nesteddoc has hundreds of fields.
- types: the types to execute the query against. This can only be types present in the index that is currently active (also see 'use' statement).
- condition: standard SQL condition using =, >, >=, <, <=, <>, IN and LIKE operators. Sql4es does not support the NOT operator but '<>' can be used instead. Use AND and OR to combine conditions. 
- limit: only works on non aggregating queries. The use of offset is not supported!

``` sql
/* the following wil explode any nested objects into a lateral view */
SELECT * from mytype

SELECT _id as id, myInt, myString FROM mytype WHERE myInt >= 3 OR (myString IN ('hello','hi','bye') AND myInt <= 3)

/* If nestedDoc contains 2 fields the result will be exploded to [myInt,nestedDoc.field1, nestedDoc.field2] */
SELECT myInt, nestedDoc FROM mytype WHERE myInt > 3 AND myString <> 'bye'

/* If the array contains 3 objects the resultset will contain 3 rows, despite the LIMIT used! */
SELECT array_of_nested_objects FROM mytype LIMIT 1
```

**Tables/Types**

Only types part of the active index or alias can be addressed in the FROM clause. An alias must be created if types from different indices must be accessed in a single query (see CREATE VIEW for alias creation). The *query cache* is the only exception to this rule. When the query cache identifier (default 'query_cache') is used within FROM it indicates the use of the query cache. Whenever present a query is fetched from the cache rather than executed which minimizes query time in case of time consuming queries.

``` sql
SELECT DISTINCT field, count(1) FROM type, query_cache
/* exactly the same as but without using the query cache */
SELECT DISTINCT field, count(1) FROM type
```

**Scoring**

By default queries are executed as a filter which means elasticsearch does not scores the results and they are returned in an arbitrary order. Add '_score' as one of the selected columns in order to change this behavior and request scoring. By default results are returned sorted on _score DESC (can be changed to ORDER BY _score ASC). Ordering on another field will disable scoring! In addition it is possible to get the id and type of a document by specifying _id and _type respectively.

``` sql
/* request scoring, document id and document type */
SELECT _id, _type, _score FROM mytype where myString = 'hello'
/* request scoring and all available fields */
SELECT _score, * FROM mytype WHERE myInt = 42
/* the following will not request any scoring! */
SELECT * FROM mytype WHERE myInt = 42
```

**Search/text matching**

Sql4es does not make a difference between searching and matching on textual fields. Behaviour totally depends on the analysis (if any) performed on the textual field being queried/searched. Under the hood a couple of simple rules are used to determine what type of query should be use:

- a single word will be put in a TermQuery (example: myString = 'hello')
- multiple words will be put in a MatchPhraseQuery (example: myString = 'hello there')
- presence of wildcards (% and \_) will trigger the use of a WildcardQuery (% is replaced with * and _ with ?). Examples: mystring = '%something' is the same as mystring LIKE '%something')
- the use of IN (…) will be put in a TermsQuery

**Aggregation**

Sql4es will request an aggregation whenever it finds a DISTINCT, GROUP BY or aggregation functions (MIN, MAX, SUM, AVG or COUNT) are requested without any normal fields. No search results are returned whenver an aggregation is requested. 

Sql4es supports some basic arithmetic functions: *, /, +, - and % (modulo).  It is also possible to combine diferent fields from the resultset within a calculation like AVG(field)/100 and SUM(field)/count(1). Note that within the current implementation these calculations are performed within the driver once data has been fetched from Elasticsearch

``` sql
/* Aggregates on a boolean and returns the sum of an int field in desc order */
SELECT myBool, sum(myInt) as summy ROM mytype GROUP BY myBool ORDER BY summy DESC

/* This is the same as above*/
SELECT DISTINCT myBool, sum(myInt) as summy ROM mytype ORDER BY summy DESC

/* Aggregates on a boolean and returns the sum of an int field only if it is larger than 1000 */
SELECT myBool, sum(myInt) as summy ROM mytype GROUP BY myBool HAVING sum(myInt) > 1000
/* Gets the average of myInt in two different ways... */
SELECT myBool, sum(myInt)/count(1) as average, avg(myInt) FROM mytype GROUP BY myBool

/* aggregation on all documents without a DISTINCT or GROUP BY */
SELECT count(*), SUM(myInt) from mytype

/* the following will NOT WORK, a DISTINCT or GROUP BY on mytext is required */
SELECT mytext, count(*), SUM(myInt) from mytype
```

Some notes on SELECT:

- limit only works on non aggregating queries. Any 'limits' on an aggregation will be omitted 
- calculations on fields are currently performed within the driver
- having (filtering on aggregated results) is currently performed within the driver
- sorting of aggregated results are currently performed within the driver

#### USE

Sql4es uses an active index/alias. By default this is the index/alias specified within the URL used to get the connection. It is possible to change the active index/alias by executing:

***USE [index / alias]***

All subsequent statements will be executed from the specified index/alias. This action only influences the driver and has no effect on Elasticsearch

#### CREATE & DROP

Sql4es supports creation of indices (create table) and aliases (create view). These statements require knowledge of ES mechanics like mappings, type definitions and aliases. 

***CREATE TABLE [(index.)type] ([field] "[field definition]" (, [field2])...) WITH ([[property="value"] (, property2=...) )***

This creates a mapping for [type] within the currently active index or in the index specified using dot notation. Whenever dotnotation is used it is assumed the part before the first dot refers to the index. If the index specified already exists it just adds the type to this index.

The field definition is the json definition put in the mapping without quoting the json elements! A string type can be defined as follows: *CREATE TABLE mytype (stringField "type:string, index:analyzed, analyzer:dutch")*. Any mapping elements, like templates, can be set using the WITH clause (see example below). All these json parts will be quoted properly and mashed together into a mapping request.

``` sql
/* creates a mapping for mytype within newindex with a template to store any strings without analysis */
CREATE TABLE index.mytype (
	myInt "type:integer",
  	myDate "type:date, format:yyyy-MM-dd"
  	myString "type:string, index:analyzed, analyzer:dutch"
) WITH (
  dynamic_templates="[{
    default_mapping: { 
    	match: *,
    	match_mapping_type: string, 
    	mapping: {type: string, index: not_analyzed }
    }
  }]"
)
```

An empty index can be created using CREATE TABLE [index] (_id "type:string"). The _id field is omitted because it is a standard ES field.

***CREATE TABLE [(index.)type] AS SELECT ...***

Creates a new index/type based on the results of a SELECT statement. The new fieldnames are taken from the SELECT, it is possible to use column-aliases to influence the fieldnames. For example CREATE TABLE myaverage AS SELECT avg(somefield) AS average will result in a new type myaverage within the currently active index with a single Double field called 'average'. Note that this is a two step process taking place at the driver. First the query is executed and secondly the index is created and results are written (in bulk) to the new type.

``` sql
/*Create another index with a type mapping based on the mapping created before*/
CREATE TABLE index.mytype AS SELECT myDate as date, myString as text FROM anyType

/* create a type with a (possibly expensive to calculate) aggregation result */
CREATE TABLE index.myagg AS SELECT myField, count(1) AS count, sum(myInt) AS sum from anyType GRUP BY myField ORDER BY count DESC
```

***CREATE VIEW [alias] AS SELECT * FROM [index1] (, [index2])... (WHERE [condition])***

Creates a new ES alias containing the specified indexes or adds the indexes to an existing alias. The optional WHERE clause adds a filter on the index-alias pairs specified. See the [elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html) for information on aliases

***DROP TABEL [index] / DROP VIEW [alias]***

Removes the specified index or alias

``` sql
/*Create an elasticsearch alias which includes two indexes with their types */
CREATE VIEW newalias AS SELECT * FROM newindex, newindex2

/* Same as above but with a filter*/
CREATE VIEW newalias AS SELECT * FROM newindex, newindex2 WHERE myInt > 99

/*Use the alias so it can be queried*/
USE newalias

/* removes myindex and remove newalias */
DROP TABLE myindexindex
DROP VIEW newalias
```

#### Update

Describes inserting and deleting data through sql

***INSERT INTO [(index.)type] ([field1], [field2]...) VALUES ([value1], [value2], ...), ([value1], ...), …***

Adds one or more documents to the specified type within the index. Fields must be defined and the number of values must match the number of fields defined. It is possible to add multiple documents within a single INSERT statement

***INSERT INTO  [(index.)type] SELECT …***

Adds all of the results from the SELECT statement to the specified type within the index. Fieldnames to insert are taken from the result (i.e. column aliases can be used). Note that, similar to the 'CREATE TABLE .. AS SELECT' the results are pulled into the driver and then indexed (using Bulk).

``` sql
/* Insert two documents into the mytype mapping */
INSERT INTO mytype (myInt, myDouble, myString) VALUES (1, 1.0, 'hi there'), (2, 2.0, 'hello!')

/* insert a single document */
INSERT INTO mytype (myInt, myDouble, myString) VALUES (3, 3.0, 'bye, bye')

/* copy records from anotherindex.mytype to myindex.mytype that meet a certain condition */
USE anotherindex
INSERT INTO myindex.mytype SELECT * from newtype WHERE myInt < 3 
```

***DELETE FROM [type] (WHERE [condition])***

Deletes all documents from the specified type that meet the condition. If no WHERE clause is specified all documents will be removed. As Elasticsearch can only delete documents based on their \_id which means that this statement is executed in two steps. First collect all _id's from documents that meet the condition, secondly delete those documents using the bulk API

``` sql
/* delete documents that meet a certain condition*/
DELETE FROM mytype WHERE myInt == 3

/*delete all documents from mytype*/
DELETE FROM mytype 
```

### Configuration

It is possible to set parameters through the provided url. All parameters are exposed to elastic search as well which means that is is possible to set Client parameters, see [elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html). The following driver specific parameters can be set:

- es.hosts: a comma seperated list with additional hosts with optional ports in the format host1(:port1), host2(:port2) … The default port 9300 is taken when no port is specified. 
- fetch.size (int default 10000): maximum number of results to fetch in a single request (10000 is elasticsearch's maximum). Can be lowered to avoid memory issues when documents fetched are very larged.
- scroll.timeout.sec (int, default 60): the time a scroll id remains valid and 'getMoreResults()' can be called. Should be increased or decreased depending on the usecase.
- query.timeout.ms (int, default 10000): the timeout set on a query. Can be altered depending on the use case.
- default.row.length (int, default 250): the initial number of columns created for results. Increase this property only when results do not fit (typically indicated by an array index out of bounds exception).
- query.cache.table (string, default 'query_cache'): the fictional table name used to indicate a query & result must be cached. Can be changed to make it shorter or more convenient 
- result.nested.lateral (boolean, default true): specifies weather nested results must be exploded (the default) or not. Can be set to false when working with the driver from your own code. In this case a column containing a nested object (wrapped in a ResultSet) will have java.sql.Types =  Types.JAVA_OBJECT and can be used as (ResultSet)rs.getObject(colNr).