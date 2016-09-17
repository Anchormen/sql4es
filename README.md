### sql4es: JDBC driver for Elasticsearch

Sql-for-Elasticsearch (sql4es) is a jdbc 4.1 driver for **Elasticsearch 2.0 - 2.4** implementing the majority of the JDBC interfaces: Connection, Statement, PreparedStatment, ResultSet, Batch and DataBase- /  ResultSetMetadata. The screenshot below shows SQLWorkbenchJ with a selection of SQL statements that can be executed using the driver. As of version 0.8.2.3 the driver supports Shield allowing the use of credentials and SSL.

![SQLWorkbenchJ screenshot with examples](release/workbench_examples.png)

#### Usage

The sql4es driver can be used by adding the jar file, found within the releases directory of the project, to the tool/application used and load the driver with name '***nl.anchormen.sql4es.jdbc.ESDriver***'. The driver expects an URL with the following format: ***jdbc:sql4es://host:port/index?params***. 

- host: the hostname or ip of one of the es hosts (required)
- port: an optional the port number to use for the transport client (default is 9300)
- index: the optional index to set active within the driver. Most statements like SELECT, DELETE and INSERT require an active index (also see USE [index/alias] statement below). It is however possible to create new indices, types and aliases without an active index.
- params: an optional set of parameters used to influence the internals of the driver (specify additional hosts, maximum number of documents to fetch in a single request etc). If your clustername is not 'elasticsearch' you should specify the clustername witin the url (see example below). Please see the Configuration section of this readme for a description of all driver specific parameters.

``` java
// register the driver and get a connection for index 'myidx'
Class.forName("nl.anchormen.sql4es.jdbc.ESDriver");
Connection con = DriverManager.getConnection("jdbc:sql4es://localhost:9300/myidx?cluster.name=your-cluster-name");
Statement st = con.createStatement();
// execute a query on mytype within myidx
ResultSet rs = st.executeQuery("SELECT * FROM mytype WHERE something >= 42");
ResultSetMetaData rsmd = rs.getMetaData();
int nrCols = rsmd.getColumnCount();
// get other column information like type
while(rs.next()){
	for(int i=1; i<=nrCols; i++){
  		System.out.println(rs.getObject(i));
	}
}
rs.close();
con.close();
```

The driver can also be used from applications able to load the jdbc driver. It has been tested with [sqlWorkbench/J](http://www.sql-workbench.net/) and [Squirrel](http://squirrel-sql.sourceforge.net/) on an Elasticsearch 2.3 cluster. A description on how to use sql4es with sqlWorkbenchJ along with a number of example queries can be found at the bottom of this readme.

As of version 0.8.2.3 the driver supports **Shield**. The following URL prameters must be set in order to use shield:

* shield.user='username:password' to set global credentials
* ssl (enables SSL)
* cluster.name='elastic cloud cluster id' (only applies when connecting with a cluster in the Elastic Cloud)

The example below is used to connect with a Shield protected cluster in the Elastic Cloud using SSL.

```scala
Connection con = DriverManager.getConnection("jdbc:sql4es://f03c93be1efeb9be9b2f46b660d10d90.eu-west-1.aws.found.io:9343/indexname?shield.user=username:password&cluster.name=f03c93be1efeb9be9b2f46b660d10d90&ssl");
```



### Supported SQL

Simply said the sql4es driver translates SQL statements to their Elasticsearch counterparts and parses results into ResultSet implementations. The following sql statements are supported:

- SELECT: fetches documents (with or without scoring) or aggregations from elasticsearch
  * COUNT, MIN, MAX, SUM, AVG
  * DISTINCT
  * WHERE (=, >, >=, <, <=, <>, IN, LIKE, AND, OR, IS NULL, IS NOT NULL, NOT [condition])
  * GROUP BY
  * HAVING
  * ORDER BY
  * LIMIT (without offset, offsets are not supported by sql4es)
- CREATE TABLE (AS) creates an index/type and optionally indexes the result of a query into it
- CREATE VIEW (AS): creates an alias, optionally with a filter
- DROP TABLE/VIEW removes an index or alias
- INSERT INTO (VALUES | SELECT): inserts documents into an index/type; either provided values or results of a query. Possible to UPDATE documents using INSERT by specifying existing document _id's
- UPDATE: executed as an elasticsearch Upsert
- DELETE FROM (WHERE): removes documents
- USE: selects an index as the driver's active one (used to interpret queries)
- EXPLAIN SELECT: returns the Elasticsearch query performed for a SELECT statement
- Table aliases like SELECT … FROM table1 as T1, table2 t2...
  - Table aliases are parsed but not used during query execution

**Remarks**

Elasticsearch does not support transactions. Hence executing batches cannot be rolled back upon failure (nor can statements be committed). It also takes some time for documents to be indexed fully so executing an INSERT directly followed by a SELECT might not include the inserted documents.

Some SQL statements or Elasticsearch features that are ***not (yet) supported***:

- ~~UPDATE is not supported, although it is possible to update documents by inserting values for an existing _id~~
  - added in 0.7.2.1: it is now possible to executes updates like UPDATE index.type SET myInt=100 WHERE myString = 'hundred'
- ~~Not possible to INSERT nestested objects~~
  - added in 0.7.2.1 using double quotes: INSERT INTO mytype ("myObject.nestedDoc.myInt") VALUES (1)
- Not possible to specify offsets (OFFSET offset or LIMIT offset, number)
- ~~Fields with type 'nested' are not supported because this type requires different methods to query and retrieve data.~~ 
  - added in 0.6.2.1: Nested types are detected by the driver and queries on those fields are executed accordingly
- Parent child relationships are not supported. It is currently not possible to index or retrieve fields of this type.
- Elasticsearch features like ~~full text search, highlighting,~~ suggestions and  templates are not supported.
  - added in 0.6.2.1: full text search can be done using *_search = '…'* and highlighting trough *SELECT highlight(some-field) FROM …*

### Concepts

Since elasticsearch is a NO-SQL database it does not contain the exact relational objects most people are familiar with (like databases, tables and records). Elasticsearch does however have a similar hierarchy of objects (index, type and document). The conceptual mapping used by sql4es is the following:

- Database = Index
- Table = Type
- Record = document
- Column = Field
- View = Alias (this does not fit from a hierarchical perspective but does match given the purpose of views / aliases)

Elasticsearch responses, both search results and aggregations, are put into a ResultSet implementation. Any nested objects are 'exploded' into a lateral view by default; this means that nested objects are treated as joined tables which are put inside the he same row (see [this page](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView) for explanation). It is possible to represent nested objects as a nested ResultSet, see the Configuration section. Note, that although objects are exploded, arrays with primitives are not! They are put in a java.sql.Array implementation supported by JDBC.

Sql4es works from an active index/alias which means that it resolves references to types from this index. If for example *myIndex* is currently active the query *SELECT * FROM sometype* will only return any results if sometype is part of myindex. Executing a SELECT on a type that does not exist within an index will return an empty result. It is possible to change the active index by executing *USE [otherIndex]* as described below. 

### QUERIES

This section describes how SQL is interpreted and converted into SE statements. The presto parser is used to parse SQL statements, please see the syntax definition on the [presto website](https://prestodb.io/docs/current/sql.html).

#### SELECT

``` sql
/* basic syntax */
SELECT [field (AS alias)] FROM [types] WHERE [condition] GROUP BY [fields] HAVING [condition] ORDER BY [field (ASC|DESC)] LIMIT [number]
```

- fields (AS alias): defines the fields to be retrieved from elasticsearch and put in the ResultSet. It is possible to use * to indicate all fields should be retrieved (including _id and _type). Fields can be addressed by their name, nested fields can be addressed  using their hierarchical names in dotted notation like: *nesteddoc.otherdoc.field*. Using a star will simply fetch all fields, also nested ones, present in a document. It is possible to specify the root of an object in order to fetch all its fields. A query like *SELECT nesteddoc FROM type* will fetch all fields present in nesteddoc. As a result it might return hundreds of columns if nesteddoc has hundreds of fields.
- types: the types to execute the query against. This can only be types present in the index or alias that is currently active (also see 'use' statement).
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
/* fetch some data from type */
SELECT DISTINCT field, count(1) FROM type, query_cache
/* exactly the same as above but now also hitting the query cache */
SELECT DISTINCT field, count(1) FROM type
```

**Text matching, search and scoring**

By default queries are executed as a filter which means elasticsearch does not scores the results and they are returned in an arbitrary order. Add '_score' as one of the selected columns in order to change this behaviour and request scoring. By default results are returned sorted on _score DESC (can be changed to ORDER BY _score ASC). Ordering on another field will disable scoring! In addition it is possible to get the id and type of a document by specifying _id and _type respectively.

Sql4es does not make a difference between searching and matching on textual fields. Behaviour totally depends on the analysis (if any) performed on the textual field being queried/searched. Under the hood a couple of simple rules are used to determine what type of query should be use:

- a single word will be put in a TermQuery (example: myString = 'hello')
- multiple words will be put in a MatchPhraseQuery (example: myString = 'hello there')
- presence of wildcards (% and \_) will trigger the use of a WildcardQuery (% is replaced with * and _ with ?). Examples: mystring = '%something' is the same as mystring LIKE '%something')
- the use of IN (…) will be put in a TermsQuery

In addition it is possible to execute a regular search with all features supported by ES. Searching is done by executing a match on the fictional field '_search' (see examples below). It is possible to request highlights for any text field using the highlight function like: SELECT highlight(field), … Fragment size and number can be set through the global configuration.

``` sql
/* term query */
SELECT _score, myString FROM mytype WHERE myString = 'hello' OR myString = 'there'
/* Same as above */
SELECT _score, myString FROM mytype WHERE myString IN ('hello', 'there')
/* use of NOT; find all documents which do not contain 'hello' or 'there' */
SELECT _score, myString FROM mytype WHERE NOT myString IN ('hello', 'there')

/* check for NULL values (missing fields) */
SELECT myInt FROM mytype WHERE myString NOT NULL
SELECT myInt FROM mytype WHERE myString IS NULL

/* phrase query */
SELECT _score, highlight(myString), myString FROM mytype WHERE myString = 'hello there'
/* wildcard query */
SELECT _score, myString FROM mytype WHERE myString = 'hel%'
/* a search for exactly the same as the first two */
SELECT _score, highlight(mystirng) FROM mytype WHERE _search = 'myString:(hello OR there)'
```

**Get document by _id**

It is possible to execute searches for document id's by specifying '=' or IN predicates on the _id field. It is possible to combine the match on an _id with other fields but matching multiple _id should always be done using IN.

``` sql
SELECT * FROM mytype WHERE _id = 'whatever_id'
SELECT * FROM mytype WHERE _id = 'whatever_id' AND myInt > 3
SELECT * FROM mytype WHERE _id = 'whatever_id' OR _id = 'another_ID' /* WRONG */
SELECT * FROM mytype WHERE _id IN ('whatever_id', 'another_ID') /* CORRECT */
```

**Aggregation**

Sql4es will request an aggregation whenever it finds a DISTINCT, GROUP BY or aggregation functions (MIN, MAX, SUM, AVG or COUNT) are requested without any normal fields. No search results are returned whenever an aggregation is requested. 

Sql4es supports some basic arithmetic functions: \*, /, +, - and % (modulo).  It is also possible to combine different fields from the resultset within a calculation like AVG(field)/100 and SUM(field)/count(1). Note that within the current implementation these calculations are performed within the driver once data has been fetched from Elasticsearch. It is possible to refer to values in other rows within functions using brackets *[offset]*. For example SUM(volume)/SUM(volume)[-1] will devide the sum of volume column for row X with the value in row X-1. If a value cannot be calculated, for example row number 0 in the example above, it will get value Float.NaN.  

``` sql
/* Aggregates on a boolean and returns the sum of an int field in desc order */
SELECT myBool, sum(myInt) as summy FROM mytype GROUP BY myBool ORDER BY summy DESC

/* This is the same as above */
SELECT DISTINCT myBool, sum(myInt) as summy ROM mytype ORDER BY summy DESC

/* Aggregates on a boolean and returns the sum of an int field only if it is larger than 1000 */
SELECT myBool, sum(myInt) as summy ROM mytype GROUP BY myBool HAVING sum(myInt) > 1000

/* Gets the average of myInt in two different ways... */
SELECT myBool, sum(myInt)/count(1) as average, avg(myInt) FROM mytype GROUP BY myBool

/* Calculates the percentage of growth of the myInt value acros increasing dates */
SELECT myDate, sum(myInt)/sum(myInt)[-1]*100 FROM mytype GROUP BY myDate ORDER BY myDate ASC

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

#### EXPLAIN

Explain can be used to view the elasticsearch query executed for a SELECT statement by executing:

***EXPLAIN [SELECT statement]***

#### USE

Sql4es uses an active index/alias. By default this is the index/alias specified within the URL used to get the connection (if any). It is possible to change the active index/alias by executing:

***USE [index / alias]***

All subsequent statements will be executed from the specified index/alias. This action only influences the driver and has no effect on Elasticsearch

#### CREATE & DROP

Sql4es supports creation of indices, types (create table) and aliases (create view). These statements require knowledge of ES mechanics like mappings, type definitions and aliases. 

***CREATE TABLE (index.)type ([field] "[field definition]" (, [field2])...) WITH (property="value" (, property2=...) )***

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

An empty index can be created using CREATE TABLE index.type (_id "type:string"). The _id field is omitted because it is a standard ES field.

***CREATE TABLE (index.)type AS SELECT ...***

Creates a new index/type based on the results of a SELECT statement. The new fieldnames are taken from the SELECT, it is possible to use column-aliases to influence the fieldnames. For example CREATE TABLE myaverage AS SELECT avg(somefield) AS average will result in a new type myaverage within the currently active index with a single Double field called 'average'. Note that this is a two step process taking place at the driver. First the query is executed and secondly the index is created and results are written (in bulk) to the new type.

``` sql
/*Create another index with a type mapping based on the mapping created before*/
CREATE TABLE index.mytype AS SELECT myDate as date, myString as text FROM anyType

/* create a type with a (possibly expensive to calculate) aggregation result */
CREATE TABLE index.myagg AS SELECT myField, count(1) AS count, sum(myInt) AS sum from anyType GROUP BY myField ORDER BY count DESC
```

***CREATE VIEW [alias] AS SELECT * FROM index1 (, [index2])... (WHERE [condition])***

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

#### INSERT & DELETE

Describes inserting and deleting data through sql

***INSERT INTO (index.)type ([field1], [field2]...) VALUES ([value1], [value2], ...), ([value1], ...), …***

Adds one or more documents to the specified type within the index. Fields must be defined and the number of values must match the number of fields defined. It is possible to add multiple documents within a single INSERT statement. It is possible to specify the _id field within the insert statement. In this case it will force elasticsearch to insert the specified document id. The insert acts as an UPDATE if the _id already exists! It is not possible to insert nested objects as they cannot be specified in the SQL language.

***INSERT INTO  (index.)type SELECT …***

Adds all of the results from the SELECT statement to the specified type within the index. Fieldnames to insert are taken from the result (i.e. column aliases can be used). Note that, similar to the 'CREATE TABLE .. AS SELECT' the results are pulled into the driver and then indexed (using Bulk).

``` sql
/* Insert two documents into the mytype mapping */
INSERT INTO mytype (myInt, myDouble, myString) VALUES (1, 1.0, 'hi there'), (2, 2.0, 'hello!')

/* insert a single document, using quotes around nested object fields */
INSERT INTO mytype (myInt, myDouble, "nestedObject.myString") VALUES (3, 3.0, 'bye, bye')

/* update or insert a document with specified _id */
INSERT INTO mytype (_id, myInt, myDouble) VALUES ('some_document_id', 4, 4.0)

/* copy records from anotherindex.mytype to myindex.mytype that meet a certain condition */
USE anotherindex
INSERT INTO myindex.mytype SELECT * from newtype WHERE myInt < 3 
```

***DELETE FROM type (WHERE [condition])***

Deletes all documents from the specified type that meet the condition. If no WHERE clause is specified all documents will be removed. As Elasticsearch can only delete documents based on their \_id which means that this statement is executed in two steps. First collect all _id's from documents that meet the condition, secondly delete those documents using the bulk API

``` sql
/* delete documents that meet a certain condition*/
DELETE FROM mytype WHERE myInt == 3

/*delete all documents from mytype*/
DELETE FROM mytype 
```

### UPDATE

It is possible to update documents within an index/type using standard SQL syntax. Note that nested object names must be surrounded by double quotes:

***UPDATE index.type SET field1=value, fiedl2='value', "doc.field"=value WHERE condition***

The update is executed in two steps. First the _id's of all documents matching the condition are fetched after which the specified fields for these documents are updated in batch using the Upsert API. 

### Configuration

It is possible to set parameters through the provided url. All parameters are exposed to elastic search as well which means that is is possible to set Client parameters, see [elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html). The following driver specific parameters can be set:

- es.hosts: a comma separated list with additional hosts with optional ports in the format host1(:port1), host2(:port2) … The default port 9300 is taken when no port is specified. 
- fetch.size (int default 10000): maximum number of results to fetch in a single request (10000 is elasticsearch's maximum). Can be lowered to avoid memory issues when documents fetched are very large.
- results.split (default: false): setting this will split the entire result into multiple ResultSet objects, each with maximal *fetch.size* number of records. The next ResultSet can be fetched using Statement.getMoreResults(). The default is *false* and the driver will put all results within a single ResultSet. This setting should be used when the client has insufficient memory to hold all the results within a single ResultSet.
- scroll.timeout.sec (int, default 10): the time a scroll id remains valid and 'getMoreResults()' can be called. Should be increased or decreased depending on the scenario at hand.
- query.timeout.ms (int, default 10000): the timeout set on a query. Can be altered depending on the use case.
- default.row.length (int, default 250): the initial number of columns created for results. Increase this property only when results do not fit (typically indicated by an array index out of bounds exception) triggered when search results are parsed.
- query.cache.table (string, default 'query_cache'): the fictional table name used to indicate using elasticsearch query cache. Can be changed to make it shorter or more convenient.
- result.nested.lateral (boolean, default true): specifies weather nested results must be exploded (the default) or not. Can be set to false when working with the driver from your own code. In this case a column containing a nested object (wrapped in a ResultSet) will have java.sql.Types =  Types.JAVA_OBJECT and can be used as (ResultSet)rs.getObject(colNr).
- fragment.size (int, default 100): specifies the preferred fragment length in characters.
- fragment.count (int, default 1): specifies the maximum number of fragments to return when requesting highlighting.

### Example using SQLWorkbenchJ

SQLWorkbenchJ is a SQL GUI that can be used to access an Elasticsearch cluster using the sql4es driver. Follow the steps below to set it up and execute the example statements.

1. download SQLWorkbenchJ for your platform from the [website](http://www.sql-workbench.net/downloads.html)
2. Install SQLWorkbencJ and open it
3. Add the SQL4ES driver:
   1. click 'Manage Drivers' in the bottom left corner
   2. click the blank document icon on the left to add a new driver
   3. Give the driver a descriptive name (sql4es for example)
   4. point the Library to the sql4es jar file on your system (found within the release directory of the project)
   5. set 'nl.anchormen.sql4es.jdbc.ESDriver' as the Classname
   6. set 'jdbc:sql4es://localhost:9300/index' as the sample URL
4. Click 'Ok' to save the configuration
5. Add a new database connection using the sql4es driver
   1. click the 'create new connection profile' button in the top
   2. give the profile a descriptive name
   3. specify the sql4es driver added before
   4. specify the url of your Elasticsearch 2.X cluster using the index 'myindex' ('jdbc:sql4es://your.es.host:port/myindex'
   5. click the save button on the top
6. Select the created profile and click 'Ok' to create the connection
   1. An empty workbench will open when everything is ok.
7. Copy the statements below into the workbench
8. Execute the statements one by one and view the results. A brief description of each statement can be found below the SQL statements
   1. Running all the statements as a sequence works as well but will not provide any results for the SELECT statements because the cluster is still indexing the inserts when they are being executed (remember, elasticsearch is not a relational database!)

``` sql
INSERT INTO mytype (myLong, myDouble, myDate, myText) VALUES (1, 1.25, '2016-02-01', 'Hi there!'),(10, 103.234, '2016-03-01', 'How are you?');

SELECT * FROM mytype;
SELECT _score, myLong, myText FROM mytype WHERE myText = 'hi' AND myDate > '2016-01-01';

EXPLAIN SELECT _score, myLong, myText FROM mytype WHERE myText = 'hi' AND myDate > '2016-01-01';

CREATE TABLE myindex2.mytype2 AS SELECT myText, myLong*10 as myLong10, myDouble FROM mytype;

DELETE FROM mytype WHERE myDouble = 1.25;
USE myindex2;
SELECT * FROM mytype2;
CREATE VIEW myview AS SELECT * FROM myindex, myindex2 WHERE myDouble > 2;
USE myview;
SELECT * FROM mytype, mytype2;
drop view myview;
drop table myindex;
drop table myindex2;
```

1. insert two documents into the new type called mytype (the type will be created and mapping will be done dynamically by elasticsearch). Check the DatabaseExplorer option to view the mapping created.

2. show all documents present in mytype

3. execute a search and show the score

4. show the Elasticsearch query performed for the search

5. create a new index and a new type based on the documents in mytype. Note that mytype2 has some different fieldnames than the type it was created of.

6. delete some documents

7. make the newly created index 'myindex2' the active one

8. show all documents within mytype2

9. create a view (alias) for myindex and myindex2 with a filter

10. make the newly created view the active one

11. select all documents present within the view, note that: 

12. not all documents are shown due to the filter on the alias

13. some of the fields are empty because the two types queried the empty fields because the two types have a couple of different fields (myDate, myLong and myLong10)

     ​
