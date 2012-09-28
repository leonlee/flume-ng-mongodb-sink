flume-ng-mongodb-sink
=============
Flume NG MongoDB sink. The source was implemented to populate JSON into MongoDB.

## Getting Started
- - -
1. Clone the repository
2. Install latest Maven and build source by 'mvn package'
3. Generate classpath by 'mvn dependency:build-classpath'
4. Append classpath in $FLUME_HOME/conf/flume-env.sh
5. Add the source definition according to **Configuration**

## Configuration
- - - 
	type: org.riderzen.flume.sink.MongoSink
	host: db host [localhost]
	port: db port [27017]
	username: db username []
	password: db password []
	model: single or dynamic, single mean all data will insert into the same collection, and dynamic means every event will specify cllection name by event header 'collection' [single]
	db: db name [events]
	collection: default collection name, will used in single model [events]
	batch: batch size of insert opertion [100]
