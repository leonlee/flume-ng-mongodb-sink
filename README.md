flume-ng-mongodb-sink
=============
Flume NG MongoDB sink. The source was implemented to populate JSON into MongoDB.

## Getting Started
- - -
1. Clone the repository
2. Install latest Maven and build source by 'mvn package'
3. Generate classpath by 'mvn dependency:build-classpath'
4. Append classpath in $FLUME_HOME/conf/flume-env.sh
5. Add the sink definition according to **Configuration**

## Configuration
- - - 
	type: org.riderzen.flume.sink.MongoSink
	host: db host [localhost]
	port: db port [27017]
	username: db username []
	password: db password []
	model: single or dynamic, single mean all data will insert into the same collection,
	    and dynamic means every event will specify cllection name by event header 'collection' [single]
	db: db name [events]
	collection: default collection name, will used in single model [events]
	batch: batch size of insert opertion [100]
	autoWrap: indicator of wrap the event body as a JSONObject that has one field [false]
	wrapField: use with autoWrap, set the field name of JSONObject [log]

### flume.conf sample
- - -
	agent2.sources = source2
	agent2.channels = channel2
	agent2.sinks = sink2
	
	agent2.sources.source2.type = org.riderzen.flume.source.MsgPackSource
	agent2.sources.source2.bind = localhost
	agent2.sources.source2.port = 1985
	
	agent2.sources.source2.channels = channel2
	
	agent2.sinks.sink2.type = org.riderzen.flume.sink.MongoSink
	agent2.sinks.sink2.host = localhost
	agent2.sinks.sink2.port = 27017
	agent2.sinks.sink2.model = single
	agent2.sinks.sink2.collection = events
	agent2.sinks.sink2.batch = 100
	
	agent2.sinks.sink2.channel = channel2
	
	agent2.channels.channel2.type = memory
	agent2.channels.channel2.capacity = 1000000
	agent2.channels.channel2.transactionCapacity = 800
	agent2.channels.channel2.keep-alive = 3

### Event Headers
    The sink supports some headers in dynamic model:
    'db': db name
    'collection' : collection name
