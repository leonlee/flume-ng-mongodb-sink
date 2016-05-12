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
	model: single or dynamic [single], single means all data will insert into the same collection,
	    and dynamic means every event will specify collection name by event header 'collection' 
	db: db name [events]
	collection: default collection name, will used in single model [events]
	batch: batch size of insert opertion [100]
	autoWrap: indicator of wrap the event body as a JSONObject that has one field [false]
	wrapField: using with autoWrap, set the field name of JSONObject [log]
	timestampField: date type field that record the creating time of record,
	    it can be a existing filed name that the sink will convert this filed to date type,
	    or it's a new filed name that the sink will create it automatically []
        the supported date pattern as follows:
            "yyyy-MM-dd"
            "yyyy-MM-dd HH:mm:ss"
            "yyyy-MM-dd HH:mm:ss.SSS"
            "yyyy-MM-dd HH:mm:ss Z"
            "yyyy-MM-dd HH:mm:ss.SSS Z"
            "yyyy-MM-dd'T'HH:mm:ssZ"
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
            "yyyy-MM-dd'T'HH:mm:ssz"
            "yyyy-MM-dd'T'HH:mm:ss.SSSz"
    authenticationEnabled: true means login by username/password, false means login without authentication [false]
    extraFields: use with extra fields to tag the information, for example your ip address, computer name[]
    username: required when "authenticationEnabled" is true []
    password: required when "authenticationEnabled" is true []

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
	agent2.sinks.sink2.extraFields.ip = 192.168.1.1
	
	agent2.sinks.sink2.channel = channel2
	
	agent2.channels.channel2.type = memory
	agent2.channels.channel2.capacity = 1000000
	agent2.channels.channel2.transactionCapacity = 800
	agent2.channels.channel2.keep-alive = 3

### Event Headers
    The sink supports some headers in dynamic model:
    'db': db name
    'collection' : collection name
