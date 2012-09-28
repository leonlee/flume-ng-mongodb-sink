package org.riderzen.flume.sink;

import com.mongodb.*;
import org.apache.commons.collections.MapUtils;
import org.apache.flume.*;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.json.simple.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.*;

import static org.testng.Assert.*;

/**
 * User: guoqiang.li
 * Date: 12-9-12
 * Time: 下午3:31
 */
public class MongoSinkTest {
    private static Mongo mongo;
    public static final String DBNAME = "myDb";

    private static Context ctx = new Context();
    private static Channel channel;


    @BeforeClass(groups = {"dev"})
    public static void setup() throws UnknownHostException {
        mongo = new Mongo("localhost", 27017);

        Map<String, String> ctxMap = new HashMap<String, String>();
        ctxMap.put(MongoSink.HOST, "localhost");
        ctxMap.put(MongoSink.PORT, "27017");
        ctxMap.put(MongoSink.DB_NAME, "test_events");
        ctxMap.put(MongoSink.COLLECTION, "test_log");
        ctxMap.put(MongoSink.BATCH_SIZE, "100");

        ctx.putAll(ctxMap);

        Context channelCtx = new Context();
        channelCtx.put("capacity", "1000000");
        channelCtx.put("transactionCapacity", "1000000");
        channel = new MemoryChannel();
        Configurables.configure(channel, channelCtx);
    }

    @AfterClass(groups = {"dev"})
    public static void tearDown() {
        mongo.dropDatabase(DBNAME);
        mongo.dropDatabase("test_events");
        mongo.close();
    }

    @Test(groups = "dev", invocationCount = 1)
    public void sinkDynamicTest() throws EventDeliveryException, InterruptedException {
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

        JSONObject msg = new JSONObject();
        msg.put("age", 11);
        msg.put("birthday", new Date().getTime());

        Transaction tx;

        for (int i = 0; i < 100; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("name", "test" + i);
            JSONObject header = new JSONObject();
            header.put(MongoSink.COLLECTION, "my_events");

            Event e = EventBuilder.withBody(msg.toJSONString().getBytes(), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        sink.process();
        sink.stop();

        for (int i = 0; i < 10; i++) {
            msg.put("name", "test" + i);

            System.out.println("i = " + i);

            DB db = mongo.getDB("test_events");
            DBCollection collection = db.getCollection("my_events");
            DBCursor cursor = collection.find(new BasicDBObject(msg));
            assertTrue(cursor.hasNext());
            DBObject dbObject = cursor.next();
            assertNotNull(dbObject);
            assertEquals(dbObject.get("name"), msg.get("name"));
            assertEquals(dbObject.get("age"), msg.get("age"));
            assertEquals(dbObject.get("birthday"), msg.get("birthday"));
        }
    }

    @Test(groups = "dev")
    public void sinkDynamicTest2() throws EventDeliveryException, InterruptedException {
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

        JSONObject msg = new JSONObject();
        msg.put("age", 11);
        msg.put("birthday", new Date().getTime());

        Transaction tx;

        for (int i = 0; i < 100; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("name", "test" + i);
            JSONObject header = new JSONObject();
            header.put(MongoSink.COLLECTION, "my_events" + i % 10);

            Event e = EventBuilder.withBody(msg.toJSONString().getBytes(), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        sink.process();
        sink.stop();

        for (int i = 0; i < 10; i++) {
            msg.put("name", "test" + i);

            System.out.println("i = " + i);

            DB db = mongo.getDB("test_events");
            DBCollection collection = db.getCollection("my_events" + i % 10);
            DBCursor cursor = collection.find(new BasicDBObject(msg));
            assertTrue(cursor.hasNext());
            DBObject dbObject = cursor.next();
            assertNotNull(dbObject);
            assertEquals(dbObject.get("name"), msg.get("name"));
            assertEquals(dbObject.get("age"), msg.get("age"));
            assertEquals(dbObject.get("birthday"), msg.get("birthday"));
        }
    }

    @Test(groups = "dev")
    public void sinkSingleModelTest() throws EventDeliveryException {
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.single.name());

        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

        Transaction tx = channel.getTransaction();
        tx.begin();
        JSONObject msg = new JSONObject();
        msg.put("name", "test");
        msg.put("age", 11);
        msg.put("birthday", new Date().getTime());

        Event e = EventBuilder.withBody(msg.toJSONString().getBytes());
        channel.put(e);
        tx.commit();
        tx.close();

        sink.process();
        sink.stop();

        DB db = mongo.getDB("test_events");
        DBCollection collection = db.getCollection("test_log");
        DBCursor cursor = collection.find(new BasicDBObject(msg));
        assertTrue(cursor.hasNext());
        DBObject dbObject = cursor.next();
        assertNotNull(dbObject);
        assertEquals(dbObject.get("name"), msg.get("name"));
        assertEquals(dbObject.get("age"), msg.get("age"));
        assertEquals(dbObject.get("birthday"), msg.get("birthday"));
    }

    @Test(groups = "dev")
    public void dbTest() {
        DB db = mongo.getDB(DBNAME);
        db.getCollectionNames();
        List<String> names = mongo.getDatabaseNames();

        assertNotNull(names);
        boolean hit = false;

        for (String name : names) {
            if (DBNAME.equals(name)) {
                hit = true;
                break;
            }
        }

        assertTrue(hit);
    }

    @Test(groups = "dev")
    public void collectionTest() {
        DB db = mongo.getDB(DBNAME);
        DBCollection myCollection = db.getCollection("myCollection");
        myCollection.save(new BasicDBObject(MapUtils.putAll(new HashMap(), new Object[]{"name", "leon", "age", 33})));
        myCollection.findOne();

        Set<String> names = db.getCollectionNames();

        assertNotNull(names);
        boolean hit = false;

        for (String name : names) {
            if ("myCollection".equals(name)) {
                hit = true;
                break;
            }
        }

        assertTrue(hit);
    }


}
