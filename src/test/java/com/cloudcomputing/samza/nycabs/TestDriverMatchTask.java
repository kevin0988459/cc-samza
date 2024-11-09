package com.cloudcomputing.samza.nycabs;

import java.time.Duration;
import java.util.ListIterator;
import java.util.Map;
import java.util.HashMap;

import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Assert;
import org.junit.Test;
import com.cloudcomputing.samza.nycabs.application.DriverMatchTaskApplication;

public class TestDriverMatchTask {
    @Test
    public void testDriverMatchTask() throws Exception {
        Map<String, String> confMap = new HashMap<>();
        confMap.put("stores.driver-loc.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        confMap.put("stores.driver-loc.key.serde", "string");
        confMap.put("stores.driver-loc.msg.serde", "json");
        confMap.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
        confMap.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");

        InMemorySystemDescriptor isd = new InMemorySystemDescriptor("kafka");

        InMemoryInputDescriptor imdriverLocation = isd.getInputDescriptor("driver-locations", new NoOpSerde<>());

        InMemoryInputDescriptor imevents = isd.getInputDescriptor("events", new NoOpSerde<>());

        InMemoryOutputDescriptor outputMatchStream = isd.getOutputDescriptor("match-stream", new NoOpSerde<>());

        TestRunner
                .of(new DriverMatchTaskApplication())
                .addInputStream(imevents, TestUtils.genStreamData("events"))
                .addInputStream(imdriverLocation, TestUtils.genStreamData("driver-locations"))
                .addOutputStream(outputMatchStream, 1)
                .addConfig(confMap)
                .addConfig("deploy.test", "true")
                .run(Duration.ofSeconds(5));

        Assert.assertEquals(5, TestRunner.consumeStream(outputMatchStream, Duration.ofSeconds(10)).get(0).size());

        ListIterator<Object> resultIter = TestRunner.consumeStream(outputMatchStream, Duration.ofSeconds(10)).get(0).listIterator();
        Map<String, Object> genderTest = (Map<String, Object>) resultIter.next();

        Assert.assertTrue(genderTest.get("clientId").toString().equals("3")
                && genderTest.get("driverId").toString().equals("9001"));

        Map<String, Object> salaryTest = (Map<String, Object>) resultIter.next();
        Assert.assertTrue(salaryTest.get("clientId").toString().equals("4")
                && salaryTest.get("driverId").toString().equals("8000"));

        Map<String, Object> ratingTest = (Map<String, Object>) resultIter.next();
        Assert.assertTrue(ratingTest.get("clientId").toString().equals("5")
                && ratingTest.get("driverId").toString().equals("8000"));
        Map<String, Object> distanceTest = (Map<String, Object>) resultIter.next();
        Assert.assertTrue(distanceTest.get("clientId").toString().equals("6")
                && distanceTest.get("driverId").toString().equals("7001"));
        Map<String, Object> rightBlockTest = (Map<String, Object>) resultIter.next();
        System.out.println(rightBlockTest.get("clientId").toString());
        System.out.println(rightBlockTest.get("driverId").toString());
        Assert.assertTrue(rightBlockTest.get("clientId").toString().equals("7")
                        && rightBlockTest.get("driverId").toString().equals("3002"));
    }
}