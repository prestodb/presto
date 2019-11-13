/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kafka;

import com.facebook.presto.kafka.util.TestUtils;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.curator.test.TestingServer;
import org.json.simple.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;

public class TestZookeeperMonitor
{
    private static final Logger log = Logger.get(TestZookeeperMonitor.class);

    private KafkaZookeeperServersetMonitor kafkaZookeeperServersetMonitor;
    private TestingServer zkServer;
    private ZkClient zkClient;
    private final String zkPath = "/kafka";

    public TestZookeeperMonitor()
            throws Exception
    {
        zkServer = new TestingServer(TestUtils.findUnusedPort());
        zkClient = new ZkClient(zkServer.getConnectString(), 30_000, 30_000);

        // Set the serializer
        zkClient.setZkSerializer(new ZkSerializer() {
            @Override
            public byte[] serialize(Object o) throws ZkMarshallingError
            {
                try {
                    return o.toString().getBytes(StandardCharsets.UTF_8);
                }
                catch (Exception e) {
                    log.warn("Exception in serializing " + e);
                }
                return "".getBytes();
            }

            @Override
            public Object deserialize(byte[] bytes) throws ZkMarshallingError
            {
                return null;
            }
        });
    }

    @AfterClass
    public void destroy()
            throws IOException
    {
        kafkaZookeeperServersetMonitor.close();
        zkClient.close();
        zkServer.close();
    }

    @BeforeTest
    public void setUp()
            throws Exception
    {
        log.info("Cleaning up zookeeper");
        zkClient.getChildren("/").stream()
                .filter(child -> !child.equals("zookeeper"))
                .forEach(child -> zkClient.deleteRecursive("/" + child));

        zkClient.unsubscribeAll();

        zkClient.createPersistent(zkPath);
        kafkaZookeeperServersetMonitor = new KafkaZookeeperServersetMonitor(zkServer.getConnectString(), zkPath, 3, 500);
    }

    @Test
    public void testGetServers() throws Exception
    {
        List<HostAddress> servers;
        List<HostAddress> expected;
        assertTrue(kafkaZookeeperServersetMonitor.getServers().isEmpty());

        addServerToZk("nameNode1", "host1", 10001);
        // Sleep for some time so that event can be propagated.
        TimeUnit.MILLISECONDS.sleep(100);
        servers = kafkaZookeeperServersetMonitor.getServers();
        expected = ImmutableList.of(HostAddress.fromParts("host1", 10001));
        assertTrue(servers.containsAll(expected) && expected.containsAll(servers));

        addServerToZk("nameNode2", "host2", 10002);
        // Sleep for some time so that event can be propagated.
        TimeUnit.MILLISECONDS.sleep(100);
        servers = kafkaZookeeperServersetMonitor.getServers();
        expected = ImmutableList.of(HostAddress.fromParts("host1", 10001), HostAddress.fromParts("host2", 10002));
        assertTrue(servers.containsAll(expected) && expected.containsAll(servers));

        // Change value of an existing name node
        addServerToZk("nameNode2", "host2", 10003);
        // Sleep for some time so that event can be propagated.
        TimeUnit.MILLISECONDS.sleep(100);
        servers = kafkaZookeeperServersetMonitor.getServers();
        expected = ImmutableList.of(HostAddress.fromParts("host1", 10001), HostAddress.fromParts("host2", 10003));
        assertTrue(servers.containsAll(expected) && expected.containsAll(servers));

        // Delete an existing name node
        zkClient.delete(getPathForNameNode("nameNode1"));
        // Sleep for some time so that event can be propagated.
        TimeUnit.MILLISECONDS.sleep(100);
        servers = kafkaZookeeperServersetMonitor.getServers();
        expected = ImmutableList.of(HostAddress.fromParts("host2", 10003));
        assertTrue(servers.containsAll(expected) && expected.containsAll(servers), servers.toString());
    }

    private void addServerToZk(String nameNode, String host, int port)
    {
        JSONObject serviceEndpoint = new JSONObject();
        serviceEndpoint.put("host", host);
        serviceEndpoint.put("port", port);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("serviceEndpoint", serviceEndpoint);

        String path = getPathForNameNode(nameNode);

        if (!zkClient.exists(path)) {
            zkClient.createPersistent(path, jsonObject.toJSONString());
        }
        else {
            zkClient.writeData(path, jsonObject.toJSONString());
        }
    }

    private String getPathForNameNode(String nameNode)
    {
        return zkPath + "/" + nameNode;
    }
}
