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
package com.facebook.presto.accumulo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import java.time.Duration;
import static java.lang.String.format;
public class TestingAccumuloServer
{
    private static final int ACCUMULO_MASTER_PORT = 9999;
    private static final int ACCUMULO_TSERVER_PORT = 9997;
    private static final int ZOOKEEPER_PORT = 2181;
    private static final TestingAccumuloServer instance = new TestingAccumuloServer();
    private final FixedHostPortGenericContainer<?> accumuloContainer;
    public static TestingAccumuloServer getInstance()
    {
        return instance;
    }
    private TestingAccumuloServer()
    {
        accumuloContainer = new FixedHostPortGenericContainer<>("prestodev/accumulo:35");
        accumuloContainer.withFixedExposedPort(ACCUMULO_MASTER_PORT, ACCUMULO_MASTER_PORT);
        accumuloContainer.withFixedExposedPort(ACCUMULO_TSERVER_PORT, ACCUMULO_TSERVER_PORT);
        accumuloContainer.withExposedPorts(ZOOKEEPER_PORT);
        accumuloContainer.waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(10)));
        // No need for an explicit stop since this server is a singleton
        // and the container will be stopped by TestContainers on shutdown
        // TODO Change this class to not be a singleton
        //  https://github.com/prestosql/presto/issues/5842
        accumuloContainer.start();
    }
    public String getInstanceName()
    {
        return "default";
    }
    public String getZooKeepers()
    {
        return format("%s:%s", accumuloContainer.getHost(), accumuloContainer.getMappedPort(ZOOKEEPER_PORT));
    }
    public String getUser()
    {
        return "root";
    }
    public String getPassword()
    {
        return "secret";
    }
    public Connector getConnector()
    {
        try {
            ZooKeeperInstance instance = new ZooKeeperInstance(getInstanceName(), getZooKeepers());
            return instance.getConnector(getUser(), new PasswordToken(getPassword()));
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
    }
}