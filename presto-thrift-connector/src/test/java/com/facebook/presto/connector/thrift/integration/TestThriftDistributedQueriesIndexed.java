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
package com.facebook.presto.connector.thrift.integration;

import com.facebook.presto.tests.AbstractTestIndexedQueries;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import org.testng.annotations.AfterClass;

import static com.facebook.presto.connector.thrift.integration.ThriftQueryRunner.createThriftQueryRunner;
import static io.airlift.log.Level.OFF;

public class TestThriftDistributedQueriesIndexed
        extends AbstractTestIndexedQueries
{
    private static final String NIFTY_LOGGER_NAME = "com.facebook.nifty.core.NiftyExceptionLogger";
    private final Level previousLogLevel;

    public TestThriftDistributedQueriesIndexed()
            throws Exception
    {
        super(() -> createThriftQueryRunner(2, 2, true));
        // During index join thrift server will log a lot of messages like "ClosedChannelException" and "java.io.IOException: Connection reset by peer".
        // Both of them mean that client closed the connection without reading all the data and are expected in case of index join.
        // One of the cases it happens is when an index snapshot builder gets out of memory and gets closed. Then index loader will retry or choose another strategy.
        // Silencing ALL nifty exceptions to not pollute the log.
        Logging logging = Logging.initialize();
        previousLogLevel = logging.getLevel(NIFTY_LOGGER_NAME);
        logging.setLevel(NIFTY_LOGGER_NAME, OFF);
    }

    @Override
    public void testExampleSystemTable()
            throws Exception
    {
        // system tables are not supported
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void close()
            throws Exception
    {
        // restore log level to the previous value
        Logging.initialize().setLevel(NIFTY_LOGGER_NAME, previousLogLevel);
        super.close();
    }
}
