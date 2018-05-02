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
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.spi.PrestoException;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import static com.facebook.presto.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static java.lang.String.format;

public final class TestingPhoenixServer
        implements Closeable
{
    private static final Logger LOG = Logger.get(TestingPhoenixServer.class);
    private HBaseTestingUtility hbaseTestingUtility;
    private int port;

    private final Configuration conf = HBaseConfiguration.create();

    public TestingPhoenixServer()
    {
        this.conf.setInt(HConstants.MASTER_INFO_PORT, -1);
        this.conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);
        this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
        this.conf.set("hbase.regionserver.wal.codec", "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");
        this.conf.setBoolean("phoenix.schema.isNamespaceMappingEnabled", true);
        this.hbaseTestingUtility = new HBaseTestingUtility(conf);

        try {
            this.port = randomPort();
            this.hbaseTestingUtility.startMiniZKCluster(1, port);

            MiniHBaseCluster hbm = hbaseTestingUtility.startMiniHBaseCluster(1, 4);
            hbm.waitForActiveAndReadyMaster();
            LOG.info("Phoenix server ready: %s", getJdbcUrl());
        }
        catch (Exception e) {
            throw new IllegalStateException("Can't start phoenix server.", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            close();
        }));
    }

    public void close()
    {
        if (hbaseTestingUtility != null) {
            try {
                LOG.info("Shutting down HBase cluster.");
                hbaseTestingUtility.shutdownMiniHBaseCluster();
                hbaseTestingUtility.shutdownMiniZKCluster();
            }
            catch (IOException e) {
                Thread.currentThread().interrupt();
                throw new PrestoException(SERVER_SHUTTING_DOWN, "Failed to shutdown HTU instance", e);
            }
            hbaseTestingUtility = null;
        }
    }

    private static int randomPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress(0));
            return socket.getLocalPort();
        }
    }

    public String getJdbcUrl()
    {
        return format("jdbc:phoenix:localhost:%d:/hbase;phoenix.schema.isNamespaceMappingEnabled=true", port);
    }
}
