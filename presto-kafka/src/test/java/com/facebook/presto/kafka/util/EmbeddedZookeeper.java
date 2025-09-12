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
package com.facebook.presto.kafka.util;

import com.google.common.io.Files;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.server.testing.TestingPrestoServer.getAvailablePort;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

public class EmbeddedZookeeper
        implements Closeable
{
    private final int port;
    private final File zkDataDir;
    private final ZooKeeperServer zkServer;
    private final ServerCnxnFactory cnxnFactory;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public EmbeddedZookeeper()
            throws IOException
    {
        this(getAvailablePort());
    }

    public EmbeddedZookeeper(int port)
            throws IOException
    {
        this.port = port;
        zkDataDir = Files.createTempDir();
        zkServer = new ZooKeeperServer();

        FileTxnSnapLog ftxn = new FileTxnSnapLog(zkDataDir, zkDataDir);
        zkServer.setTxnLogFactory(ftxn);

        cnxnFactory = NIOServerCnxnFactory.createFactory(new InetSocketAddress(port), 0);
    }

    public void start()
            throws InterruptedException, IOException
    {
        if (!started.getAndSet(true)) {
            cnxnFactory.startup(zkServer);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (started.get() && !stopped.getAndSet(true)) {
            cnxnFactory.shutdown();
            try {
                cnxnFactory.join();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (zkServer.isRunning()) {
                zkServer.shutdown();
            }

            deleteRecursively(zkDataDir.toPath(), ALLOW_INSECURE);
        }
    }

    public String getConnectString()
    {
        return "127.0.0.1:" + Integer.toString(port);
    }

    public int getPort()
    {
        return port;
    }
}
