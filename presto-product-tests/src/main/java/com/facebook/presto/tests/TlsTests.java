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
package com.facebook.presto.tests;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.List;

import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.TestGroups.TLS;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TlsTests
{
    @Inject(optional = true)
    @Named("databases.presto.http_port")
    private Integer httpPort;

    @Inject(optional = true)
    @Named("databases.presto.https_port")
    private Integer httpsPort;

    @Test(groups = {TLS, PROFILE_SPECIFIC_TESTS})
    public void testHttpPortIsClosed()
            throws Exception
    {
        assertThat(httpPort).isNotNull();
        assertThat(httpsPort).isNotNull();

        waitForNodeRefresh();
        List<String> activeNodesUrls = getActiveNodesUrls();
        assertThat(activeNodesUrls).hasSize(3);

        List<String> hosts = activeNodesUrls.stream()
                .map((uri) -> URI.create(uri).getHost())
                .collect(toList());

        for (String host : hosts) {
            assertPortIsOpen(host, httpsPort);
            assertPortIsClosed(host, httpPort);
        }
    }

    private void waitForNodeRefresh()
            throws InterruptedException
    {
        long deadline = System.currentTimeMillis() + MINUTES.toMillis(1);
        while (System.currentTimeMillis() < deadline) {
            if (getActiveNodesUrls().size() == 3) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Worker nodes haven't been discovered in 1 minutes.");
    }

    private List<String> getActiveNodesUrls()
    {
        QueryResult queryResult = onPresto()
                .executeQuery("SELECT http_uri FROM system.runtime.nodes");
        return queryResult.rows()
                .stream()
                .map((row) -> row.get(0).toString())
                .collect(toList());
    }

    private static void assertPortIsClosed(String host, Integer port)
    {
        if (isPortOpen(host, port)) {
            fail(format("Port %d at %s is expected to be closed", port, host));
        }
    }

    private static void assertPortIsOpen(String host, Integer port)
    {
        if (!isPortOpen(host, port)) {
            fail(format("Port %d at %s is expected to be open", port, host));
        }
    }

    private static boolean isPortOpen(String host, Integer port)
    {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(InetAddress.getByName(host), port), 1000);
            return true;
        }
        catch (ConnectException | SocketTimeoutException e) {
            return false;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
