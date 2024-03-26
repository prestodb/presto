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
package com.facebook.presto.operator;

import com.facebook.presto.testing.docker.DockerContainer;
import com.google.common.collect.ImmutableList;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AlluxioLocalCluster
        implements Closeable
{
    private final DockerContainer masterDockerContainer;

    private final DockerContainer workerDockerContainer;

    public AlluxioLocalCluster()
    {
        Map<String, String> masterEnv = new HashMap<>();
        masterEnv.put("ALLUXIO_JAVA_OPTS", "-Dalluxio.master.hostname=alluxio-master");
        masterDockerContainer = new DockerContainer("alluxio/alluxio:2.8.1",
                ImmutableList.of(19999, 19998),
                masterEnv,
                AlluxioLocalCluster::masterHealthCheck,
                Optional.of("master"),
                Optional.empty(),
                Optional.empty(),
                true);
        masterDockerContainer.executeCmd("root", new String[] {"/bin/sh", "-c", "echo '127.0.0.1 alluxio-master' >> /etc/hosts"});
        masterDockerContainer.executeCmd("alluxio", new String[] {"/bin/sh", "-c", "/opt/alluxio/bin/alluxio fs chmod 777 /"});

        String masterIp = masterDockerContainer.getHostIp();
        String masterHostname = masterDockerContainer.getHostname();
        Map<String, String> workerEnv = new HashMap<>();
        workerEnv.put("ALLUXIO_JAVA_OPTS", "-Dalluxio.worker.tieredstore.level0.alias=HDD " +
                "-Dalluxio.worker.tieredstore.level0.dirs.path=/mnt -Dalluxio.master.hostname=alluxio-master -Dalluxio.master.port=19998");
        workerDockerContainer = new DockerContainer("alluxio/alluxio:2.8.1",
                ImmutableList.of(29999, 30000),
                workerEnv,
                AlluxioLocalCluster::workerHealthCheck,
                Optional.of("worker"),
                Optional.of("alluxio-master:" + masterIp),
                Optional.of("localhost"),
                true);
        workerDockerContainer.executeCmd("root", new String[] {"/bin/sh", "-c", "echo '" + masterIp + " " + masterHostname + "' >> /etc/hosts"});

        String workerHostname = workerDockerContainer.getHostname();
        String workerHostIp = workerDockerContainer.getHostIp();
        masterDockerContainer.executeCmd("root", new String[] {"/bin/sh", "-c", "echo '" + workerHostIp + " " + workerHostname + "' >> /etc/hosts"});

        //Security.setProperty(workerHostname, "127.0.0.1");
    }

    private static void masterHealthCheck(DockerContainer.HostPortProvider hostPortProvider)
            throws URISyntaxException, IOException
    {
        int masterWebPort = hostPortProvider.getHostPort(19999);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        URIBuilder uriBuilder = new URIBuilder("http://127.0.0.1:" + masterWebPort);
        HttpGet httpGet = new HttpGet(uriBuilder.build());
        CloseableHttpResponse response = httpClient.execute(httpGet);
        StatusLine statusLine = response.getStatusLine();
        if (statusLine.getStatusCode() != 200) {
            throw new RuntimeException("Master is not started");
        }
    }

    private static void workerHealthCheck(DockerContainer.HostPortProvider hostPortProvider)
            throws URISyntaxException, IOException
    {
        int workerWebPort = hostPortProvider.getHostPort(30000);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        URIBuilder uriBuilder = new URIBuilder("http://127.0.0.1:" + workerWebPort);
        HttpGet httpGet = new HttpGet(uriBuilder.build());
        CloseableHttpResponse response = httpClient.execute(httpGet);
        StatusLine statusLine = response.getStatusLine();
        if (statusLine.getStatusCode() != 200) {
            throw new RuntimeException("Worker is not started");
        }
    }

    public int getMasterWebPort()
    {
        return masterDockerContainer.getHostPort(19999);
    }

    public int getMasterRpcPort()
    {
        return masterDockerContainer.getHostPort(19998);
    }

    public int getWorkerWebPort()
    {
        return workerDockerContainer.getHostPort(30000);
    }

    public int getWorkerRpcPort()
    {
        return workerDockerContainer.getHostPort(29999);
    }

    @Override
    public void close()
            throws IOException
    {
        workerDockerContainer.close();
        masterDockerContainer.close();
    }
}
