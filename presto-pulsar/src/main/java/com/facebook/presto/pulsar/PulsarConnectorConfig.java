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
package com.facebook.presto.pulsar;

import io.airlift.configuration.Config;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.validation.constraints.NotNull;

public class PulsarConnectorConfig implements AutoCloseable {

    private String brokerServiceUrl = "http://localhost:8080";
    private String zookeeperUri = "localhost:2181";
    private int entryReadBatchSize = 100;
    private int targetNumSplits = 4;
    private PulsarAdmin pulsarAdmin;

    @NotNull
    public String getBrokerServiceUrl() {
        return brokerServiceUrl;
    }

    @Config("pulsar.broker-service-url")
    public PulsarConnectorConfig setBrokerServiceUrl(String brokerServiceUrl) {
        this.brokerServiceUrl = brokerServiceUrl;
        return this;
    }

    @NotNull
    public String getZookeeperUri() {
        return this.zookeeperUri;
    }

    @Config("pulsar.zookeeper-uri")
    public PulsarConnectorConfig setZookeeperUri(String zookeeperUri) {
        this.zookeeperUri = zookeeperUri;
        return this;
    }

    @NotNull
    public int getEntryReadBatchSize() {
        return this.entryReadBatchSize;
    }

    @Config("pulsar.entry-read-batch-size")
    public PulsarConnectorConfig setEntryReadBatchSize(int batchSize) {
        this.entryReadBatchSize = batchSize;
        return this;
    }

    @NotNull
    public int getTargetNumSplits() {
        return this.targetNumSplits;
    }

    @Config("pulsar.target-num-splits")
    public PulsarConnectorConfig setTargetNumSplits(int targetNumSplits) {
        this.targetNumSplits = targetNumSplits;
        return this;
    }

    @NotNull
    public PulsarAdmin getPulsarAdmin() throws PulsarClientException {
        if (this.pulsarAdmin == null) {
            this.pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(getBrokerServiceUrl()).build();
        }
        return this.pulsarAdmin;
    }

    @Override
    public void close() throws Exception {
        this.pulsarAdmin.close();
    }

    @Override
    public String toString() {
        return "PulsarConnectorConfig{" +
                "brokerServiceUrl='" + brokerServiceUrl + '\'' +
                '}';
    }
}
