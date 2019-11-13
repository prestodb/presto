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

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.requireNonNull;

public class KafkaZookeeperServerset
        implements KafkaCluster
{
    public KafkaZookeeperServersetMonitor zkMonitor;

    @Inject
    public KafkaZookeeperServerset(KafkaConnectorConfig config)
    {
        String zkServerHostAndPort = requireNonNull(config.getZookeeperUri(), "zkServerHostAndPort is null");
        String zkKafkaBrokerPath = requireNonNull(config.getZookeeperPath(), "zkKafkaBrokerPath is null");
        int zkRetries = requireNonNull(config.getZookeeperMaxRetries(), "zkMaxRetried is null");
        int zkRetrySleepTime = requireNonNull(config.getZookeeperRetrySleepTime(), "zkRetrySleepTime is null");
        this.zkMonitor = new KafkaZookeeperServersetMonitor(zkServerHostAndPort, zkKafkaBrokerPath, zkRetries, zkRetrySleepTime);
    }

    public HostAddress selectRandomServer()
    {
        List<HostAddress> addresses = zkMonitor.getServers();
        Collections.shuffle(addresses);

        return selectRandom(addresses);
    }

    private static <T> T selectRandom(Iterable<T> iterable)
    {
        List<T> list = ImmutableList.copyOf(iterable);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }
}
