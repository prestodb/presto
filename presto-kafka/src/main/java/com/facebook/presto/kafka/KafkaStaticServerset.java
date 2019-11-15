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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KafkaStaticServerset
{
    private final List<HostAddress> nodes;

    @Inject
    public KafkaStaticServerset(KafkaConnectorConfig config)
    {
        requireNonNull(config.getNodes(), "nodes is null");
        checkArgument(!config.getNodes().isEmpty(), "nodes must specify at least one URI");
        this.nodes = config.getNodes();
    }

    public HostAddress selectRandomServer()
    {
        return selectRandom(this.nodes);
    }

    private static <T> T selectRandom(Iterable<T> iterable)
    {
        List<T> list = ImmutableList.copyOf(iterable);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }
}
