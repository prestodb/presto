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

import com.facebook.airlift.log.Level;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.kafka.KafkaPlugin.DEFAULT_EXTENSION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public abstract class KafkaQueryRunnerBuilder
        extends DistributedQueryRunner.Builder
{
    protected final EmbeddedKafka embeddedKafka;
    protected Map<String, String> extraKafkaProperties = ImmutableMap.of();
    protected Module extension = DEFAULT_EXTENSION;

    protected KafkaQueryRunnerBuilder(EmbeddedKafka embeddedKafka, String defaultSessionSchema)
    {
        super(testSessionBuilder()
                .setCatalog("kafka")
                .setSchema(defaultSessionSchema)
                .build());
        this.embeddedKafka = requireNonNull(embeddedKafka, "embeddedKafka is null");
    }

    public KafkaQueryRunnerBuilder setExtraKafkaProperties(Map<String, String> extraKafkaProperties)
    {
        this.extraKafkaProperties = ImmutableMap.copyOf(requireNonNull(extraKafkaProperties, "extraKafkaProperties is null"));
        return this;
    }

    public KafkaQueryRunnerBuilder setExtension(Module extension)
    {
        this.extension = requireNonNull(extension, "extension is null");
        return this;
    }

    @Override
    public final DistributedQueryRunner build()
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.kafka", Level.WARN);

        DistributedQueryRunner queryRunner = super.build();
        try {
            embeddedKafka.start();
            preInit(queryRunner);
            queryRunner.installPlugin(new KafkaPlugin(extension));
            Map<String, String> kafkaProperties = new HashMap<>(ImmutableMap.copyOf(extraKafkaProperties));
            kafkaProperties.putIfAbsent("kafka.nodes", embeddedKafka.getConnectString());
            kafkaProperties.putIfAbsent("kafka.messages-per-split", "1000");
            queryRunner.createCatalog("kafka", "kafka", kafkaProperties);
            postInit(queryRunner);
            return queryRunner;
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    protected void preInit(DistributedQueryRunner queryRunner)
            throws Exception
    {}

    protected void postInit(DistributedQueryRunner queryRunner) {}
}
