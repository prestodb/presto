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

import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import java.io.IOException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.kafka.util.EmbeddedKafka.createEmbeddedKafka;

@Test
public class TestKafkaDistributed
        extends AbstractTestQueries
{
    private EmbeddedKafka embeddedKafka;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.embeddedKafka = createEmbeddedKafka();
        return createKafkaQueryRunner(embeddedKafka);
    }

    public TestKafkaDistributed(EmbeddedKafka embeddedKafka)
    {
        this.embeddedKafka = embeddedKafka;
    }

    protected static DistributedQueryRunner createKafkaQueryRunner(EmbeddedKafka embeddedKafka)
            throws Exception
    {
        DistributedQueryRunner queryRunner = KafkaQueryRunner.builder(embeddedKafka)
                .setTables(TpchTable.getTables())
                .setExtraTopicDescription(ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .build())
                .setExtraKafkaProperties(ImmutableMap.<String, String>builder()
                        .put("kafka.messages-per-split", "100")
                        .build())
                .build();
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws IOException
    {
        embeddedKafka.close();
    }
}
