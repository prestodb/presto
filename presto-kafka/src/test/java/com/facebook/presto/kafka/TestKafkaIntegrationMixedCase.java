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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.kafka.KafkaQueryRunner.createKafkaQueryRunner;
import static com.facebook.presto.kafka.util.EmbeddedKafka.createEmbeddedKafka;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

@Test
public class TestKafkaIntegrationMixedCase
        extends AbstractTestQueryFramework
{
    private EmbeddedKafka embeddedKafka;
    private KafkaQueryRunner kafkaQueryRunner;

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        this.embeddedKafka = createEmbeddedKafka();
        return createKafkaQueryRunner(embeddedKafka, TpchTable.getTables(), ImmutableMap.of("case-sensitive-name-matching", "true"));
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws IOException
    {
        if (embeddedKafka != null) {
            embeddedKafka.close();
        }
    }


}