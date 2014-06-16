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

import com.facebook.presto.kafka.util.CodecSupplier;
import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.kafka.util.TestUtils;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.kafka.util.TestUtils.loadTpchTopic;
import static com.facebook.presto.kafka.util.TestUtils.loadTpchTopicDescription;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

@Test
public class TestKafkaDistributed
        extends AbstractTestDistributedQueries
{
    private static final Logger log = Logger.get("TestQueries");

    private static final String TPCH_NAME = "tpch";
    private static final ConnectorSession TPCH_SESSION = new ConnectorSession("user", "test", "kafka", TPCH_NAME, UTC_KEY, ENGLISH, null, null);

    private EmbeddedKafka embeddedKafka;

    public TestKafkaDistributed()
            throws Exception
    {
        super(createQueryRunner());
        checkState(queryRunner instanceof DistributedQueryRunner, "test must be run using the DistributedQueryRunner");
    }

    private DistributedQueryRunner queryRunner()
    {
        return DistributedQueryRunner.class.cast(queryRunner);
    }

    @BeforeClass
    public void startKafka()
            throws Exception
    {
        embeddedKafka = EmbeddedKafka.createEmbeddedKafka();
        embeddedKafka.start();

        JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec = new CodecSupplier<>(KafkaTopicDescription.class, queryRunner().getCoordinator().getMetadata()).get();

        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> topicDescriptionBuilder = ImmutableMap.builder();

        log.info("Loading data...");
        long startTime = System.nanoTime();

        for (TpchTable<?> table : TpchTable.getTables()) {
            String tableName = table.getTableName();
            SchemaTableName tpchTable = new SchemaTableName(TPCH_NAME, tableName);

            log.info("Loading table %s...", tableName);

            embeddedKafka.createTopics(tpchTable.toString());

            topicDescriptionBuilder.put(loadTpchTopicDescription(topicDescriptionJsonCodec, tpchTable.toString(), tpchTable));
            loadTpchTopic(embeddedKafka, queryRunner().getClient(), tpchTable.toString(), new QualifiedTableName(TPCH_NAME, TpchMetadata.TINY_SCHEMA_NAME, tableName));
        }

        TestUtils.installKafkaPlugin(embeddedKafka, queryRunner, topicDescriptionBuilder.build());

        log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
    }

    @AfterClass(alwaysRun = true)
    @SuppressWarnings({"EmptyTryBlock", "UnusedDeclaration"})
    public void destroy()
            throws IOException
    {
        try (QueryRunner queryRunner = this.queryRunner;
                EmbeddedKafka embeddedKafka = this.embeddedKafka) {
            // use try-with-resources to close everything safely
        }
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(TPCH_SESSION, 4);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(TPCH_NAME, TPCH_NAME);

        return queryRunner;
    }

    //
    // Kafka connector does not support table creation.
    //

    @Override
    public void testCreateSampledTableAsSelectLimit()
            throws Exception
    {
    }

    @Override
    public void testCreateTableAsSelect()
            throws Exception
    {
    }

    @Override
    public void testCreateTableAsSelectGroupBy()
            throws Exception
    {
    }

    @Override
    public void testCreateTableAsSelectJoin()
            throws Exception
    {
    }

    @Override
    public void testCreateTableAsSelectLimit()
            throws Exception
    {
    }

    //
    // Kafka connector does not support views.
    //

    @Override
    public void testView()
            throws Exception
    {
    }

    @Override
    public void testViewMetadata()
            throws Exception
    {
    }

    //
    // Kafka connector does not support sampling.
    //

    @Override
    public void testApproximateQueryCount()
            throws Exception
    {
    }

    @Override
    public void testApproximateQueryCountCustkey()
            throws Exception
    {
    }

    @Override
    public void testApproximateQuerySum()
            throws Exception
    {
    }

    @Override
    public void testApproximateQueryAverage()
            throws Exception
    {
    }

    //
    // Kafka connector does not insert.
    //

    @Override
    public void testInsert()
            throws Exception
    {
    }

    //
    // Kafka connector does not table rename.
    //

    @Override
    public void testRenameTable()
            throws Exception
    {
    }
}
