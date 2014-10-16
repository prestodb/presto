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
package com.facebook.presto.cassandra;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.cassandraunit.model.StrategyModel;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class CassandraTestingUtils
{
    private static final String CLUSTER_NAME = "TestCluster";
    private static final String HOST = "localhost:9160";

    private CassandraTestingUtils()
    {
    }

    public static void createTestData(String keyspaceName, String columnFamilyName)
    {
        List<ColumnFamilyDefinition> columnFamilyDefinitions = createColumnFamilyDefinitions(keyspaceName, columnFamilyName);
        Keyspace keyspace = createOrReplaceKeyspace(keyspaceName, columnFamilyDefinitions);

        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

        long timestamp = System.currentTimeMillis();
        for (int rowNumber = 1; rowNumber < 10; rowNumber++) {
            addRow(columnFamilyName, mutator, timestamp, rowNumber);
        }
        mutator.execute();
    }

    public static Keyspace createOrReplaceKeyspace(String keyspaceName, List<ColumnFamilyDefinition> columnFamilyDefinitions)
    {
        Cluster cluster = getOrCreateCluster();

        KeyspaceDefinition keyspaceDefinition = HFactory.createKeyspaceDefinition(
                keyspaceName,
                StrategyModel.SIMPLE_STRATEGY.value(),
                1,
                columnFamilyDefinitions);

        if (cluster.describeKeyspace(keyspaceName) != null) {
            cluster.dropKeyspace(keyspaceName, true);
        }
        cluster.addKeyspace(keyspaceDefinition, true);
        return HFactory.createKeyspace(keyspaceName, cluster);
    }

    public static Keyspace createOrReplaceKeyspace(String keyspaceName)
    {
        return createOrReplaceKeyspace(keyspaceName, ImmutableList.<ColumnFamilyDefinition>of());
    }

    private static Cluster getOrCreateCluster()
    {
        return HFactory.getOrCreateCluster(CLUSTER_NAME, HOST);
    }

    private static void addRow(String columnFamilyName, Mutator<String> mutator, long timestamp, int rowNumber)
    {
        String key = String.format("key %04d", rowNumber);
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_utf8",
                        "utf8 " + rowNumber,
                        timestamp,
                        StringSerializer.get(),
                        StringSerializer.get()));
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_bytes",
                        Ints.toByteArray(rowNumber),
                        timestamp,
                        StringSerializer.get(),
                        BytesArraySerializer.get()));
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_integer",
                        rowNumber,
                        timestamp,
                        StringSerializer.get(),
                        IntegerSerializer.get()));
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_long",
                        1000L + rowNumber,
                        timestamp,
                        StringSerializer.get(),
                        LongSerializer.get()));
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_uuid",
                        UUID.fromString(String.format("00000000-0000-0000-0000-%012d", rowNumber)),
                        timestamp,
                        StringSerializer.get(),
                        UUIDSerializer.get()));
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_lexical_uuid",
                        UUID.fromString(String.format("00000000-0000-0000-0000-%012d", rowNumber)),
                        timestamp,
                        StringSerializer.get(),
                        UUIDSerializer.get()));
    }

    private static List<ColumnFamilyDefinition> createColumnFamilyDefinitions(String keyspaceName, String columnFamilyName)
    {
        List<ColumnFamilyDefinition> columnFamilyDefinitions = new ArrayList<>();

        ImmutableList.Builder<ColumnDefinition> columnsDefinition = ImmutableList.builder();

        columnsDefinition.add(createColumnDefinition("t_utf8", ComparatorType.UTF8TYPE));
        columnsDefinition.add(createColumnDefinition("t_bytes", ComparatorType.BYTESTYPE));
        columnsDefinition.add(createColumnDefinition("t_integer", ComparatorType.INTEGERTYPE));
        columnsDefinition.add(createColumnDefinition("t_int32", ComparatorType.INT32TYPE));
        columnsDefinition.add(createColumnDefinition("t_long", ComparatorType.LONGTYPE));
        columnsDefinition.add(createColumnDefinition("t_boolean", ComparatorType.BOOLEANTYPE));
        columnsDefinition.add(createColumnDefinition("t_uuid", ComparatorType.UUIDTYPE));
        columnsDefinition.add(createColumnDefinition("t_lexical_uuid", ComparatorType.LEXICALUUIDTYPE));

        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(
                keyspaceName,
                columnFamilyName,
                ComparatorType.UTF8TYPE,
                columnsDefinition.build());

        cfDef.setColumnType(ColumnType.STANDARD);
        cfDef.setComment("presto test table");

        cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getTypeName());

        columnFamilyDefinitions.add(cfDef);

        return columnFamilyDefinitions;
    }

    private static BasicColumnDefinition createColumnDefinition(String columnName, ComparatorType type)
    {
        BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
        columnDefinition.setName(ByteBuffer.wrap(columnName.getBytes(Charsets.UTF_8)));
        columnDefinition.setValidationClass(type.getClassName());
        return columnDefinition;
    }
}
