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
package io.prestosql.plugin.kudu.procedures;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.kudu.KuduClientSession;
import io.prestosql.plugin.kudu.properties.KuduTableProperties;
import io.prestosql.plugin.kudu.properties.RangePartition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.procedure.Procedure.Argument;

import javax.inject.Inject;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static java.util.Objects.requireNonNull;

public class RangePartitionProcedures
{
    private static final MethodHandle ADD = methodHandle(RangePartitionProcedures.class, "addRangePartition",
            String.class, String.class, String.class);
    private static final MethodHandle DROP = methodHandle(RangePartitionProcedures.class, "dropRangePartition",
            String.class, String.class, String.class);

    private final KuduClientSession clientSession;

    @Inject
    public RangePartitionProcedures(KuduClientSession clientSession)
    {
        this.clientSession = requireNonNull(clientSession);
    }

    public Procedure getAddPartitionProcedure()
    {
        return new Procedure(
                "system",
                "add_range_partition",
                ImmutableList.of(new Argument("schema", VARCHAR), new Argument("table", VARCHAR),
                        new Argument("range_bounds", VARCHAR)),
                ADD.bindTo(this));
    }

    public Procedure getDropPartitionProcedure()
    {
        return new Procedure(
                "system",
                "drop_range_partition",
                ImmutableList.of(new Argument("schema", VARCHAR), new Argument("table", VARCHAR),
                        new Argument("range_bounds", VARCHAR)),
                DROP.bindTo(this));
    }

    public void addRangePartition(String schema, String table, String rangeBounds)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        RangePartition rangePartition = KuduTableProperties.parseRangePartition(rangeBounds);
        clientSession.addRangePartition(schemaTableName, rangePartition);
    }

    public void dropRangePartition(String schema, String table, String rangeBounds)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        RangePartition rangePartition = KuduTableProperties.parseRangePartition(rangeBounds);
        clientSession.dropRangePartition(schemaTableName, rangePartition);
    }
}
