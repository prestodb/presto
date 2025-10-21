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
package com.facebook.presto.spi.procedure;

import com.facebook.presto.spi.ConnectorDistributedProcedureHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorProcedureContext;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static com.facebook.presto.spi.procedure.DistributedProcedure.DistributedProcedureType.TABLE_DATA_REWRITE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TableDataRewriteDistributedProcedure
        extends DistributedProcedure
{
    public static final String SCHEMA = "schema";
    public static final String TABLE_NAME = "table_name";
    public static final String FILTER = "filter";

    private final BeginCallDistributedProcedure beginCallDistributedProcedure;
    private final FinishCallDistributedProcedure finishCallDistributedProcedure;
    private Supplier<ConnectorProcedureContext> contextSupplier;
    private int schemaIndex = -1;
    private int tableNameIndex = -1;
    private OptionalInt filterIndex = OptionalInt.empty();

    public TableDataRewriteDistributedProcedure(String schema, String name,
                                                List<Argument> arguments,
                                                BeginCallDistributedProcedure beginCallDistributedProcedure,
                                                FinishCallDistributedProcedure finishCallDistributedProcedure,
                                                Supplier<ConnectorProcedureContext> contextSupplier)
    {
        super(TABLE_DATA_REWRITE, schema, name, arguments);
        this.beginCallDistributedProcedure = requireNonNull(beginCallDistributedProcedure, "beginCallDistributedProcedure is null");
        this.finishCallDistributedProcedure = requireNonNull(finishCallDistributedProcedure, "finishCallDistributedProcedure is null");
        this.contextSupplier = requireNonNull(contextSupplier, "contextSupplier is null");
        for (int i = 0; i < getArguments().size(); i++) {
            if (getArguments().get(i).getName().equals(SCHEMA)) {
                checkArgument(getArguments().get(i).getType().toString().equalsIgnoreCase("varchar"),
                        format("Argument `%s` must be string type", SCHEMA));
                schemaIndex = i;
            }
            else if (getArguments().get(i).getName().equals(TABLE_NAME)) {
                checkArgument(getArguments().get(i).getType().toString().equalsIgnoreCase("varchar"),
                        format("Argument `%s` must be string type", TABLE_NAME));
                tableNameIndex = i;
            }
            else if (getArguments().get(i).getName().equals(FILTER)) {
                filterIndex = OptionalInt.of(i);
            }
        }
        checkArgument(schemaIndex >= 0 && tableNameIndex >= 0,
                format("A distributed procedure need at least 2 arguments: `%s` and `%s` for the target table", SCHEMA, TABLE_NAME));
    }

    @Override
    public ConnectorDistributedProcedureHandle begin(ConnectorSession session, ConnectorProcedureContext procedureContext, ConnectorTableLayoutHandle tableLayoutHandle, Object[] arguments)
    {
        return this.beginCallDistributedProcedure.begin(session, procedureContext, tableLayoutHandle, arguments);
    }

    @Override
    public void finish(ConnectorProcedureContext procedureContext, ConnectorDistributedProcedureHandle procedureHandle, Collection<Slice> fragments)
    {
        this.finishCallDistributedProcedure.finish(procedureContext, procedureHandle, fragments);
    }

    public ConnectorProcedureContext createContext()
    {
        return contextSupplier.get();
    }

    public String getSchema(Object[] parameters)
    {
        return (String) parameters[schemaIndex];
    }

    public String getTableName(Object[] parameters)
    {
        return (String) parameters[tableNameIndex];
    }

    public String getFilter(Object[] parameters)
    {
        if (filterIndex.isPresent()) {
            return (String) parameters[filterIndex.getAsInt()];
        }
        else {
            return "TRUE";
        }
    }

    @FunctionalInterface
    public interface BeginCallDistributedProcedure
    {
        ConnectorDistributedProcedureHandle begin(ConnectorSession session, ConnectorProcedureContext procedureContext, ConnectorTableLayoutHandle tableLayoutHandle, Object[] arguments);
    }

    @FunctionalInterface
    public interface FinishCallDistributedProcedure
    {
        void finish(ConnectorProcedureContext procedureContext, ConnectorDistributedProcedureHandle procedureHandle, Collection<Slice> fragments);
    }
}
