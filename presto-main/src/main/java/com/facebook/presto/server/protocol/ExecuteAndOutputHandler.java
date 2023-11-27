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
package com.facebook.presto.server.protocol;

import com.facebook.presto.Session;
import com.facebook.presto.client.Column;
import com.facebook.presto.common.QueryTypeAndExecutionExtraMessage;
import com.facebook.presto.common.QueryTypeAndExecutionExtraMessage.ExecutionExtraMessage;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.BuiltInQueryAnalysis;
import com.facebook.presto.sql.analyzer.utils.StatementUtils;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public abstract class ExecuteAndOutputHandler<T extends ExecutionExtraMessage>
{
    // intermediary information calculated and set during execution, and maybe used in order to refactor output
    QueryTypeAndExecutionExtraMessage<T> queryExtraMessage;

    public void executeInTask(Statement statement, Optional<BuiltInQueryAnalysis> queryAnalysis, Metadata metadata, Session session)
    {
        String updateType = StatementUtils.getQueryType(statement.getClass()).map(QueryType::toString)
                .orElse("UNKNOWN");
        this.queryExtraMessage = new QueryTypeAndExecutionExtraMessage(updateType, execute(statement, queryAnalysis, metadata, session));
    }

    public OutputResultData handleOutputWithResult(List<Column> columns, Iterable<List<Object>> value,
                                                   Predicate predicate, Consumer<Long> consumer)
    {
        return OutputResultData.EMPTY;
    }

    public OutputColumn getOutputColumns()
    {
        return OutputColumn.EMPTY;
    }

    public OutputResultData handleOutputWithoutResult()
    {
        return OutputResultData.EMPTY;
    }

    abstract T execute(Statement statement, Optional<BuiltInQueryAnalysis> queryAnalysis, Metadata metadata, Session session);

    public static class OutputResultData
    {
        public static final OutputResultData EMPTY = new OutputResultData(false, ImmutableList.of());
        private final boolean needRefactor;
        private final Iterable<List<Object>> value;

        public OutputResultData(boolean needRefactor, Iterable<List<Object>> value)
        {
            this.needRefactor = needRefactor;
            this.value = requireNonNull(value, "value is null");
        }

        public boolean isNeedRefactor()
        {
            return needRefactor;
        }

        public Iterable<List<Object>> getValue()
        {
            return value;
        }
    }

    public static class OutputColumn
    {
        public static final OutputColumn EMPTY = new OutputColumn(false, ImmutableList.of(), ImmutableList.of());
        private final boolean needRefactor;
        private final List<String> columnNames;
        private final List<Type> columnTypes;

        public OutputColumn(boolean needRefactor, List<String> columnNames, List<Type> columnTypes)
        {
            this.needRefactor = needRefactor;
            this.columnNames = requireNonNull(columnNames, "columnNames is null");
            this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
        }

        public boolean isNeedRefactor()
        {
            return needRefactor;
        }

        public List<String> getColumnNames()
        {
            return columnNames;
        }

        public List<Type> getColumnTypes()
        {
            return columnTypes;
        }
    }
}
