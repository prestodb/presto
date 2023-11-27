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
import com.facebook.presto.common.QueryTypeAndExecutionExtraMessage.ExtraMessageForExecuteBatch;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.BuiltInQueryAnalysis;
import com.facebook.presto.sql.analyzer.utils.InsertValuesExtractor;
import com.facebook.presto.sql.analyzer.utils.InsertValuesExtractor.InsertValuesMessage;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.primitives.Ints.saturatedCast;

public class ExecuteAndOutputHandlerForExecuteBatch
        extends ExecuteAndOutputHandler<ExtraMessageForExecuteBatch>
{
    @Override
    ExtraMessageForExecuteBatch execute(Statement statement, Optional<BuiltInQueryAnalysis> queryAnalysis, Metadata metadata, Session session)
    {
        // get the rows count for each values line in a ``insert value`` statement
        //  for example, ``insert into test values (?, ?), (?, ?)`` contains 2 rows in a single values line
        InsertValuesMessage insertValuesMessage = InsertValuesExtractor.getInsertValuesMessage(statement);
        if (!insertValuesMessage.isInsertValues()) {
            throw new PrestoException(NOT_SUPPORTED, "Execute batch is only supported for insert values");
        }
        return new ExtraMessageForExecuteBatch(insertValuesMessage.getRowsCount());
    }

    @Override
    public OutputResultData handleOutputWithResult(List<Column> columns, Iterable<List<Object>> data,
                                                   Predicate predicate, Consumer<Long> consumer)
    {
        int rowCountForEachBatchLine = queryExtraMessage.getExecutionExtraMessage().getRowsForEachBatchLine();
        long totalCount = 0L;
        Iterator<List<Object>> iterator = data.iterator();
        if (iterator.hasNext()) {
            Number number = (Number) iterator.next().get(0);
            if (number != null) {
                totalCount = number.longValue();
            }
        }

        if (predicate.test(null)) {
            consumer.accept(totalCount);
        }
        Builder<Integer> arrayDataBuilder = ImmutableList.builder();
        for (int i = 0; i < saturatedCast(totalCount) / rowCountForEachBatchLine; i++) {
            arrayDataBuilder.add(rowCountForEachBatchLine);
        }
        Iterable<List<Object>> newResultData = ImmutableSet.of(ImmutableList.of(arrayDataBuilder.build()));
        return new OutputResultData(true, newResultData);
    }

    @Override
    public OutputColumn getOutputColumns()
    {
        return new OutputColumn(true, ImmutableList.of("result"), ImmutableList.of(new ArrayType(INTEGER)));
    }
}
