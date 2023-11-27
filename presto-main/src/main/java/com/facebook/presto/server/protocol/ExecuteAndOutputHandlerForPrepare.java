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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.client.ResultForPrepare;
import com.facebook.presto.common.QueryTypeAndExecutionExtraMessage.ExtraMessageForPrepare;
import com.facebook.presto.common.type.UnknownType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.BuiltInQueryAnalysis;
import com.facebook.presto.sql.analyzer.utils.InsertValuesExtractor;
import com.facebook.presto.sql.analyzer.utils.ParameterExtractor;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;

public class ExecuteAndOutputHandlerForPrepare
        extends ExecuteAndOutputHandler<ExtraMessageForPrepare>
{
    private static final JsonCodec<ResultForPrepare> QUERY_RESULT_FOR_PREPARE_CODEC = jsonCodec(ResultForPrepare.class);

    @Override
    public ExtraMessageForPrepare execute(Statement statement, Optional<BuiltInQueryAnalysis> analysis, Metadata metadata, Session session)
    {
        boolean isInsertValues = InsertValuesExtractor.getInsertValuesMessage(statement).isInsertValues();
        List<Parameter> parameterLists = ParameterExtractor.getParameters(statement);

        // TODO: return the explicit type for validate in jdbc client
        Builder<String> typesForValidateBuilder = ImmutableList.builder();
        parameterLists.stream()
                .map(parameter -> UnknownType.UNKNOWN)
                .map(type -> type.getTypeSignature().toString())
                .forEach(typeStr -> typesForValidateBuilder.add(typeStr));
        return new ExtraMessageForPrepare(isInsertValues && !parameterLists.isEmpty(), typesForValidateBuilder.build());
    }

    @Override
    public OutputColumn getOutputColumns()
    {
        return new OutputColumn(true, ImmutableList.of("result"), ImmutableList.of(VARBINARY));
    }

    @Override
    public OutputResultData handleOutputWithoutResult()
    {
        ResultForPrepare resultForPrepare = new ResultForPrepare(queryExtraMessage.getExecutionExtraMessage().isInsertValuesWithParameter(),
                queryExtraMessage.getExecutionExtraMessage().getTypesForValidate());
        Iterable<List<Object>> data = ImmutableSet.of(ImmutableList.of(QUERY_RESULT_FOR_PREPARE_CODEC.toBytes(resultForPrepare)));
        return new OutputResultData(true, data);
    }
}
