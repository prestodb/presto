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
package com.facebook.presto.verifier.source;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.verifier.annotation.ForHelper;
import com.facebook.presto.verifier.framework.QueryConfiguration;
import com.facebook.presto.verifier.framework.SourceQuery;
import com.facebook.presto.verifier.framework.VerificationContext;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.prestoaction.PrestoAction.ResultSetConverter;
import com.facebook.presto.verifier.prestoaction.PrestoActionFactory;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.verifier.framework.QueryStage.SOURCE;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static java.util.Objects.requireNonNull;

public class PrestoQuerySourceQuerySupplier
        implements SourceQuerySupplier
{
    public static final String PRESTO_QUERY_SOURCE_QUERY_SUPPLIER = "presto-query";
    private static final ResultSetConverter<SourceQuery> SOURCE_QUERY_CONVERTER = resultSet ->
            Optional.of(new SourceQuery(
                    resultSet.getString("suite"),
                    resultSet.getString("name"),
                    resultSet.getString("control_query"),
                    resultSet.getString("test_query"),
                    new QueryConfiguration(
                            resultSet.getString("control_catalog"),
                            resultSet.getString("control_schema"),
                            Optional.ofNullable(resultSet.getString("control_username")),
                            Optional.ofNullable(resultSet.getString("control_password")),
                            Optional.ofNullable(resultSet.getString("control_session_properties"))
                                    .map(StringToStringMapColumnMapper.CODEC::fromJson)),
                    new QueryConfiguration(
                            resultSet.getString("test_catalog"),
                            resultSet.getString("test_schema"),
                            Optional.ofNullable(resultSet.getString("test_username")),
                            Optional.ofNullable(resultSet.getString("test_password")),
                            Optional.ofNullable(resultSet.getString("test_session_properties"))
                                    .map(StringToStringMapColumnMapper.CODEC::fromJson))));

    private final PrestoAction helperAction;
    private final SqlParser sqlParser;
    private final String query;

    @Inject
    public PrestoQuerySourceQuerySupplier(
            @ForHelper PrestoActionFactory helperActionFactory,
            SqlParser sqlParser,
            PrestoQuerySourceQueryConfig config)
    {
        this.helperAction = helperActionFactory.create(
                new QueryConfiguration(config.getCatalog(), config.getSchema(), config.getUsername(), config.getPassword(), Optional.empty()),
                VerificationContext.create("", ""));
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.query = requireNonNull(config.getQuery(), "query is null");
    }

    @Override
    public List<SourceQuery> get()
    {
        return helperAction.execute(sqlParser.createStatement(query, PARSING_OPTIONS), SOURCE, SOURCE_QUERY_CONVERTER).getResults();
    }
}
