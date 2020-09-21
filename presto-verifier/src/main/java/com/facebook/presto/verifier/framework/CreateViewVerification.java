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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.prestoaction.PrestoAction.ResultSetConverter;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;
import com.facebook.presto.verifier.rewrite.QueryRewriter;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.tree.ShowCreate.Type.VIEW;
import static java.util.Objects.requireNonNull;

public class CreateViewVerification
        extends DdlVerification<CreateView>
{
    public static final ResultSetConverter<String> SHOW_CREATE_VIEW_CONVERTER = resultSet -> {
        try {
            return Optional.of(resultSet.getString("Create View"));
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    };
    private static final QualifiedName DUMMY_VIEW_NAME = QualifiedName.of("dummy");

    private final QueryRewriter queryRewriter;

    public CreateViewVerification(
            SqlParser sqlParser,
            QueryActions queryActions,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            SqlExceptionClassifier exceptionClassifier,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig)
    {
        super(sqlParser, queryActions, sourceQuery, exceptionClassifier, verificationContext, verifierConfig, SHOW_CREATE_VIEW_CONVERTER);
        this.queryRewriter = requireNonNull(queryRewriter, "queryRewriter is null");
    }

    @Override
    protected QueryObjectBundle getQueryRewrite(ClusterType clusterType)
    {
        return queryRewriter.rewriteQuery(getSourceQuery().getQuery(clusterType), clusterType);
    }

    @Override
    protected Statement getChecksumQuery(QueryObjectBundle queryBundle)
    {
        return new ShowCreate(VIEW, queryBundle.getObjectName());
    }

    @Override
    protected boolean match(CreateView controlObject, CreateView testObject, QueryObjectBundle control, QueryObjectBundle test)
    {
        controlObject = new CreateView(
                DUMMY_VIEW_NAME,
                controlObject.getQuery(),
                controlObject.isReplace(),
                controlObject.getSecurity());
        testObject = new CreateView(
                DUMMY_VIEW_NAME,
                testObject.getQuery(),
                testObject.isReplace(),
                testObject.getSecurity());
        return Objects.equals(controlObject, testObject);
    }
}
