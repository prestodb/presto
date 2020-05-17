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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.facebook.presto.verifier.resolver.FailureResolverManager;
import com.facebook.presto.verifier.rewrite.QueryRewriter;

import static com.facebook.presto.sql.tree.ShowCreate.Type.VIEW;

public class CreateViewVerification
        extends CreateObjectVerification
{
    public CreateViewVerification(
            PrestoAction prestoAction,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            DeterminismAnalyzer determinismAnalyzer,
            FailureResolverManager failureResolverManager,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig,
            TypeManager typeManager,
            ChecksumValidator checksumValidator)
    {
        super(prestoAction, sourceQuery, queryRewriter, determinismAnalyzer, failureResolverManager, verificationContext, verifierConfig, typeManager, checksumValidator);
    }

    @Override
    protected Statement getShowObjectQuery(QueryBundle bundle)
    {
        return new ShowCreate(VIEW, bundle.getTableName());
    }
}
