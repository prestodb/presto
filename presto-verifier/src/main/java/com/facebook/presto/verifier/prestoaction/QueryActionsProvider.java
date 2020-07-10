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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.presto.verifier.annotation.ForControl;
import com.facebook.presto.verifier.annotation.ForHelper;
import com.facebook.presto.verifier.annotation.ForTest;
import com.facebook.presto.verifier.framework.SourceQuery;
import com.facebook.presto.verifier.framework.VerificationContext;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class QueryActionsProvider
        implements QueryActionsFactory
{
    private final PrestoActionFactory helpActionFactory;
    private final QueryActionFactory controlActionFactory;
    private final QueryActionFactory testActionFactory;

    @Inject
    public QueryActionsProvider(
            @ForHelper PrestoActionFactory helpActionFactory,
            @ForControl QueryActionFactory controlActionFactory,
            @ForTest QueryActionFactory testActionFactory)
    {
        this.helpActionFactory = requireNonNull(helpActionFactory, "helpActionFactory is null");
        this.controlActionFactory = requireNonNull(controlActionFactory, "controlActionFactory is null");
        this.testActionFactory = requireNonNull(testActionFactory, "testActionFactory is null");
    }

    public QueryActions create(SourceQuery sourceQuery, VerificationContext verificationContext)
    {
        return new QueryActions(
                helpActionFactory.create(sourceQuery.getControlConfiguration(), verificationContext),
                controlActionFactory.create(sourceQuery.getControlConfiguration(), verificationContext),
                testActionFactory.create(sourceQuery.getTestConfiguration(), verificationContext));
    }
}
