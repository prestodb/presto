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
package com.facebook.presto.verifier.rewrite;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.verifier.annotation.ForControl;
import com.facebook.presto.verifier.annotation.ForTest;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static java.util.Objects.requireNonNull;

public class VerificationQueryRewriterFactory
        implements QueryRewriterFactory
{
    private final SqlParser sqlParser;
    private final List<Property> tablePropertyOverrides;
    private final QueryRewriteConfig controlConfig;
    private final QueryRewriteConfig testConfig;

    @Inject
    public VerificationQueryRewriterFactory(
            SqlParser sqlParser,
            List<Property> tablePropertyOverrides,
            @ForControl QueryRewriteConfig controlConfig,
            @ForTest QueryRewriteConfig testConfig)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.tablePropertyOverrides = requireNonNull(tablePropertyOverrides, "tablePropertyOverrides is null");
        this.controlConfig = requireNonNull(controlConfig, "controlConfig is null");
        this.testConfig = requireNonNull(testConfig, "testConfig is null");
    }

    @Override
    public QueryRewriter create(PrestoAction prestoAction)
    {
        return new QueryRewriter(
                sqlParser,
                prestoAction,
                tablePropertyOverrides,
                ImmutableMap.of(
                        CONTROL, controlConfig.getTablePrefix(),
                        TEST, testConfig.getTablePrefix()));
    }
}
