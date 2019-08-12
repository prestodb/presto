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
import com.facebook.presto.sql.tree.Property;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PrestoQueryRewriterFactory
        implements QueryRewriterFactory
{
    private final SqlParser sqlParser;
    private final List<Property> tablePropertyOverrides;
    private final VerifierConfig verifierConfig;

    @Inject
    public PrestoQueryRewriterFactory(
            SqlParser sqlParser,
            List<Property> tablePropertyOverrides,
            VerifierConfig verifierConfig)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.tablePropertyOverrides = requireNonNull(tablePropertyOverrides, "tablePropertyOverrides is null");
        this.verifierConfig = requireNonNull(verifierConfig, "verifierConfig is null");
    }

    @Override
    public QueryRewriter create(PrestoAction prestoAction)
    {
        return new QueryRewriter(sqlParser, prestoAction, tablePropertyOverrides, verifierConfig);
    }
}
