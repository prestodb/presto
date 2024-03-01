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
package com.facebook.presto.verifier.resolver;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.verifier.prestoaction.PrestoAction;

import static java.util.Objects.requireNonNull;

public class FailureResolverFactoryContext
{
    private final SqlParser sqlParser;
    private final PrestoAction prestoAction;

    public FailureResolverFactoryContext(
            SqlParser sqlParser,
            PrestoAction prestoAction)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
    }

    public SqlParser getSqlParser()
    {
        return sqlParser;
    }

    public PrestoAction getPrestoAction()
    {
        return prestoAction;
    }
}
