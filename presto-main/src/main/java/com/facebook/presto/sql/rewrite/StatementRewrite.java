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

package com.facebook.presto.sql.rewrite;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class StatementRewrite
{
    private static final List<Rewrite> REWRITES = ImmutableList.of(
            new ShowQueriesRewrite());

    private StatementRewrite() {}

    public static Statement rewrite(Session session, Metadata metadata, SqlParser parser, Statement node)
    {
        for (Rewrite rewrite : REWRITES) {
            node = requireNonNull(rewrite.rewrite(session, metadata, parser, node), "Statement rewrite returned null");
        }
        return node;
    }

    interface Rewrite
    {
        Statement rewrite(Session session, Metadata metadata, SqlParser parser, Statement node);
    }
}
