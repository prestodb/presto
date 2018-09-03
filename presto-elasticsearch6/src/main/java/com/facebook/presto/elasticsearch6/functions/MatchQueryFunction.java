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
package com.facebook.presto.elasticsearch6.functions;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * elasticsearch matchQuery(varchar) and matchPhrase(varchar)
 */
public final class MatchQueryFunction
{
    public static final String MATCH_COLUMN_SEP = "__%s@#$%^&*()_+~";

    @ScalarFunction("match_query")
    @Description("es match_query(varchar)")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public Slice matchQuery(
            @SqlType(StandardTypes.VARCHAR) Slice filter)
    {
        if (filter == null) {
            return null;
        }
        String filterStr = filter.toStringUtf8();

        QueryBuilder builder = QueryBuilders.matchQuery(MATCH_COLUMN_SEP, filterStr);
        return Slices.utf8Slice(builder.toString());
    }

    @ScalarFunction("match_phrase")
    @Description("es match_phrase(varchar)")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public Slice matchPhrase(
            @SqlType(StandardTypes.VARCHAR) Slice filter)
    {
        if (filter == null) {
            return null;
        }
        String filterStr = filter.toStringUtf8();

        QueryBuilder builder = QueryBuilders.matchPhraseQuery(MATCH_COLUMN_SEP, filterStr);
        return Slices.utf8Slice(builder.toString());
    }
}
