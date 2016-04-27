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

package com.facebook.presto.sql;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;

public final class SqlFormatterUtil
{
    private SqlFormatterUtil() {}

    public static String getFormattedSql(Statement statement, SqlParser sqlParser)
    {
        String sql = SqlFormatter.formatSql(statement);

        // verify round-trip
        Statement parsed;
        try {
            parsed = sqlParser.createStatement(sql);
        }
        catch (ParsingException e) {
            throw new PrestoException(INTERNAL_ERROR, "Formatted query does not parse: " + statement);
        }
        if (!statement.equals(parsed)) {
            throw new PrestoException(INTERNAL_ERROR, "Query does not round-trip: " + statement);
        }

        return sql;
    }
}
