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
package io.prestosql.sql;

import io.prestosql.spi.PrestoException;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.REJECT;

public final class SqlFormatterUtil
{
    private SqlFormatterUtil() {}

    public static String getFormattedSql(Statement statement, SqlParser sqlParser, Optional<List<Expression>> parameters)
    {
        String sql = SqlFormatter.formatSql(statement, parameters);

        // verify round-trip
        Statement parsed;
        try {
            ParsingOptions parsingOptions = new ParsingOptions(REJECT /* formatted SQL should be unambiguous */);
            parsed = sqlParser.createStatement(sql, parsingOptions);
        }
        catch (ParsingException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Formatted query does not parse: " + statement);
        }
        if (!statement.equals(parsed)) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Query does not round-trip: " + statement);
        }

        return sql;
    }
}
