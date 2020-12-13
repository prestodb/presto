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

import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.framework.QueryResult;
import com.facebook.presto.verifier.framework.QueryStage;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;

public interface PrestoAction
        extends QueryAction
{
    @FunctionalInterface
    interface ResultSetConverter<R>
    {
        Optional<R> apply(ResultSet resultSet)
                throws SQLException;

        ResultSetConverter<List<Object>> DEFAULT = resultSet -> {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                row.add(resultSet.getObject(i));
            }
            return Optional.of(unmodifiableList(row));
        };
    }

    QueryActionStats execute(Statement statement, QueryStage queryStage);

    <R> QueryResult<R> execute(Statement statement, QueryStage queryStage, ResultSetConverter<R> converter);
}
