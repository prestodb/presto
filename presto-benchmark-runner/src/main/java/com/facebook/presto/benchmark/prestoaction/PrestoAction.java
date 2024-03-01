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
package com.facebook.presto.benchmark.prestoaction;

import com.facebook.presto.benchmark.framework.QueryResult;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public interface PrestoAction
{
    @FunctionalInterface
    interface ResultSetConverter<R>
    {
        R apply(ResultSet resultSet)
                throws SQLException;

        ResultSetConverter<List<Object>> DEFAULT = resultSet -> {
            ImmutableList.Builder<Object> row = ImmutableList.builder();
            for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                row.add(resultSet.getObject(i));
            }
            return row.build();
        };
    }

    QueryStats execute(Statement statement);

    <R> QueryResult<R> execute(Statement statement, ResultSetConverter<R> converter);
}
