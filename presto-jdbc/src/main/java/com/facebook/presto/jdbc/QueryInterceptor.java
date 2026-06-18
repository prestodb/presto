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
package com.facebook.presto.jdbc;

import java.sql.Statement;
import java.util.Map;
import java.util.Optional;

public interface QueryInterceptor
{
    /**
     * Called once per {@link PrestoConnection} at instantiation.
     *
     * @param properties the session properties supplied in the URI
     */
    default void init(Map<String, String> properties)
    {
    }

    /**
     * Called once to release any resources when {@link PrestoConnection} is closing.
     */
    default void destroy()
    {
    }

    /**
     * Called before the query has been sent to the server. This method is only called on
     * top-level queries i.e. a query executed by a {@link QueryInterceptor} is not intercepted.
     *
     * This method can optionally return a {@link PrestoResultSet}. If a <code>PrestoResultSet</code> is returned from
     * from <code>preProcess</code> then the intercepted query is not executed and that <code>PrestoResultSet</code> is
     * returned instead. If there are multiple  <code>QueryInterceptors</code>, <code>preProcess</code> will be called
     * on each and the last <code>PrestoResultSet</code> is returned.
     *
     * @param sql the SQL string of the query.
     * @param interceptedStatement the Statement being executed.
     * @return optional <code>PrestoResultSet</code> to be returned
     */
    default Optional<PrestoResultSet> preProcess(String sql, Statement interceptedStatement)
    {
        return Optional.empty();
    }

    /**
     * Called after the query has been sent to the server. This method is only called on
     * top-level queries i.e. a query executed by a {@link QueryInterceptor} is not intercepted.
     *
     * This method can optionally return a {@link PrestoResultSet}. If a <code>PrestoResultSet</code> is returned
     * from postProcess then that <code>PrestoResultSet</code>  is returned instead. If there are multiple
     *  <code>QueryInterceptors</code>, <code>postProcess</code> will be called on each and the last <code>PrestoResultSet</code> is returned.
     *
     * @param sql the SQL string of the query.
     * @param interceptedStatement the {@link Statement} being executed.
     * @param interceptedResultSet the intercepted <code>PrestoResultSet</code>
     * @return optional <code>PrestoResultSet</code> to be returned instead of the original <code>PrestoResultSet</code>
     */
    default Optional<PrestoResultSet> postProcess(String sql, Statement interceptedStatement, PrestoResultSet interceptedResultSet)
    {
        return Optional.empty();
    }
}
