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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.parser.ParsingException;

import java.util.Map;
import java.util.Optional;

public class NativeQueryPreparer
        implements QueryPreparer
{
    @Override
    public PreparedQuery prepareQuery(AnalyzerOptions analyzerOptions, String query, Map<String, String> preparedStatements, WarningCollector warningCollector)
            throws ParsingException, PrestoException, SemanticException
    {
        return new NativePreparedQuery(Optional.of(query), Optional.of(query));
    }
}
