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
package io.prestosql.benchmark;

import io.prestosql.operator.Driver;
import io.prestosql.operator.TaskContext;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.NullOutputOperator.NullOutputFactory;
import org.intellij.lang.annotations.Language;

import java.util.List;

public abstract class AbstractSqlBenchmark
        extends AbstractOperatorBenchmark
{
    @Language("SQL")
    private final String query;

    protected AbstractSqlBenchmark(
            LocalQueryRunner localQueryRunner,
            String benchmarkName,
            int warmupIterations,
            int measuredIterations,
            @Language("SQL") String query)
    {
        super(localQueryRunner, benchmarkName, warmupIterations, measuredIterations);
        this.query = query;
    }

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        return localQueryRunner.createDrivers(session, query, new NullOutputFactory(), taskContext);
    }
}
