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

import io.prestosql.testing.LocalQueryRunner;

import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public abstract class ArrayComparisonBenchmark
{
    public static void main(String... args)
    {
        LocalQueryRunner localQueryRunner = createLocalQueryRunner();
        new ArrayEqualsBenchmark(localQueryRunner).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new ArrayLessThanBenchmark(localQueryRunner).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new ArrayGreaterThanBenchmark(localQueryRunner).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new ArrayNotEqualBenchmark(localQueryRunner).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new ArrayLessThanOrEqualBenchmark(localQueryRunner).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new ArrayGreaterThanOrEqualBenchmark(localQueryRunner).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }

    public static class ArrayEqualsBenchmark
            extends AbstractSqlBenchmark
    {
        public ArrayEqualsBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "array_equals", 5, 50, "SELECT COUNT_IF(ARRAY [orderkey, orderkey + 1, orderkey + 2] = ARRAY[orderkey + 1, orderkey + 2, orderkey + 3]) FROM orders");
        }
    }

    public static class ArrayLessThanBenchmark
            extends AbstractSqlBenchmark
    {
        public ArrayLessThanBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "array_less_than", 5, 50, "SELECT COUNT_IF(ARRAY [quantity, quantity + 5] < ARRAY[quantity, quantity + 5, quantity + 3]) FROM lineitem");
        }
    }

    public static class ArrayGreaterThanBenchmark
            extends AbstractSqlBenchmark
    {
        public ArrayGreaterThanBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "array_greater_than", 5, 50, "SELECT COUNT_IF(ARRAY [quantity, quantity + 6] > ARRAY[quantity, quantity + 5, quantity + 3]) FROM lineitem");
        }
    }

    public static class ArrayNotEqualBenchmark
            extends AbstractSqlBenchmark
    {
        public ArrayNotEqualBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "array_not_equal", 5, 50, "SELECT COUNT_IF(ARRAY [orderkey, orderkey + 1, orderkey + 2] != ARRAY[orderkey + 1, orderkey + 2, orderkey + 3]) FROM orders");
        }
    }

    public static class ArrayLessThanOrEqualBenchmark
            extends AbstractSqlBenchmark
    {
        public ArrayLessThanOrEqualBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "array_less_than_or_equal", 5, 50, "SELECT COUNT_IF(ARRAY [quantity, quantity + 5, quantity + 2] <= ARRAY[quantity, quantity + 5, quantity + 3]) FROM lineitem");
        }
    }

    public static class ArrayGreaterThanOrEqualBenchmark
            extends AbstractSqlBenchmark
    {
        public ArrayGreaterThanOrEqualBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "array_greater_than_or_equal", 5, 50, "SELECT COUNT_IF(ARRAY [quantity, quantity + 6] >= ARRAY[quantity, quantity + 5, quantity + 3]) FROM lineitem");
        }
    }
}
