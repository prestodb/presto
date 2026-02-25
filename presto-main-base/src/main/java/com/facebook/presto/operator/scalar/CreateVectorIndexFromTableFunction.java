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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/**
 * Generic 3-arg overload of create_vector_index for the CREATE VECTOR INDEX ... ON TABLE syntax.
 *
 * <p>When the StatementAnalyzer rewrites CREATE VECTOR INDEX into a synthetic CTAS,
 * the column arguments (id_column, embedding_column) become column references resolved
 * against the source table. This requires generic type parameters so the function
 * signature can accept the actual column types (e.g., BIGINT id, ARRAY(REAL) embedding)
 * rather than VARCHAR string literals.
 *
 * <p>The function is never executed — the CreateVectorIndexRewriteOptimizer rewrites
 * the plan before execution. All overloads throw PrestoException if reached.
 *
 * <p>Overloads cover realistic type combinations:
 * <ul>
 *   <li>T1 (id column): long (INTEGER/BIGINT) or Slice (VARCHAR)</li>
 *   <li>T2 (embedding column): Block (ARRAY&lt;REAL&gt; or ARRAY&lt;DOUBLE&gt;)</li>
 * </ul>
 */
@ScalarFunction(value = "create_vector_index", deterministic = false, calledOnNullInput = true)
@Description("Creates a vector index. Used with CREATE VECTOR INDEX ... ON TABLE syntax.")
public final class CreateVectorIndexFromTableFunction
{
    private CreateVectorIndexFromTableFunction() {}

    @TypeParameter("T1")
    @TypeParameter("T2")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice createVectorIndexLong(
            @SqlType("T1") long idColumn,
            @SqlType("T2") Block embeddingColumn,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice properties)
    {
        return throwUnsupported();
    }

    @TypeParameter("T1")
    @TypeParameter("T2")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice createVectorIndexSlice(
            @SqlType("T1") Slice idColumn,
            @SqlType("T2") Block embeddingColumn,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice properties)
    {
        return throwUnsupported();
    }

    private static Slice throwUnsupported()
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR,
                "create_vector_index is only supported via the connector optimizer. " +
                "Ensure CreateVectorIndexRewriteOptimizer is active.");
    }
}
