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
package com.facebook.presto.spi.relation;

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.relation.Predicate;
import com.facebook.presto.spi.api.Experimental;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;

import java.util.Map;
import java.util.function.Supplier;

/**
 * An experimental API to allow Hive connector to implement smart
 * filtering on the encoded data to avoid materializing data unnecessarily.
 */
@Experimental
public interface PredicateCompiler
{
    /**
     * Predicate expression may not contain any variable references, only input references.
     */
    Supplier<Predicate> compilePredicate(SqlFunctionProperties sqlFunctionProperties, Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions, RowExpression predicate);
}
