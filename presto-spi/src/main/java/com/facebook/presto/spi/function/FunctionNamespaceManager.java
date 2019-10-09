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
package com.facebook.presto.spi.function;

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.api.Experimental;
import com.facebook.presto.spi.relation.FullyQualifiedName;

import java.util.Collection;
import java.util.List;

@Experimental
public interface FunctionNamespaceManager<F extends SqlFunction>
{
    void createFunction(F function, boolean replace);

    List<F> listFunctions();

    /**
     * Ideally function namespaces should support transactions like connectors do, and getCandidates should be transaction-aware.
     * queryId serves as a transaction ID before proper support for transaction is introduced.
     * TODO Support transaction in function namespaces
     */
    Collection<F> getFunctions(QueryId queryId, FullyQualifiedName functionName);

    /**
     * If a SqlFunction for a given signature is returned from {@link #getFunctions(QueryId, FullyQualifiedName)}
     * for a given queryId, getFunctionHandle with the same queryId should return a valid FunctionHandle, even if the function
     * is deleted. Multiple calls of this function with the same parameters should return the same FunctionHandle.
     * queryId serves as a transaction ID before proper support for transaction is introduced.
     * TODO Support transaction in function namespaces
     * @return FunctionHandle or null if the namespace manager does not manage any function with the given signature.
     */
    FunctionHandle getFunctionHandle(QueryId queryId, Signature signature);

    FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle);
}
