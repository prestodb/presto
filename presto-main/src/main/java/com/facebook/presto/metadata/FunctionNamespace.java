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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.QualifiedName;

import java.util.List;

public interface FunctionNamespace
{
    void addFunctions(List<? extends SqlFunction> functions);

    List<SqlFunction> listFunctions();

    FunctionHandle resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes);

    FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle);
}
