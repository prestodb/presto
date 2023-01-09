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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.Type;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SqlFunctionExecutor
{
    FunctionImplementationType getImplementationType();

    void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde);

    CompletableFuture<SqlFunctionResult> executeFunction(
            String source,
            RemoteScalarFunctionImplementation functionImplementation,
            Page input,
            List<Integer> channels,
            List<Type> argumentTypes,
            Type returnType);
}
