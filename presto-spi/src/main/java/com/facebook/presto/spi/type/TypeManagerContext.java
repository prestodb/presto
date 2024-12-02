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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.function.FunctionMetadataManager;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TypeManagerContext
{
    private final Optional<FunctionMetadataManager> functionMetadataManager;

    public TypeManagerContext(Optional<FunctionMetadataManager> functionMetadataManager)
    {
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
    }

    public FunctionMetadataManager getFunctionMetadataManager()
    {
        return functionMetadataManager.orElseThrow(() -> new IllegalArgumentException("functionMetadataManager is not present"));
    }
}
