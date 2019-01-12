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
package io.prestosql.metadata;

import java.lang.invoke.MethodHandle;

import static java.util.Objects.requireNonNull;

public class FunctionInvoker
{
    private final MethodHandle methodHandle;

    public FunctionInvoker(MethodHandle methodHandle)
    {
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
    }

    public MethodHandle methodHandle()
    {
        return methodHandle;
    }
}
