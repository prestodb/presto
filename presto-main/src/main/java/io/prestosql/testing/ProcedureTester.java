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
package io.prestosql.testing;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ProcedureTester
{
    private boolean called;
    private String name;
    private List<Object> arguments;

    public synchronized void reset()
    {
        called = false;
    }

    public void recordCalled(String name, Object... arguments)
    {
        recordCalled(name, asList(arguments));
    }

    public synchronized void recordCalled(String name, List<Object> arguments)
    {
        checkState(!called, "already called");
        called = true;
        this.name = requireNonNull(name, "name is null");
        this.arguments = unmodifiableList(requireNonNull(arguments, "arguments is null"));
    }

    public synchronized boolean wasCalled()
    {
        return called;
    }

    public synchronized String getCalledName()
    {
        checkState(called, "not called");
        return name;
    }

    public synchronized List<Object> getCalledArguments()
    {
        checkState(called, "not called");
        return arguments;
    }
}
