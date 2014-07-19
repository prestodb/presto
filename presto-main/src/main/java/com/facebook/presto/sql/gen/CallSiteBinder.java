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
package com.facebook.presto.sql.gen;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

public final class CallSiteBinder
{
    private int nextId;

    private final Map<Long, CallSite> bindings = new HashMap<>();

    public Binding bind(MethodHandle method)
    {
        long bindingId = nextId++;
        ConstantCallSite callSite = new ConstantCallSite(method);
        bindings.put(bindingId, callSite);
        return new Binding(bindingId, callSite.type());
    }

    public Binding bind(Object constant, Class<?> type)
    {
        return bind(MethodHandles.constant(type, constant));
    }

    public Map<Long, CallSite> getBindings()
    {
        return ImmutableMap.copyOf(bindings);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("nextId", nextId)
                .add("bindings", bindings)
                .toString();
    }
}
