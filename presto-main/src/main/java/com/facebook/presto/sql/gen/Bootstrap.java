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

import com.google.common.base.Throwables;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class Bootstrap
{
    public static final Method BOOTSTRAP_METHOD;

    static {
        try {
            BOOTSTRAP_METHOD = Bootstrap.class.getMethod("bootstrap", MethodHandles.Lookup.class, String.class, MethodType.class, long.class);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    private Bootstrap()
    {
    }

    public static CallSite bootstrap(MethodHandles.Lookup callerLookup, String name, MethodType type, long bindingId)
    {
        try {
            MethodHandle handle = callerLookup.findStaticGetter(callerLookup.lookupClass(), "callSites", Map.class);
            Map<Long, MethodHandle> bindings = (Map<Long, MethodHandle>) handle.invokeExact();
            checkNotNull(bindings, "'callSites' field in %s is null", callerLookup.lookupClass().getName());

            MethodHandle method = bindings.get(bindingId);
            checkArgument(method != null, "Binding %s for function %s%s not found", bindingId, name, type.parameterList());
            return new ConstantCallSite(method);
        }
        catch (Throwable e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
        }
    }
}
