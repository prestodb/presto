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

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

public final class Bootstrap
{
    private static volatile BootstrapFunctionBinder functionBinder;

    private Bootstrap()
    {
    }

    public static CallSite bootstrap(MethodHandles.Lookup lookup, String name, MethodType type, long bindingId)
    {
        return functionBinder.bootstrap(name, type, bindingId);
    }

    public static void setFunctionBinder(BootstrapFunctionBinder binder)
    {
        functionBinder = binder;
    }
}
