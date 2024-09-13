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
package com.facebook.presto.plancheckerproviders;

import com.facebook.presto.plancheckerproviders.nativechecker.NativePlanCheckerProviderFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.plan.PlanCheckerProviderFactory;
import com.google.common.collect.ImmutableList;

public class PlanCheckerProviderPlugin
        implements Plugin
{
    @Override
    public Iterable<PlanCheckerProviderFactory> getPlanCheckerProviderFactories()
    {
        return ImmutableList.of(new NativePlanCheckerProviderFactory(getClassLoader()));
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PlanCheckerProviderPlugin.class.getClassLoader();
        }
        return classLoader;
    }
}
