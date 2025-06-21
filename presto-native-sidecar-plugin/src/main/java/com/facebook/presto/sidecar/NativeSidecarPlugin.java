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
package com.facebook.presto.sidecar;

import com.facebook.presto.sidecar.expressions.NativeExpressionOptimizerFactory;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManagerFactory;
import com.facebook.presto.sidecar.nativechecker.NativePlanCheckerProviderFactory;
import com.facebook.presto.sidecar.sessionpropertyproviders.NativeSystemSessionPropertyProviderFactory;
import com.facebook.presto.sidecar.typemanager.NativeTypeManagerFactory;
import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.facebook.presto.spi.plan.PlanCheckerProviderFactory;
import com.facebook.presto.spi.session.WorkerSessionPropertyProviderFactory;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerFactory;
import com.facebook.presto.spi.type.TypeManagerFactory;
import com.google.common.collect.ImmutableList;

public class NativeSidecarPlugin
        implements CoordinatorPlugin
{
    @Override
    public Iterable<WorkerSessionPropertyProviderFactory> getWorkerSessionPropertyProviderFactories()
    {
        return ImmutableList.of(new NativeSystemSessionPropertyProviderFactory());
    }

    @Override
    public Iterable<TypeManagerFactory> getTypeManagerFactories()
    {
        return ImmutableList.of(new NativeTypeManagerFactory());
    }

    @Override
    public Iterable<PlanCheckerProviderFactory> getPlanCheckerProviderFactories()
    {
        return ImmutableList.of(new NativePlanCheckerProviderFactory(getClassLoader()));
    }

    @Override
    public Iterable<FunctionNamespaceManagerFactory> getFunctionNamespaceManagerFactories()
    {
        return ImmutableList.of(new NativeFunctionNamespaceManagerFactory());
    }

    @Override
    public Iterable<ExpressionOptimizerFactory> getExpressionOptimizerFactories()
    {
        return ImmutableList.of(new NativeExpressionOptimizerFactory(getClassLoader()));
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = NativeSidecarPlugin.class.getClassLoader();
        }
        return classLoader;
    }
}
