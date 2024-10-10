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
package com.facebook.presto.session.sql.expressions;

import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerFactory;
import com.google.common.collect.ImmutableList;

public class NativeExpressionsPlugin
        implements CoordinatorPlugin
{
    @Override
    public Iterable<ExpressionOptimizerFactory> getRowExpressionInterpreterServiceFactories()
    {
        return ImmutableList.of(new NativeExpressionOptimizerFactory(getClassLoader()));
    }

    private static ClassLoader getClassLoader()
    {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), NativeExpressionsPlugin.class.getClassLoader());
    }

    private static ClassLoader firstNonNull(ClassLoader contextClassLoader, ClassLoader classLoader)
    {
        if (contextClassLoader != null) {
            return contextClassLoader;
        }
        return classLoader;
    }
}
