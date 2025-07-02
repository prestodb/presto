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
package com.facebook.presto.spi;

import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.facebook.presto.spi.plan.PlanCheckerProviderFactory;
import com.facebook.presto.spi.session.WorkerSessionPropertyProviderFactory;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerFactory;
import com.facebook.presto.spi.tvf.TVFProviderFactory;
import com.facebook.presto.spi.type.TypeManagerFactory;

import static java.util.Collections.emptyList;

/**
 * The {@link CoordinatorPlugin} interface allows for plugins to provide additional functionality
 * specifically for a coordinator in a Presto cluster. This is a successor to {@link Plugin} for
 * coordinator specific plugins and will eventually replace it.
 */
public interface CoordinatorPlugin
{
    default Iterable<FunctionNamespaceManagerFactory> getFunctionNamespaceManagerFactories()
    {
        return emptyList();
    }

    default Iterable<WorkerSessionPropertyProviderFactory> getWorkerSessionPropertyProviderFactories()
    {
        return emptyList();
    }

    default Iterable<PlanCheckerProviderFactory> getPlanCheckerProviderFactories()
    {
        return emptyList();
    }

    default Iterable<ExpressionOptimizerFactory> getExpressionOptimizerFactories()
    {
        return emptyList();
    }

    default Iterable<TypeManagerFactory> getTypeManagerFactories()
    {
        return emptyList();
    }

    default Iterable<TVFProviderFactory> getTVFProviderFactories()
    {
        return emptyList();
    }
}
