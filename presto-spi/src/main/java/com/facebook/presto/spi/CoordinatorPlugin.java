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
import com.facebook.presto.spi.session.WorkerSessionPropertyProviderFactory;

import static java.util.Collections.emptyList;

/**
 * Introduces a new {@link CoordinatorPlugin} interface for native C++ clusters.
 * Certain elements of the {@link Plugin} SPI are not used in Prestissimo deployments, or may result in inconsistencies.
 * The {@link CoordinatorPlugin} includes only the interfaces relevant to native C++ clusters and
 * is a successor to {@link Plugin} and will eventually replace it.
 * It contains only interfaces that are valid and used in a coordinator.
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
}
