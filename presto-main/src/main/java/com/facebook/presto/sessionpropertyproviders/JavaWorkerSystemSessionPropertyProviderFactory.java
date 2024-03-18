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
package com.facebook.presto.sessionpropertyproviders;

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.session.SystemSessionPropertyProvider;
import com.facebook.presto.spi.session.SystemSessionPropertyProviderFactory;

import javax.inject.Inject;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class JavaWorkerSystemSessionPropertyProviderFactory
        implements SystemSessionPropertyProviderFactory
{
    private final JavaWorkerSystemSessionPropertyProvider provider;

    @Inject
    public JavaWorkerSystemSessionPropertyProviderFactory(JavaWorkerSystemSessionPropertyProvider provider)
    {
        this.provider = requireNonNull(provider, "provider is null");
    }

    @Override
    public String getName()
    {
        return "java-worker";
    }

    @Override
    public SystemSessionPropertyProvider create(Map<String, String> config, NodeManager nodeManager)
    {
        return provider;
    }
}
