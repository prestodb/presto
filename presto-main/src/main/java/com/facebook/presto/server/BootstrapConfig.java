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
package com.facebook.presto.server;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

public class BootstrapConfig
{
    private boolean quiet;
    private boolean strictConfig = true;
    private boolean requireExplicitBindings = true;

    public boolean isQuiet()
    {
        return quiet;
    }

    @Config("bootstrap.quiet")
    @ConfigDescription("Whether to require all specified configs to be used")
    public BootstrapConfig setQuiet(boolean quiet)
    {
        this.quiet = quiet;
        return this;
    }

    public boolean isStrictConfig()
    {
        return strictConfig;
    }

    @Config("bootstrap.strict-config")
    @ConfigDescription("Whether to require all specified configs to be used")
    public BootstrapConfig setStrictConfig(boolean strictConfig)
    {
        this.strictConfig = strictConfig;
        return this;
    }

    public boolean isRequireExplicitBindings()
    {
        return requireExplicitBindings;
    }

    @Config("bootstrap.require-explicit-bindings")
    public BootstrapConfig setRequireExplicitBindings(boolean requireExplicitBindings)
    {
        this.requireExplicitBindings = requireExplicitBindings;
        return this;
    }
}
