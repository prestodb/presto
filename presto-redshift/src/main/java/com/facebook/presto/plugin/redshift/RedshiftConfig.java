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
package com.facebook.presto.plugin.redshift;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;

public class RedshiftConfig
        extends BaseJdbcConfig
{
    private boolean datasourceManagedViewsEnabled;

    public boolean isDatasourceManagedViewsEnabled()
    {
        return datasourceManagedViewsEnabled;
    }

    @Config("enable-datasource-managed-views")
    @ConfigDescription("Enable datasource-managed view handling. When disabled, Presto retrieves and analyzes Redshift view definitions. " +
            "When enabled, view resolution is delegated to Redshift and Presto does not analyze underlying view definitions.")
    public RedshiftConfig setDatasourceManagedViewsEnabled(boolean datasourceManagedViewsEnabled)
    {
        this.datasourceManagedViewsEnabled = datasourceManagedViewsEnabled;
        return this;
    }
}
