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

package com.facebook.presto.druid;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;

public class DruidSessionProperties
{
    private static final String COMPUTE_PUSHDOWN = "compute_pushdown_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    public static boolean isComputePushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(COMPUTE_PUSHDOWN, Boolean.class);
    }

    @Inject
    public DruidSessionProperties(DruidConfig druidConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        COMPUTE_PUSHDOWN,
                        "Pushdown query processing to druid",
                        druidConfig.isComputePushdownEnabled(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
