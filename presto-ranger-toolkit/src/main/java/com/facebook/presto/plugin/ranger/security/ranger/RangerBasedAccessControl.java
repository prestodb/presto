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
package com.facebook.presto.plugin.ranger.security.ranger;

import com.facebook.presto.spi.connector.ConnectorAccessControl;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.IOException;

public class RangerBasedAccessControl
        implements ConnectorAccessControl
{
    private static final Logger log = Logger.get(RangerBasedAccessControl.class);

    @Inject
    public RangerBasedAccessControl(RangerBasedAccessControlConfig config)
            throws IOException
    {
        RangerConnectorAccessFactory rangerConnectorAccessFactory = new RangerConnectorAccessFactory();

        rangerConnectorAccessFactory.create(config);
    }
}
