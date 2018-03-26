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

import com.facebook.presto.Session;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManagerFactory;

import java.io.IOException;

public interface SessionSupplier
{
    Session createSession(QueryId queryId, SessionContext context, ResourceGroupId resourceGroupId);

    void addConfigurationManager(SessionPropertyConfigurationManagerFactory sessionConfigFactory);

    void loadConfigurationManager()
            throws IOException;
}
