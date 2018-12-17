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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.raptorx.RaptorConnector;
import com.facebook.presto.spi.connector.Connector;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class OrganizationJobFactory
        implements JobFactory
{
    private final ChunkCompactor compactor;
    private final RaptorConnector raptorConnector;

    @Inject
    public OrganizationJobFactory(ChunkCompactor compactor, Connector raptorConnector)
    {
        this.compactor = requireNonNull(compactor, "compactor is null");
        this.raptorConnector = (RaptorConnector) requireNonNull(raptorConnector, "transactionManager is null");
    }

    @Override
    public Runnable create(OrganizationSet organizationSet)
    {
        return new OrganizationJob(organizationSet, compactor, raptorConnector);
    }
}
