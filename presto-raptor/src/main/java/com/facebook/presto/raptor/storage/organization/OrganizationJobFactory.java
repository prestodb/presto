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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardManager;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;

import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static java.util.Objects.requireNonNull;

public class OrganizationJobFactory
        implements JobFactory
{
    private final MetadataDao metadataDao;
    private final ShardManager shardManager;
    private final ShardCompactor compactor;

    @Inject
    public OrganizationJobFactory(@ForMetadata IDBI dbi, ShardManager shardManager, ShardCompactor compactor)
    {
        requireNonNull(dbi, "dbi is null");
        this.metadataDao = onDemandDao(dbi, MetadataDao.class);
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.compactor = requireNonNull(compactor, "compactor is null");
    }

    @Override
    public Runnable create(OrganizationSet organizationSet)
    {
        return new OrganizationJob(organizationSet, metadataDao, shardManager, compactor);
    }
}
