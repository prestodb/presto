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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.facebook.presto.raptorx.util.DatabaseUtil.verifyMetadata;

public class BucketManager
{
    private final DistributionDao dao;

    @Inject
    public BucketManager(Database database, TypeManager typeManager)
    {
        Jdbi dbi = createJdbi(database.getMasterConnection());
        dbi.registerRowMapper(new DistributionInfo.Mapper(typeManager));
        this.dao = dbi.onDemand(DistributionDao.class);
    }

    public List<Long> getBucketNodes(long distributionId)
    {
        Map<Integer, Long> bucketNodes = new TreeMap<>();
        for (BucketNode entry : dao.getBucketNodes(distributionId)) {
            bucketNodes.put(entry.getBucketNumber(), entry.getNodeId());
        }

        int bucketCount = dao.getDistributionInfo(distributionId).getBucketCount();
        verifyMetadata(bucketNodes.size() == bucketCount, "Wrong node count for distribution ID: %s", distributionId);

        return ImmutableList.copyOf(bucketNodes.values());
    }
}
