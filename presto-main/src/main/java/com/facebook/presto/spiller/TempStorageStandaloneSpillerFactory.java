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
package com.facebook.presto.spiller;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.storage.TempStorageManager;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getTempStorageSpillerBufferSize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TempStorageStandaloneSpillerFactory
        implements StandaloneSpillerFactory
{
    private final TempStorageManager tempStorageManager;
    private final PagesSerdeFactory serdeFactory;
    private final String tempStorageName;
    private final SpillerStats spillerStats;

    @Inject
    public TempStorageStandaloneSpillerFactory(
            TempStorageManager tempStorageManager,
            BlockEncodingSerde blockEncodingSerde,
            NodeSpillConfig nodeSpillConfig,
            FeaturesConfig featuresConfig,
            SpillerStats spillerStats)
    {
        this.tempStorageManager = requireNonNull(tempStorageManager, "tempStorageManager is null");
        requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        requireNonNull(nodeSpillConfig, "nodeSpillConfig is null");
        this.serdeFactory = new PagesSerdeFactory(blockEncodingSerde, nodeSpillConfig.isSpillCompressionEnabled());
        this.tempStorageName = requireNonNull(featuresConfig, "featuresConfig is null").getSpillerTempStorage();
        this.spillerStats = requireNonNull(spillerStats, "spillerStats can not be null");
    }

    public TempStorageStandaloneSpiller create(Session session)
    {
        TempDataOperationContext tempDataOperationContext = new TempDataOperationContext(
                session.getSource(),
                session.getQueryId().getId(),
                session.getClientInfo(),
                Optional.of(session.getClientTags()),
                session.getIdentity());
        TempStorage tempStorage = tempStorageManager.getTempStorage(tempStorageName);
        return new TempStorageStandaloneSpiller(
                tempDataOperationContext,
                tempStorage,
                serdeFactory.createPagesSerde(),
                spillerStats,
                toIntExact(getTempStorageSpillerBufferSize(session).toBytes()));
    }
}
