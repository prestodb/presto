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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import io.airlift.json.JsonCodec;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class RaptorMetadataFactory
{
    private final String connectorId;
    private final IDBI dbi;
    private final ShardManager shardManager;
    private final JsonCodec<ShardInfo> shardInfoCodec;
    private final JsonCodec<ShardDelta> shardDeltaCodec;

    @Inject
    public RaptorMetadataFactory(
            RaptorConnectorId connectorId,
            @ForMetadata IDBI dbi,
            ShardManager shardManager,
            JsonCodec<ShardInfo> shardInfoCodec,
            JsonCodec<ShardDelta> shardDeltaCodec)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.shardInfoCodec = requireNonNull(shardInfoCodec, "shardInfoCodec is null");
        this.shardDeltaCodec = requireNonNull(shardDeltaCodec, "shardDeltaCodec is null");
    }

    public RaptorMetadata create()
    {
        return new RaptorMetadata(connectorId, dbi, shardManager, shardInfoCodec, shardDeltaCodec);
    }
}
