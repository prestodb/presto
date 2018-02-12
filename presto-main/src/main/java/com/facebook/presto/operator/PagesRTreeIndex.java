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
package com.facebook.presto.operator;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.Session;
import com.facebook.presto.geospatial.GeometryUtils;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

import static com.facebook.presto.geospatial.GeometryUtils.deserialize;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PagesRTreeIndex
        implements PagesSpatialIndex
{
    private static final long[] EMPTY_ADDRESSES = new long[0];

    private final List<Type> types;
    private final List<Integer> outputChannels;
    private final List<List<Block>> channels;
    private final STRtree rtree;
    private final BiPredicate<OGCGeometry, OGCGeometry> spatialRelationshipTest;
    private final JoinFilterFunction filterFunction;

    public static final class GeometryWithAddress
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GeometryWithAddress.class).instanceSize();
        private final OGCGeometry ogcGeometry;
        private final long address;

        public GeometryWithAddress(OGCGeometry ogcGeometry, long address)
        {
            this.ogcGeometry = ogcGeometry;
            this.address = address;
        }

        public long getEstimatedMemorySizeInBytes()
        {
            return INSTANCE_SIZE + GeometryUtils.getEstimatedMemorySizeInBytes(ogcGeometry);
        }
    }

    public PagesRTreeIndex(
            Session session,
            LongArrayList addresses,
            List<Type> types,
            List<Integer> outputChannels,
            List<List<Block>> channels,
            STRtree rtree,
            BiPredicate<OGCGeometry, OGCGeometry> spatialRelationshipTest,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory)
    {
        this.types = types;
        this.outputChannels = outputChannels;
        this.channels = requireNonNull(channels, "channels is null");
        this.rtree = requireNonNull(rtree, "rtree is null");
        this.spatialRelationshipTest = requireNonNull(spatialRelationshipTest, "spatial relationship is null");
        this.filterFunction = filterFunctionFactory.map(factory -> factory.create(session.toConnectorSession(), addresses, channels)).orElse(null);
    }

    private static Envelope getEnvelope(OGCGeometry ogcGeometry)
    {
        com.esri.core.geometry.Envelope env = new com.esri.core.geometry.Envelope();
        ogcGeometry.getEsriGeometry().queryEnvelope(env);

        return new Envelope(env.getXMin(), env.getXMax(), env.getYMin(), env.getYMax());
    }

    /**
     * Returns an array of addresses from {@link PagesIndex#valueAddresses} corresponding
     * to rows with matching geometries.
     * <p>
     * The caller is responsible for calling {@link #isJoinAddressEligible(long, int, Page)}
     * for each of these addresses to apply additional join filters.
     */
    @Override
    public long[] findJoinAddresses(int probePosition, Page probe, int probeGeometryChannel)
    {
        Block probeGeometryBlock = probe.getBlock(probeGeometryChannel);
        if (probeGeometryBlock.isNull(probePosition)) {
            return EMPTY_ADDRESSES;
        }

        Slice slice = probeGeometryBlock.getSlice(probePosition, 0, probeGeometryBlock.getSliceLength(probePosition));
        OGCGeometry probeGeometry = deserialize(slice);
        verify(probeGeometry != null);
        if (probeGeometry.isEmpty()) {
            return EMPTY_ADDRESSES;
        }

        LongArrayList matchingAddresses = new LongArrayList();

        Envelope envelope = getEnvelope(probeGeometry);
        rtree.query(envelope, item -> {
            GeometryWithAddress geometryWithAddress = (GeometryWithAddress) item;
            if (spatialRelationshipTest.test(geometryWithAddress.ogcGeometry, probeGeometry)) {
                matchingAddresses.add(geometryWithAddress.address);
            }
        });

        return matchingAddresses.toLongArray(null);
    }

    @Override
    public boolean isJoinAddressEligible(long joinAddress, int probePosition, Page probe)
    {
        return filterFunction == null || filterFunction.filter(toIntExact(joinAddress), probePosition, probe);
    }

    @Override
    public void appendTo(long joinAddress, PageBuilder pageBuilder, int outputChannelOffset)
    {
        int blockIndex = decodeSliceIndex(joinAddress);
        int blockPosition = decodePosition(joinAddress);

        for (int outputIndex : outputChannels) {
            Type type = types.get(outputIndex);
            List<Block> channel = channels.get(outputIndex);
            Block block = channel.get(blockIndex);
            type.appendTo(block, blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
    }
}
