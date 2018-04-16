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
import com.facebook.presto.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.geospatial.serde.GeometrySerde.deserialize;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class PagesRTreeIndex
        implements PagesSpatialIndex
{
    private static final int[] EMPTY_ADDRESSES = new int[0];

    private final LongArrayList addresses;
    private final List<Type> types;
    private final List<Integer> outputChannels;
    private final List<List<Block>> channels;
    private final STRtree rtree;
    private final int radiusChannel;
    private final SpatialPredicate spatialRelationshipTest;
    private final JoinFilterFunction filterFunction;

    public static final class GeometryWithPosition
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GeometryWithPosition.class).instanceSize();
        private final OGCGeometry ogcGeometry;
        private final int position;

        public GeometryWithPosition(OGCGeometry ogcGeometry, int position)
        {
            this.ogcGeometry = ogcGeometry;
            this.position = position;
        }

        public long getEstimatedMemorySizeInBytes()
        {
            return INSTANCE_SIZE + ogcGeometry.estimateMemorySize();
        }
    }

    public PagesRTreeIndex(
            Session session,
            LongArrayList addresses,
            List<Type> types,
            List<Integer> outputChannels,
            List<List<Block>> channels,
            STRtree rtree,
            Optional<Integer> radiusChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.types = types;
        this.outputChannels = outputChannels;
        this.channels = requireNonNull(channels, "channels is null");
        this.rtree = requireNonNull(rtree, "rtree is null");
        this.radiusChannel = radiusChannel.orElse(-1);
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
     * The caller is responsible for calling {@link #isJoinPositionEligible(int, int, Page)}
     * for each of these addresses to apply additional join filters.
     */
    @Override
    public int[] findJoinPositions(int probePosition, Page probe, int probeGeometryChannel)
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

        IntArrayList matchingPositions = new IntArrayList();

        Envelope envelope = getEnvelope(probeGeometry);
        if (radiusChannel == -1) {
            rtree.query(envelope, item -> {
                GeometryWithPosition geometryWithPosition = (GeometryWithPosition) item;
                if (spatialRelationshipTest.apply(geometryWithPosition.ogcGeometry, probeGeometry, OptionalDouble.empty())) {
                    matchingPositions.add(geometryWithPosition.position);
                }
            });
        }
        else {
            rtree.query(envelope, item -> {
                GeometryWithPosition geometryWithPosition = (GeometryWithPosition) item;
                if (spatialRelationshipTest.apply(geometryWithPosition.ogcGeometry, probeGeometry, OptionalDouble.of(getRadius(geometryWithPosition.position)))) {
                    matchingPositions.add(geometryWithPosition.position);
                }
            });
        }

        return matchingPositions.toIntArray(null);
    }

    private double getRadius(int joinPosition)
    {
        long joinAddress = addresses.getLong(joinPosition);
        int blockIndex = decodeSliceIndex(joinAddress);
        int blockPosition = decodePosition(joinAddress);

        return DOUBLE.getDouble(channels.get(radiusChannel).get(blockIndex), blockPosition);
    }

    @Override
    public boolean isJoinPositionEligible(int joinPosition, int probePosition, Page probe)
    {
        return filterFunction == null || filterFunction.filter(joinPosition, probePosition, probe);
    }

    @Override
    public void appendTo(int joinPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long joinAddress = addresses.getLong(joinPosition);
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
