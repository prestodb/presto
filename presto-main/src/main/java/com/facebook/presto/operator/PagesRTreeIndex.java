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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.array.AdaptiveLongBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.geospatial.GeometryUtils;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.geospatial.rtree.Flatbush;
import com.facebook.presto.geospatial.rtree.HasExtent;
import com.facebook.presto.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.geospatial.GeometryUtils.getExtent;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserialize;
import static com.facebook.presto.operator.JoinUtils.channelsToPages;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PagesRTreeIndex
        implements PagesSpatialIndex
{
    private static final int[] EMPTY_ADDRESSES = new int[0];

    private final AdaptiveLongBigArray addresses;
    private final List<Type> types;
    private final List<Integer> outputChannels;
    private final List<List<Block>> channels;
    private final Flatbush<GeometryWithPosition> rtree;
    private final int radiusChannel;
    private final SpatialPredicate spatialRelationshipTest;
    private final JoinFilterFunction filterFunction;
    private final Map<Integer, Rectangle> partitions;

    public static final class GeometryWithPosition
            implements HasExtent
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GeometryWithPosition.class).instanceSize();

        private final OGCGeometry ogcGeometry;
        private final int partition;
        private final int position;
        private final Rectangle extent;

        public GeometryWithPosition(OGCGeometry ogcGeometry, int partition, int position)
        {
            this(ogcGeometry, partition, position, 0.0f);
        }

        public GeometryWithPosition(OGCGeometry ogcGeometry, int partition, int position, double radius)
        {
            this.ogcGeometry = requireNonNull(ogcGeometry, "ogcGeometry is null");
            this.partition = partition;
            this.position = position;
            this.extent = GeometryUtils.getExtent(ogcGeometry, radius);
        }

        public OGCGeometry getGeometry()
        {
            return ogcGeometry;
        }

        public int getPartition()
        {
            return partition;
        }

        public int getPosition()
        {
            return position;
        }

        @Override
        public Rectangle getExtent()
        {
            return extent;
        }

        @Override
        public long getEstimatedSizeInBytes()
        {
            return INSTANCE_SIZE + ogcGeometry.estimateMemorySize() + extent.getEstimatedSizeInBytes();
        }
    }

    public PagesRTreeIndex(
            Session session,
            AdaptiveLongBigArray addresses,
            List<Type> types,
            List<Integer> outputChannels,
            List<List<Block>> channels,
            Flatbush<GeometryWithPosition> rtree,
            Optional<Integer> radiusChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Map<Integer, Rectangle> partitions)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.types = types;
        this.outputChannels = outputChannels;
        this.channels = requireNonNull(channels, "channels is null");
        this.rtree = requireNonNull(rtree, "rtree is null");
        this.radiusChannel = radiusChannel.orElse(-1);
        this.spatialRelationshipTest = requireNonNull(spatialRelationshipTest, "spatial relationship is null");
        this.filterFunction = filterFunctionFactory.map(factory -> factory.create(session.getSqlFunctionProperties(), addresses, channelsToPages(channels))).orElse(null);
        this.partitions = requireNonNull(partitions, "partitions is null");
    }

    /**
     * Returns an array of addresses from {@link PagesIndex#getValueAddresses()} corresponding
     * to rows with matching geometries.
     * <p>
     * The caller is responsible for calling {@link #isJoinPositionEligible(int, int, Page)}
     * for each of these addresses to apply additional join filters.
     */
    @Override
    public int[] findJoinPositions(int probePosition, Page probe, int probeGeometryChannel, Optional<Integer> probePartitionChannel)
    {
        Block probeGeometryBlock = probe.getBlock(probeGeometryChannel);
        if (probeGeometryBlock.isNull(probePosition)) {
            return EMPTY_ADDRESSES;
        }

        int probePartition = probePartitionChannel.map(channel -> toIntExact(INTEGER.getLong(probe.getBlock(channel), probePosition))).orElse(-1);

        Slice slice = probeGeometryBlock.getSlice(probePosition, 0, probeGeometryBlock.getSliceLength(probePosition));
        OGCGeometry probeGeometry = deserialize(slice);
        verify(probeGeometry != null);
        if (probeGeometry.isEmpty()) {
            return EMPTY_ADDRESSES;
        }

        IntArrayList matchingPositions = new IntArrayList();

        Rectangle queryRectangle = getExtent(probeGeometry);
        boolean probeIsPoint = queryRectangle.isPointlike();
        rtree.findIntersections(queryRectangle, geometryWithPosition -> {
            OGCGeometry buildGeometry = geometryWithPosition.getGeometry();
            Rectangle buildEnvelope = geometryWithPosition.getExtent();
            if (partitions.isEmpty() || (probePartition == geometryWithPosition.getPartition() &&
                    (probeIsPoint || buildEnvelope.isPointlike() || testReferencePoint(queryRectangle, buildEnvelope, probePartition)))) {
                OptionalDouble radius = radiusChannel == -1 ?
                        OptionalDouble.empty() :
                        OptionalDouble.of(getRadius(geometryWithPosition.getPosition()));
                if (spatialRelationshipTest.apply(buildGeometry, probeGeometry, radius)) {
                    matchingPositions.add(geometryWithPosition.getPosition());
                }
            }
        });

        return matchingPositions.toIntArray();
    }

    private boolean testReferencePoint(Rectangle probeEnvelope, Rectangle buildEnvelope, int partition)
    {
        Rectangle intersection = buildEnvelope.intersection(probeEnvelope);
        if (intersection == null) {
            return false;
        }

        Rectangle extent = partitions.get(partition);
        double x = intersection.getXMin();
        double y = intersection.getYMin();
        return x >= extent.getXMin() && x < extent.getXMax() && y >= extent.getYMin() && y < extent.getYMax();
    }

    private double getRadius(int joinPosition)
    {
        long joinAddress = addresses.get(joinPosition);
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
        long joinAddress = addresses.get(joinPosition);
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
