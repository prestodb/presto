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
package io.prestosql.operator;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.geospatial.Rectangle;
import io.prestosql.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.google.common.base.Verify.verify;
import static io.prestosql.geospatial.serde.GeometrySerde.deserialize;
import static io.prestosql.operator.JoinUtils.channelsToPages;
import static io.prestosql.operator.SyntheticAddress.decodePosition;
import static io.prestosql.operator.SyntheticAddress.decodeSliceIndex;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.Math.toIntExact;
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
    private final Map<Integer, Rectangle> partitions;

    public static final class GeometryWithPosition
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GeometryWithPosition.class).instanceSize();

        private final OGCGeometry ogcGeometry;
        private final int partition;
        private final int position;

        public GeometryWithPosition(OGCGeometry ogcGeometry, int partition, int position)
        {
            this.ogcGeometry = requireNonNull(ogcGeometry, "ogcGeometry is null");
            this.partition = partition;
            this.position = position;
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
        this.filterFunction = filterFunctionFactory.map(factory -> factory.create(session.toConnectorSession(), addresses, channelsToPages(channels))).orElse(null);
        this.partitions = requireNonNull(partitions, "partitions is null");
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

        boolean probeIsPoint = probeGeometry instanceof OGCPoint;

        IntArrayList matchingPositions = new IntArrayList();

        Envelope envelope = getEnvelope(probeGeometry);
        rtree.query(envelope, item -> {
            GeometryWithPosition geometryWithPosition = (GeometryWithPosition) item;
            OGCGeometry buildGeometry = geometryWithPosition.getGeometry();
            if (partitions.isEmpty() || (probePartition == geometryWithPosition.getPartition() && (probeIsPoint || (buildGeometry instanceof OGCPoint) || testReferencePoint(envelope, buildGeometry, probePartition)))) {
                if (radiusChannel == -1) {
                    if (spatialRelationshipTest.apply(buildGeometry, probeGeometry, OptionalDouble.empty())) {
                        matchingPositions.add(geometryWithPosition.getPosition());
                    }
                }
                else {
                    if (spatialRelationshipTest.apply(geometryWithPosition.getGeometry(), probeGeometry, OptionalDouble.of(getRadius(geometryWithPosition.getPosition())))) {
                        matchingPositions.add(geometryWithPosition.getPosition());
                    }
                }
            }
        });

        return matchingPositions.toIntArray(null);
    }

    private boolean testReferencePoint(Envelope probeEnvelope, OGCGeometry buildGeometry, int partition)
    {
        Envelope buildEnvelope = getEnvelope(buildGeometry);
        Envelope intersection = buildEnvelope.intersection(probeEnvelope);
        if (intersection.isNull()) {
            return false;
        }

        Rectangle extent = partitions.get(partition);

        double x = intersection.getMinX();
        double y = intersection.getMinY();
        return x >= extent.getXMin() && x < extent.getXMax() && y >= extent.getYMin() && y < extent.getYMax();
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
