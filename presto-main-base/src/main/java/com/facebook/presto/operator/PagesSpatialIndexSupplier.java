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

import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.Session;
import com.facebook.presto.common.array.AdaptiveLongBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.geospatial.rtree.Flatbush;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.PagesRTreeIndex.GeometryWithPosition;
import com.facebook.presto.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.geospatial.GeometryUtils.accelerateGeometry;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserialize;
import static com.facebook.presto.operator.PagesSpatialIndex.EMPTY_INDEX;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.google.common.base.Verify.verify;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PagesSpatialIndexSupplier
        implements Supplier<PagesSpatialIndex>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesSpatialIndexSupplier.class).instanceSize();
    private static final int MEMORY_USAGE_UPDATE_INCREMENT_BYTES = 100 * 1024 * 1024;   // 100 MB

    private final Session session;
    private final AdaptiveLongBigArray addresses;
    private final List<Type> types;
    private final List<Integer> outputChannels;
    private final List<List<Block>> channels;
    private final Optional<Integer> radiusChannel;
    private final SpatialPredicate spatialRelationshipTest;
    private final Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory;
    private final Flatbush<GeometryWithPosition> rtree;
    private final Map<Integer, Rectangle> partitions;
    private final long memorySizeInBytes;

    public PagesSpatialIndexSupplier(
            Session session,
            AdaptiveLongBigArray addresses,
            int positionCount,
            List<Type> types,
            List<Integer> outputChannels,
            List<List<Block>> channels,
            int geometryChannel,
            Optional<Integer> radiusChannel,
            Optional<Integer> partitionChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory,
            Map<Integer, Rectangle> partitions,
            LocalMemoryContext localUserMemoryContext)
    {
        requireNonNull(localUserMemoryContext, "localUserMemoryContext is null");
        this.session = session;
        this.addresses = addresses;
        this.types = types;
        this.outputChannels = outputChannels;
        this.channels = channels;
        this.spatialRelationshipTest = spatialRelationshipTest;
        this.filterFunctionFactory = filterFunctionFactory;
        this.partitions = partitions;

        this.rtree = buildRTree(addresses, positionCount, channels, geometryChannel, radiusChannel, partitionChannel, localUserMemoryContext);
        this.radiusChannel = radiusChannel;
        this.memorySizeInBytes = INSTANCE_SIZE + rtree.getEstimatedSizeInBytes();
    }

    private static Flatbush<GeometryWithPosition> buildRTree(
            AdaptiveLongBigArray addresses,
            int positionCount,
            List<List<Block>> channels,
            int geometryChannel,
            Optional<Integer> radiusChannel,
            Optional<Integer> partitionChannel,
            LocalMemoryContext localUserMemoryContext)
    {
        Operator relateOperator = OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Relate);

        ObjectArrayList<GeometryWithPosition> geometries = new ObjectArrayList<>();

        long recordedSizeInBytes = localUserMemoryContext.getBytes();
        long addedSizeInBytes = 0;

        for (int position = 0; position < positionCount; position++) {
            long pageAddress = addresses.get(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            Block block = channels.get(geometryChannel).get(blockIndex);
            // TODO Consider pushing is-null and is-empty checks into a filter below the join
            if (block.isNull(blockPosition)) {
                continue;
            }

            Slice slice = block.getSlice(blockPosition, 0, block.getSliceLength(blockPosition));
            OGCGeometry ogcGeometry = deserialize(slice);
            verify(ogcGeometry != null);
            if (ogcGeometry.isEmpty()) {
                continue;
            }

            double radius = radiusChannel.map(channel -> DOUBLE.getDouble(channels.get(channel).get(blockIndex), blockPosition)).orElse(0.0);
            if (radius < 0) {
                continue;
            }

            if (!radiusChannel.isPresent()) {
                // If radiusChannel is supplied, this is a distance query, for which our acceleration won't help.
                accelerateGeometry(ogcGeometry, relateOperator);
            }

            int partition = -1;
            if (partitionChannel.isPresent()) {
                Block partitionBlock = channels.get(partitionChannel.get()).get(blockIndex);
                partition = toIntExact(INTEGER.getLong(partitionBlock, blockPosition));
            }

            GeometryWithPosition geometryWithPosition = new GeometryWithPosition(ogcGeometry, partition, position, radius);
            geometries.add(geometryWithPosition);

            addedSizeInBytes += geometryWithPosition.getEstimatedSizeInBytes();

            if (addedSizeInBytes >= MEMORY_USAGE_UPDATE_INCREMENT_BYTES) {
                localUserMemoryContext.setBytes(recordedSizeInBytes + addedSizeInBytes);
                recordedSizeInBytes += addedSizeInBytes;
                addedSizeInBytes = 0;
            }
        }

        return new Flatbush<>(geometries.toArray(new GeometryWithPosition[] {}));
    }

    // doesn't include memory used by channels and addresses which are shared with PagesIndex
    public DataSize getEstimatedSize()
    {
        return new DataSize(memorySizeInBytes, BYTE);
    }

    @Override
    public PagesSpatialIndex get()
    {
        if (rtree.isEmpty()) {
            return EMPTY_INDEX;
        }
        return new PagesRTreeIndex(session, addresses, types, outputChannels, channels, rtree, radiusChannel, spatialRelationshipTest, filterFunctionFactory, partitions);
    }
}
