package com.facebook.presto.cli;

import com.facebook.presto.Main;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.DatabaseMetadata;
import com.facebook.presto.metadata.DatabaseStorageManager;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.command.Command;
import org.skife.jdbi.v2.DBI;

import static com.facebook.presto.operator.ProjectionFunctions.concat;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;

@Command(name = "demo3", description = "Run the demo query 3")
public class DemoQuery3
        extends Main.BaseCommand
{
    private final DatabaseStorageManager storageManager;
    private final DatabaseMetadata metadata;

    public DemoQuery3()
    {
        DBI storageManagerDbi = new DBI("jdbc:h2:file:var/presto-data/db/StorageManager;DB_CLOSE_DELAY=-1");
        DBI metadataDbi = new DBI("jdbc:h2:file:var/presto-data/db/Metadata;DB_CLOSE_DELAY=-1");

        storageManager = new DatabaseStorageManager(storageManagerDbi);
        metadata = new DatabaseMetadata(metadataDbi);

    }

    public void run()
    {
        for (int i = 0; i < 30; i++) {
            try {
                long start = System.nanoTime();

                BlockIterable ds = Utils.getColumn(storageManager, metadata, "hivedba_query_stats", "ds");
                BlockIterable startTime = Utils.getColumn(storageManager, metadata, "hivedba_query_stats", "start_time");
                BlockIterable endTime = Utils.getColumn(storageManager, metadata, "hivedba_query_stats", "end_time");
                BlockIterable cpuMsec = Utils.getColumn(storageManager, metadata, "hivedba_query_stats", "cpu_msec");

                AlignmentOperator alignmentOperator = new AlignmentOperator(ds, startTime, endTime, cpuMsec);
//                    Query3FilterAndProjectOperator filterAndProject = new Query3FilterAndProjectOperator(alignmentOperator);
                FilterAndProjectOperator filterAndProject = new FilterAndProjectOperator(alignmentOperator, new Query3Filter(), new Query3Projection());
                AggregationOperator aggregation = new AggregationOperator(filterAndProject,
                        ImmutableList.of(CountAggregation.PROVIDER, LongSumAggregation.provider(0, 0)),
                        ImmutableList.of(concat(singleColumn(TupleInfo.Type.FIXED_INT_64, 0, 0), singleColumn(TupleInfo.Type.FIXED_INT_64, 1, 0))));

                Utils.printResults(start, aggregation);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static class Query3Filter implements FilterFunction
    {
        private static final Slice constant1 = Slices.copiedBuffer("2012-07-01", Charsets.UTF_8);
        private static final Slice constant2 = Slices.copiedBuffer("2012-10-11", Charsets.UTF_8);

        @Override
        public boolean filter(BlockCursor[] cursors)
        {
            long startTime = cursors[1].getLong(0);
            long endTime = cursors[2].getLong(0);
            Slice partition = cursors[0].getSlice(0);
            return startTime <= 1343350800 && endTime > 1343350800 && partition.compareTo(constant1) >= 0 && partition.compareTo(constant2) <= 0;
        }
    }

    private static class Query3Projection implements ProjectionFunction
    {
        @Override
        public TupleInfo getTupleInfo()
        {
            return TupleInfo.SINGLE_LONG;
        }

        @Override
        public void project(BlockCursor[] cursors, BlockBuilder blockBuilder)
        {
            long startTime = cursors[1].getLong(0);
            long endTime = cursors[2].getLong(0);
            long cpuMsec = cursors[3].getLong(0);
            project(blockBuilder, startTime, endTime, cpuMsec);
        }

        @Override
        public void project(Tuple[] tuples, BlockBuilder blockBuilder)
        {
            long startTime = tuples[1].getLong(0);
            long endTime = tuples[2].getLong(0);
            long cpuMsec = tuples[3].getLong(0);
            project(blockBuilder, startTime, endTime, cpuMsec);
        }

        private BlockBuilder project(BlockBuilder blockBuilder, long startTime, long endTime, long cpuMsec)
        {
            return blockBuilder.append((cpuMsec * (1343350800 - startTime) + endTime + startTime) + (1000 * 86400));
        }
    }
}
