package com.facebook.presto.cli;

import com.facebook.presto.Main;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.DatabaseMetadata;
import com.facebook.presto.metadata.DatabaseStorageManager;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.command.Command;
import org.skife.jdbi.v2.DBI;

import static com.facebook.presto.operator.ProjectionFunctions.concat;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;

@Command(name = "demo2", description = "Run the demo query 2")
public class DemoQuery2
        extends Main.BaseCommand
{

    private final DatabaseStorageManager storageManager;
    private final DatabaseMetadata metadata;

    public DemoQuery2()
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
                BlockIterable poolName = Utils.getColumn(storageManager, metadata, "hivedba_query_stats", "pool_name");
                BlockIterable cpuMsec = Utils.getColumn(storageManager, metadata, "hivedba_query_stats", "cpu_msec");

                AlignmentOperator alignmentOperator = new AlignmentOperator(ds, poolName, cpuMsec);
//                    Query2FilterAndProjectOperator filterAndProject = new Query2FilterAndProjectOperator(alignmentOperator);
                FilterAndProjectOperator filterAndProject = new FilterAndProjectOperator(alignmentOperator,
                        new Query2Filter(),
                        singleColumn(VARIABLE_BINARY, 1, 0),
                        singleColumn(FIXED_INT_64, 2, 0));
                HashAggregationOperator aggregation = new HashAggregationOperator(filterAndProject,
                        0,
                        ImmutableList.of(CountAggregation.PROVIDER, LongSumAggregation.provider(1, 0)),
                        ImmutableList.of(concat(singleColumn(TupleInfo.Type.VARIABLE_BINARY, 0, 0), singleColumn(TupleInfo.Type.FIXED_INT_64, 1, 0), singleColumn(TupleInfo.Type.FIXED_INT_64, 2, 0))));

                Utils.printResults(start, aggregation);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static class Query2Filter implements FilterFunction
    {
        private static final Slice constant2 = Slices.copiedBuffer("2012-10-11", Charsets.UTF_8);

        @Override
        public boolean filter(BlockCursor[] cursors)
        {
            Slice partition = cursors[0].getSlice(0);
            return partition.equals(constant2);
        }
    }
}
