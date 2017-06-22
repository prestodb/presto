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
package com.facebook.presto.hdfs;

import com.facebook.presto.hdfs.exception.TableNotFoundException;
import com.facebook.presto.hdfs.fs.FSFactory;
import com.facebook.presto.hdfs.function.Function;
import com.facebook.presto.hdfs.function.Function0;
import com.facebook.presto.hdfs.metaserver.MetaServer;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.hdfs.Types.checkType;
import static java.util.Objects.requireNonNull;
/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSSplitManager
implements ConnectorSplitManager
{
    private final HDFSConnectorId connectorId;
    private final MetaServer metaServer;
    private final FSFactory fsFactory;

    @Inject
    public HDFSSplitManager(
            HDFSConnectorId connectorId,
            MetaServer metaServer,
            FSFactory fsFactory)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.metaServer = requireNonNull(metaServer, "metaServer is null");
        this.fsFactory = requireNonNull(fsFactory, "fsFactory is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        HDFSTableLayoutHandle layout = checkType(layoutHandle, HDFSTableLayoutHandle.class, "layoutHandle");
        Optional<HDFSTableHandle> tableHandle = metaServer.getTableHandle(connectorId.getConnectorId(), layout.getSchemaTableName().getSchemaName(), layout.getSchemaTableName().getTableName());
        if (!tableHandle.isPresent()) {
            throw new TableNotFoundException(layout.getSchemaTableName().toString());
        }
        String tablePath = tableHandle.get().getPath();
        String dbName = tableHandle.get().getSchemaName();
        String tblName = tableHandle.get().getTableName();
        Optional<TupleDomain<ColumnHandle>> predicatesOptional = layout.getPredicates();

        List<ConnectorSplit> splits = new ArrayList<>();
        List<Path> files;
        Function function = new Function0(80);

        if (predicatesOptional.isPresent()) {
            TupleDomain<ColumnHandle> predicates = predicatesOptional.get();
            ColumnHandle fiberCol = layout.getFiberColumn();
            ColumnHandle timeCol = layout.getTimestampColumn();
            Optional<Map<ColumnHandle, Domain>> domains = predicates.getDomains();
            if (!domains.isPresent()) {
                files = fsFactory.listFiles(new Path(tablePath));
            }
            else {
                String fiber = null;
                long fiberId = -1L;
                long timeLow = -1L;
                long timeHigh = -1L;
                if (domains.get().containsKey(fiberCol)) {
                    // parse fiber domains
                    Domain fiberDomain = domains.get().get(fiberCol);
                    ValueSet fiberValueSet = fiberDomain.getValues();
                    if (fiberValueSet instanceof SortedRangeSet) {
                        if (fiberValueSet.isSingleValue()) {
                            Object valueObj = fiberValueSet.getSingleValue();
                            if (valueObj instanceof Integer) {
                                fiber = String.valueOf(valueObj);
                            }
                            if (valueObj instanceof Long) {
                                fiber = String.valueOf(valueObj);
                            }
                            if (valueObj instanceof Slice) {
                                Slice fiberSlice = (Slice) fiberValueSet.getSingleValue();
                                fiber = fiberSlice.toStringUtf8();
                            }
                            // TODO get fiberNum
                            fiberId = function.apply(fiber); // mod fiberNum
//                            switch (fiber.toLowerCase()) {
//                                case "harry":
//                                    fiberId = 0L;
//                                    break;
//                                case "hermione":
//                                    fiberId = 1L;
//                                    break;
//                                case "ron":
//                                    fiberId = 2L;
//                                    break;
//                            }
                        }
                    }
                }
                if (domains.get().containsKey(timeCol)) {
                    // parse time domains
                    Domain timeDomain = domains.get().get(timeCol);
                    ValueSet timeValueSet = timeDomain.getValues();
                    if (timeValueSet instanceof SortedRangeSet) {
                        Range range = ((SortedRangeSet) timeValueSet).getOrderedRanges().get(0);
                        Marker low = range.getLow();
                        Marker high = range.getHigh();
                        if (!low.isLowerUnbounded()) {
                            timeLow = (Long) low.getValue();
                        }
                        if (!high.isUpperUnbounded()) {
                            timeHigh = (Long) high.getValue();
                        }
                    }
                }
                if (fiber == null && timeLow == -1L && timeHigh == -1L) {
                    files = fsFactory.listFiles(new Path(tablePath));
                }
                else {
                    files = metaServer.filterBlocks(
                            dbName,
                            tblName,
                            fiberId == -1L ? Optional.empty() : Optional.of(fiberId),
                            timeLow == -1L ? Optional.empty() : Optional.of(timeLow),
                            timeHigh == -1L ? Optional.empty() : Optional.of(timeHigh))
                            .stream().map(Path::new).collect(Collectors.toList());         // filter file paths with fiber domains and time domains using meta server
                }
            }
        }
        else {
            files = fsFactory.listFiles(new Path(tablePath));
        }

        files.forEach(file -> splits.add(new HDFSSplit(connectorId,
                        tableHandle.get().getSchemaTableName(),
                        file.toString(), 0, -1,
                        fsFactory.getBlockLocations(file, 0, Long.MAX_VALUE))));
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
