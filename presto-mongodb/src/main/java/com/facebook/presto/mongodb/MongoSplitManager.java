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
package com.facebook.presto.mongodb;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.*;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoCursor;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.bson.Document;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import javax.inject.Inject;
import java.sql.Timestamp;
import java.util.*;
import java.util.LinkedHashMap;

import static com.facebook.presto.mongodb.ObjectIdType.OBJECT_ID;
import static com.facebook.presto.spi.HostAddress.fromParts;
import static com.facebook.presto.spi.predicate.Range.equal;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.stream.Collectors.toList;

import static com.facebook.presto.spi.HostAddress.fromParts;
import static java.util.stream.Collectors.toList;

public class MongoSplitManager
        implements ConnectorSplitManager
{
    private final List<HostAddress> addresses;
    private final MongoSession mongoSession;
    private static final Logger LOG = Logger.get(MongoSplitManager.class);

    @Inject
    public MongoSplitManager(MongoClientConfig config,MongoSession mongoSession)
    {
        this.addresses = config.getSeeds().stream()
                .map(s -> fromParts(s.getHost(), s.getPort()))
                .collect(toList());
        this.mongoSession=mongoSession;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        ImmutableList.Builder<MongoSplit> mongoSplitsList = ImmutableList.builder();
        MongoTableLayoutHandle tableLayout = (MongoTableLayoutHandle) layout;
        MongoTableHandle tableHandle = tableLayout.getTable();

        //Get the collection's chunks info detail
        MongoCursor<Document> chunkCursor= mongoSession.getChunkInfo(tableHandle.getSchemaTableName());

        //Get the collection's sharded type
        String shardType= mongoSession.getShardType(tableHandle.getSchemaTableName());
        LOG.debug("shardType:%s",shardType);

        //If the collection is not sharded or is hash-sharded, return splitsource  with the whole collection as a split;
        if(shardType==null || shardType.equals("hashed"))
        {
            return new FixedSplitSource(ImmutableList.of(new MongoSplit(tableHandle.getSchemaTableName(),
                    tableLayout.getTupleDomain(),
                    addresses)));
        }

        //Scan the chunks, one chunk as a split
        if(chunkCursor.hasNext()) {
            MongoSplit chunkSplit;
            Optional<Map<ColumnHandle, Domain>> chunkDomain;
            Document oneChunk;
            Document minDoc;
            Document maxDoc;
            LinkedHashMap<Object, Object> keySet;
            int splitCount = 0;
            while (chunkCursor.hasNext()) {

                oneChunk = (Document) chunkCursor.next();

                minDoc = (Document) oneChunk.get("min");
                maxDoc = (Document) oneChunk.get("max");

                //init every chunkDomain with the tableLayout's toupleDomain
                chunkDomain = tableLayout.getTupleDomain().getDomains();
                Map<ColumnHandle, Domain> chunkMap = new HashMap<ColumnHandle, Domain>();
                chunkMap.putAll(chunkDomain.get());

                MongoColumnHandle predicateColumnHandle = null;
                Domain newDomain = null;
                Domain oldDomain = null;

                String firstKey = minDoc.keySet().iterator().next();
                Object firstKeyLowValue = minDoc.get(firstKey);
                Object firstKeyHighValue = maxDoc.get(firstKey);


                if (firstKeyLowValue instanceof MinKey) {
                    if (firstKeyHighValue instanceof String) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                VARCHAR,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.lessThan(VARCHAR,
                                        utf8Slice(firstKeyHighValue.toString()))),
                                false);
                    } else if (firstKeyHighValue instanceof ObjectId) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                OBJECT_ID,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.lessThan(OBJECT_ID,
                                        utf8Slice(firstKeyHighValue.toString()))),
                                false);
                    }else if (firstKeyHighValue instanceof Integer) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                BIGINT,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT,
                                        Long.valueOf((Integer) firstKeyHighValue))),
                                false);
                    }else if (firstKeyHighValue instanceof Long) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                BIGINT,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT,
                                        (Long)firstKeyHighValue)),
                                false);
                    }else if (firstKeyHighValue instanceof Float || firstKeyHighValue instanceof Double) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                DOUBLE,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE,
                                        (Double) firstKeyHighValue)),
                                false);
                    }else if (firstKeyHighValue instanceof Date) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                TIMESTAMP,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.lessThan(TIMESTAMP,
                                        ((Date) firstKeyHighValue).getTime())),
                                false);
                    }else {
                        return new FixedSplitSource(ImmutableList.of(new MongoSplit(tableHandle.getSchemaTableName(),
                                tableLayout.getTupleDomain(),
                                addresses)));
                    }
                    oldDomain = chunkMap.get(predicateColumnHandle);
                    if (oldDomain != null) {
                        newDomain = Domain.create(newDomain.getValues().intersect(oldDomain.getValues()), false);
                    }
                }else if (firstKeyHighValue instanceof MaxKey) {
                    if (firstKeyLowValue instanceof String) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                VARCHAR,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(VARCHAR,
                                        utf8Slice(firstKeyLowValue.toString()))),
                                false);
                    } else if (firstKeyLowValue instanceof ObjectId) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                OBJECT_ID,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(OBJECT_ID,
                                        utf8Slice(firstKeyLowValue.toString()))),
                                false);
                    }else if (firstKeyLowValue instanceof Integer) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                BIGINT,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT,
                                        Long.valueOf((Integer) firstKeyLowValue))),
                                false);
                    }else if (firstKeyLowValue instanceof Long) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                BIGINT,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT,
                                        (Long)firstKeyLowValue)),
                                false);
                    } else if (firstKeyLowValue instanceof Float || firstKeyLowValue instanceof Double) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                DOUBLE,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE,
                                        (Double) firstKeyLowValue)),
                                false);
                    }else if (firstKeyLowValue instanceof Date) {
                        predicateColumnHandle = new MongoColumnHandle(firstKey,
                                TIMESTAMP,
                                false);
                        newDomain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(TIMESTAMP,
                                        ((Date) firstKeyLowValue).getTime())),
                                false);
                    }else {
                        return new FixedSplitSource(ImmutableList.of(new MongoSplit(tableHandle.getSchemaTableName(),
                                tableLayout.getTupleDomain(),
                                addresses)));
                    }

                    oldDomain = chunkMap.get(predicateColumnHandle);
                    if (oldDomain != null) {
                        newDomain = Domain.create(newDomain.getValues().intersect(oldDomain.getValues()), false);
                    }
                }
                else if (firstKeyLowValue instanceof String) {
                    predicateColumnHandle = new MongoColumnHandle(firstKey,
                            VARCHAR,
                            false);
                    newDomain = Domain.create(ValueSet.ofRanges(Range.range(VARCHAR,
                                    utf8Slice(firstKeyLowValue.toString()), true,
                                    utf8Slice(firstKeyHighValue.toString()),
                                    false)),
                            false);

                    oldDomain = chunkMap.get(predicateColumnHandle);
                    if (oldDomain != null) {
                        newDomain = Domain.create(newDomain.getValues().intersect(oldDomain.getValues()), false);
                    }
                }
                else if (firstKeyLowValue instanceof Integer) {
                    predicateColumnHandle = new MongoColumnHandle(firstKey,
                            BIGINT,
                            false);
                    newDomain = Domain.create(ValueSet.ofRanges(Range.range(BIGINT,
                                    Long.valueOf((Integer)firstKeyLowValue), true,
                                    Long.valueOf((Integer)firstKeyHighValue),
                                    false)),
                            false);

                    oldDomain = chunkMap.get(predicateColumnHandle);
                    if (oldDomain != null) {
                        newDomain = Domain.create(newDomain.getValues().intersect(oldDomain.getValues()), false);
                    }
                }
                else if (firstKeyLowValue instanceof Long) {
                    predicateColumnHandle = new MongoColumnHandle(firstKey,
                            BIGINT,
                            false);
                    newDomain = Domain.create(ValueSet.ofRanges(Range.range(BIGINT,
                                    (Long)firstKeyLowValue, true,
                                    (Long)firstKeyHighValue,
                                    false)),
                            false);

                    oldDomain = chunkMap.get(predicateColumnHandle);
                    if (oldDomain != null) {
                        newDomain = Domain.create(newDomain.getValues().intersect(oldDomain.getValues()), false);
                    }
                }
                else if (firstKeyLowValue instanceof Float || firstKeyLowValue instanceof Double) {
                    predicateColumnHandle = new MongoColumnHandle(firstKey,
                            DOUBLE,
                            false);
                    newDomain = Domain.create(ValueSet.ofRanges(Range.range(DOUBLE,
                                    (Double)firstKeyLowValue, true,
                                    (Double)firstKeyHighValue,
                                    false)),
                            false);

                    oldDomain = chunkMap.get(predicateColumnHandle);
                    if (oldDomain != null) {
                        newDomain = Domain.create(newDomain.getValues().intersect(oldDomain.getValues()), false);
                    }
                }else if (firstKeyLowValue instanceof Date) {
                    predicateColumnHandle = new MongoColumnHandle(firstKey,
                            TIMESTAMP,
                            false);
                    newDomain = Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP,
                                    ((Date)firstKeyLowValue).getTime(), true,
                                    ((Date) firstKeyHighValue).getTime(),
                                    false)),
                            false);

                    oldDomain = chunkMap.get(predicateColumnHandle);
                    if (oldDomain != null) {
                        newDomain = Domain.create(newDomain.getValues().intersect(oldDomain.getValues()), false);
                    }
                }else if (firstKeyLowValue instanceof ObjectId) {
                    predicateColumnHandle = new MongoColumnHandle(firstKey,
                            OBJECT_ID,
                            false);
                    newDomain = Domain.create(ValueSet.ofRanges(Range.range(OBJECT_ID,
                                    utf8Slice(firstKeyLowValue.toString()), true,
                                    utf8Slice(firstKeyHighValue.toString()),
                                    false)),
                            false);

                    oldDomain = chunkMap.get(predicateColumnHandle);
                    if (oldDomain != null) {
                        newDomain = Domain.create(newDomain.getValues().intersect(oldDomain.getValues()), false);
                    }
                }else {
                    return new FixedSplitSource(ImmutableList.of(new MongoSplit(tableHandle.getSchemaTableName(),
                            tableLayout.getTupleDomain(),
                            addresses)));
                }


                if (!newDomain.getValues().isNone()) {
                    splitCount++;
                    //Print splits' detail info
                    LOG.debug("Split %s range info details below:", splitCount);
                    for (Range range : newDomain.getValues().getRanges().getOrderedRanges()) {
                        if (range.isSingleValue()) {
                            LOG.debug("Split %s range signle value:%s", splitCount, translateValue(range.getSingleValue(),range.getType()));
                        } else {
                            if (!range.getLow().isLowerUnbounded()) {
                                LOG.debug("Split %s range low value:%s", splitCount, translateValue(range.getLow().getValue(),range.getType()));
                            }
                            if (!range.getHigh().isUpperUnbounded()) {
                                LOG.debug("Split %s range high value:%s", splitCount, translateValue(range.getHigh().getValue(), range.getType()));
                            }
                        }
                    }
                    mapPutOrReplace(chunkMap, predicateColumnHandle, newDomain);
                    chunkSplit = new MongoSplit(tableHandle.getSchemaTableName(),
                            TupleDomain.withColumnDomains(chunkMap),
                            addresses);
                    mongoSplitsList.add(chunkSplit);
                }
            }
            LOG.debug("Total splits: %s", splitCount);
            return new FixedSplitSource(mongoSplitsList.build());
        }
        else {
            LOG.debug("Collection is not sharded. Total splits:1.");
            return new FixedSplitSource(ImmutableList.of(new MongoSplit(tableHandle.getSchemaTableName(),
                    tableLayout.getTupleDomain(),
                    addresses)));
        }
    }

    private void mapPutOrReplace(Map map,Object key,Object value)
    {
        if(map.containsKey(key))
        {
            map.replace(key, value);
        }
        else
        {
            map.put(key, value);
        }
    }

    private static Object translateValue(Object source, Type type)
    {
        if (source instanceof Slice) {
            if (type instanceof ObjectIdType) {
                //return new ObjectId(((Slice) source).getBytes());
                return new ObjectId(((Slice) source).toStringUtf8());
            }
            else {
                return ((Slice) source).toStringUtf8();
            }
        }
        if(type instanceof TimestampType)
        {
            return new Date((Long)source);
        }
        return source;
    }
}
