package com.facebook.presto.dynamo;

import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.facebook.presto.baseplugin.BaseConnectorInfo;
import com.facebook.presto.baseplugin.BaseProvider;
import com.facebook.presto.baseplugin.BaseSplit;
import com.facebook.presto.baseplugin.BaseSplitManager;
import com.facebook.presto.baseplugin.BaseTableHandle;
import com.facebook.presto.baseplugin.predicate.BaseComparisonOperator;
import com.facebook.presto.baseplugin.predicate.BasePredicate;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by amehta on 8/15/16.
 */
public class DynamoSplitManager extends BaseSplitManager {
    @Inject
    public DynamoSplitManager(NodeManager nodeManager, BaseConnectorInfo baseConnectorInfo, BaseProvider baseProvider) {
        super(nodeManager, baseConnectorInfo, baseProvider);
    }

    @Override
    public List<BasePredicate> updateBasePredicates(List<BasePredicate> basePredicates, BaseTableHandle tableHandle, BaseProvider baseProvider, ConnectorSession session) {
        //filter out invalid and/or non-key predicates
        return basePredicates.stream()
                .filter(((DynamoProvider) baseProvider).getDynamoIndex(tableHandle.getSchemaTableName(), Optional.of(session))::containsKey)
                .collect(Collectors.toList());
    }

    @Override
    public List<BaseSplit> updateBaseSplits(List<BaseSplit> baseSplits, BaseProvider baseProvider, ConnectorSession session) {//split predicates contain only key attributes
        DynamoProvider dynamoProvider = (DynamoProvider) baseProvider;
        return baseSplits.stream().flatMap(b -> splitBaseSplit(b, dynamoProvider, session)).collect(Collectors.toList());
    }

    private Stream<BaseSplit> splitBaseSplit(BaseSplit baseSplit, DynamoProvider dynamoProvider, ConnectorSession session){//these should be pre-filtered for only key attributes
        if(baseSplit.getPredicates().isEmpty()){
            return ImmutableList.of(baseSplit).stream();
        }
        DynamoIndex index = dynamoProvider.getDynamoIndex(baseSplit.getTableName(), Optional.of(session));
        List<BasePredicate> basePredicates = Lists.newArrayList(baseSplit.getPredicates());
        Map<String,List<BasePredicate>> predicateMap = new HashMap<>();//key is index

        Optional<BasePredicate> hashPredicate = basePredicates.stream().filter(p -> index.containsKey(p, KeyType.HASH)).findFirst();
        Optional<BasePredicate> rangePredicate = basePredicates.stream().filter(p -> index.containsKey(p, KeyType.RANGE)).findFirst();

        if(hashPredicate.isPresent()){// main table
            predicateMap.put(baseSplit.getTableName().getTableName(), new ArrayList<>());
            predicateMap.get(baseSplit.getTableName().getTableName()).add(hashPredicate.get());
            rangePredicate.ifPresent(r -> predicateMap.get(baseSplit.getTableName().getTableName()).add(r));
        }

        for (BasePredicate basePredicate : basePredicates) {// hash predicates
            Optional<String> indexName = index.getIndexNameForHashPredicate(basePredicate);
            if(indexName.isPresent()) {
                predicateMap.put(indexName.get(), new ArrayList<>());
                predicateMap.get(indexName.get()).add(basePredicate);
            }
        }

        for (BasePredicate basePredicate : basePredicates) {// range predicates
            for (String indexName : predicateMap.keySet()) {
                if(index.getIndexName().equals(indexName) && index.isValidRangeKey(basePredicate)){
                    predicateMap.get(indexName).add(basePredicate);
                }else{
                    Optional<DynamoIndex> dynamoIndex = index.getIndices().stream()
                            .filter(i -> i.getIndexName().equals(indexName))
                            .findFirst();
                    if(dynamoIndex.isPresent() && dynamoIndex.get().isValidRangeKey(basePredicate)){
                        predicateMap.get(indexName).add(basePredicate);
                    }
                }
            }
        }

        if(predicateMap.isEmpty()){//in the event a valid predicate pair/group was not found
            return ImmutableList.of(new BaseSplit(baseSplit.getAddresses(), baseSplit.getTableName(), ImmutableList.of())).stream();
        }

        return predicateMap.entrySet().stream()
                .map(e -> new BaseSplit(
                        baseSplit.getAddresses(),
                        baseSplit.getTableName(),
                        e.getValue()))
                .flatMap(this::splitBaseSplitOnPredicate);
    }

    //splits the given split on the hash/range predicate if the comparison operator is splittable
    private Stream<BaseSplit> splitBaseSplitOnPredicate(BaseSplit baseSplit) {// split should only have one or two predicates
        Stream<BasePredicate> predicateStream;
        Optional<BasePredicate> rangePredicate = Optional.empty();
        switch (baseSplit.getPredicates().size()){
            case 2:
                rangePredicate = Optional.of(baseSplit.getPredicates().get(1));
            case 1:
                BasePredicate hashPredicate = baseSplit.getPredicates().get(0);
                switch (hashPredicate.getBaseComparisonOperator()){
                    //break up IN predicate into x number of EQ predicates (one for each value in the IN clause)
                    case IN:
                        predicateStream = hashPredicate.getValues().stream().map(v -> new BasePredicate(hashPredicate.getBaseColumnHandle(), BaseComparisonOperator.EQUAL, ImmutableList.of(v)));
                        if(rangePredicate.isPresent()){
                            BasePredicate range = rangePredicate.get();
                            return predicateStream.map(p -> new BaseSplit(baseSplit.getAddresses(), baseSplit.getTableName(), ImmutableList.of(p, range)));
                        }
                        return predicateStream.map(p -> new BaseSplit(baseSplit.getAddresses(), baseSplit.getTableName(), ImmutableList.of(p)));
                }
        }
        return ImmutableList.of(baseSplit).stream();
    }
}
