package com.facebook.presto.dynamo;

import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.facebook.presto.baseplugin.predicate.BasePredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by amehta on 7/25/16.
 */
public class DynamoIndex {
    private final String indexName;
    private final KeySchemaElement hashKey;
    private final Optional<KeySchemaElement> rangeKey;

    private final List<DynamoIndex> indices;

    public DynamoIndex(Optional<KeySchemaElement> rangeKey, KeySchemaElement hashKey, String indexName) {
        this.rangeKey = rangeKey;
        this.hashKey = hashKey;
        this.indexName = indexName;
        this.indices = new ArrayList<>();
    }

    public String getIndexName() {
        return indexName;
    }

    public KeySchemaElement getHashKey() {
        return hashKey;
    }

    public Optional<KeySchemaElement> getRangeKey() {
        return rangeKey;
    }

    public List<DynamoIndex> getIndices() {
        return indices;
    }

    public boolean isValidHashKey(BasePredicate basePredicate){
        return hashKey.getAttributeName().equals(basePredicate.getBaseColumnHandle().getColumnName())
                && DynamoUtils.HASH_OPERATORS.contains(basePredicate.getBaseComparisonOperator());
    }

    public boolean isValidRangeKey(BasePredicate basePredicate) {
        return rangeKey.isPresent()
                && rangeKey.get().getAttributeName().equals(basePredicate.getBaseColumnHandle().getColumnName())
                && DynamoUtils.RANGE_OPERATORS.contains(basePredicate.getBaseComparisonOperator());
    }

    public boolean containsKey(BasePredicate basePredicate, KeyType keyType){
        switch (keyType){
            case HASH:
                return isValidHashKey(basePredicate);
            case RANGE:
                return isValidRangeKey(basePredicate);
            default:
                return false;
        }
    }

    public boolean containsKey(BasePredicate basePredicate){
        boolean isContains = isValidHashKey(basePredicate) || isValidRangeKey(basePredicate);
        if(!isContains){
            isContains = indices.stream().filter(i -> i.containsKey(basePredicate)).findAny().isPresent();
        }
        return isContains;
    }

    public Optional<String> getIndexNameForHashPredicate(BasePredicate basePredicate){
        if(containsKey(basePredicate, KeyType.HASH)){
            return Optional.of(indexName);
        }else{
            Optional<DynamoIndex> index = indices.stream().filter(i -> i.containsKey(basePredicate, KeyType.HASH)).findFirst();
            return index.isPresent() ? Optional.of(index.get().getIndexName()) : Optional.empty();
        }
    }

    public Optional<String> getIndexNameForRangePredicate(BasePredicate basePredicate){
        if(containsKey(basePredicate, KeyType.RANGE)){
            return Optional.of(indexName);
        }else{
            Optional<DynamoIndex> index = indices.stream().filter(i -> i.containsKey(basePredicate, KeyType.RANGE)).findFirst();
            return index.isPresent() ? Optional.of(index.get().getIndexName()) : Optional.empty();
        }
    }
}
