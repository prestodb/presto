package com.facebook.presto.dynamo;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.facebook.presto.baseplugin.BaseColumnHandle;
import com.facebook.presto.baseplugin.BaseProvider;
import com.facebook.presto.baseplugin.BaseQuery;
import com.facebook.presto.baseplugin.BaseSplit;
import com.facebook.presto.baseplugin.cache.BaseRecordBatch;
import com.facebook.presto.baseplugin.predicate.BasePredicate;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by amehta on 6/14/16.
 */
public class DynamoProvider extends BaseProvider {
    private final DynamoConfig dynamoConfig;
    private final Map<String, AmazonDynamoDB> clientMap; //key is regionName
    private final Map<String,DynamoIndex> indexMap; //maps tableName to table schema
    private final Map<String,Region> schemaMap;
    private final RetryPolicy defaultRetryPolicy;

    @Inject
    public DynamoProvider(DynamoConfig config) {
        super(config);
        this.dynamoConfig = config;
        this.clientMap = new ConcurrentHashMap<>();
        this.indexMap = new ConcurrentHashMap<>();
        this.defaultRetryPolicy = PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(dynamoConfig.getMaxRetries(Optional.empty()));

        //adds the regions
        ImmutableMap.Builder<String,Region> schemaMap = ImmutableMap.builder();
        for (Regions r : Regions.values()) {
            Optional<Region> regionOpt = Optional.ofNullable(Region.getRegion(r));
            regionOpt.ifPresent(region -> schemaMap.put(region.getName(), region));
        }
        //adds current region on ec2
        Optional.ofNullable(Regions.getCurrentRegion()).ifPresent(r -> schemaMap.put(r.getName(), r));
        this.schemaMap = schemaMap.build();
    }

    public DynamoConfig getDynamoConfig() {
        return dynamoConfig;
    }

    public AmazonDynamoDB getClient(String key, Optional<ConnectorSession> session){// create object as separate variable to allow room for further client configuration later
        if (!clientMap.containsKey(key)) {
            ClientConfiguration configuration = new ClientConfiguration().withRetryPolicy(defaultRetryPolicy);
            AmazonDynamoDB dynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials(dynamoConfig.getAwsKey(session), dynamoConfig.getSecretKey(session)), configuration);
            dynamoDB.setRegion(schemaMap.get(key));
            clientMap.put(key, dynamoDB);
        }
        return clientMap.get(key);
    }

    private DynamoIndex getDynamoIndex(List<KeySchemaElement> keys, String indexName){
        Optional<KeySchemaElement> rangeKey = keys.stream().filter(k -> k.getKeyType().equals(KeyType.RANGE.name())).findFirst();
        KeySchemaElement hashKey = keys.stream().filter(k -> k.getKeyType().equals(KeyType.HASH.name())).findFirst().get();
        return new DynamoIndex(rangeKey, hashKey, indexName);
    }

    public DynamoIndex getDynamoIndex(SchemaTableName schemaTableName, Optional<ConnectorSession> session){
        if(!indexMap.containsKey(schemaTableName.getTableName())){
            TableDescription tableDescription = getClient(schemaTableName.getSchemaName(), session).describeTable(schemaTableName.getTableName()).getTable();
            DynamoIndex tableIndex = getDynamoIndex(tableDescription.getKeySchema(), schemaTableName.getTableName());
            //build gsis
            if(tableDescription.getGlobalSecondaryIndexes() != null) {
                tableIndex.getIndices().addAll(tableDescription.getGlobalSecondaryIndexes().stream().map(g -> getDynamoIndex(g.getKeySchema(), g.getIndexName())).collect(Collectors.toList()));
            }
            //build lsis
            if(tableDescription.getLocalSecondaryIndexes() != null) {
                tableIndex.getIndices().addAll(tableDescription.getLocalSecondaryIndexes().stream().map(g -> getDynamoIndex(g.getKeySchema(), g.getIndexName())).collect(Collectors.toList()));
            }
            indexMap.put(schemaTableName.getTableName(), tableIndex);
        }
        return indexMap.get(schemaTableName.getTableName());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.copyOf(schemaMap.keySet());
    }

    @Override
    public List<BaseColumnHandle> generateTableColumns(ConnectorSession session, SchemaTableName tableName) {
        Optional<ConnectorSession> sessionOpt = Optional.of(session);

        List<Map<String,AttributeValue>> records = getClient(tableName.getSchemaName(), sessionOpt).scan(new ScanRequest(tableName.getTableName()+dynamoConfig.getLookupSuffix(sessionOpt))).getItems();
        ImmutableList.Builder<BaseColumnHandle> columns = ImmutableList.builder();
        for(int i=0 ; i < records.size() ; i++){
            Map<String,AttributeValue> record = records.get(i);
            columns.add(new BaseColumnHandle(record.get(dynamoConfig.getLookupColumnName(Optional.of(session))).getS(), DynamoUtils.getTypeForDynamoType(record.get(dynamoConfig.getLookupColumnType(sessionOpt)).getS()), i));
        }
        return columns.build();
    }

    @Override
    public List<SchemaTableName> listTableNames(ConnectorSession session, Optional<String> schemaName) {
        Optional<Region> region = Optional.ofNullable(Regions.getCurrentRegion());
        String schema = schemaName.orElse(region.isPresent() ? region.get().getName() : Regions.DEFAULT_REGION.name());
        return getClient(schema, Optional.of(session)).listTables().getTableNames().stream().map(t -> new SchemaTableName(schema, t)).collect(Collectors.toList());
    }

    private void applyPredicatesToQuery(QueryRequest request, BaseSplit baseSplit, Optional<ConnectorSession> session){//applies predicates to query and assigns an index name if needed
        List<BasePredicate> basePredicates = baseSplit.getPredicates();
        ImmutableMap.Builder<String,Condition> keyConditions = ImmutableMap.builder();
        Optional<String> indexName = Optional.empty();

        switch (basePredicates.size()){
            case 2:
                BasePredicate rangePredicate = basePredicates.get(1);
                Condition rangeCondition = new Condition();
                rangeCondition.setComparisonOperator(DynamoUtils.fromBaseComparisonOperator(rangePredicate.getBaseComparisonOperator()));
                rangeCondition.setAttributeValueList(rangePredicate.getValues().stream().map(v -> DynamoUtils.getAttributeValueForObject(v)).collect(Collectors.toList()));
                keyConditions.put(rangePredicate.getBaseColumnHandle().getColumnName(),rangeCondition);
            case 1:
                BasePredicate hashPredicate = basePredicates.get(0);
                Condition hashCondition = new Condition();
                hashCondition.setComparisonOperator(DynamoUtils.fromBaseComparisonOperator(hashPredicate.getBaseComparisonOperator()));
                hashCondition.setAttributeValueList(hashPredicate.getValues().stream().map(v -> DynamoUtils.getAttributeValueForObject(v)).collect(Collectors.toList()));
                keyConditions.put(hashPredicate.getBaseColumnHandle().getColumnName(), hashCondition);
                indexName = getDynamoIndex(baseSplit.getTableName(), session).getIndexNameForHashPredicate(hashPredicate);
        }

        if(indexName.isPresent() && !indexName.get().equals(baseSplit.getTableName().getTableName())){
            request.setIndexName(indexName.get());
        }
        request.setKeyConditions(keyConditions.build());
    }

    private void updateQueryBatch(DynamoQuery query, DynamoRecordBatch dynamoRecordBatch){//updates the limit on the query and determines if last batch
        boolean limitUsed = false;
        if(query.getLimit().isPresent()){//update limit
            Integer limit = query.getLimit().get();
            limit -= dynamoRecordBatch.getBaseRecords().size();
            query.setLimit(limit > 0 ? Optional.of(limit) : Optional.empty());
            limitUsed = limit <= 0;
        }
        query.setOffset(Optional.ofNullable(dynamoRecordBatch.getLastKeyEvaluated()));
        dynamoRecordBatch.setLastBatch(limitUsed || !query.getOffset().isPresent());
    }

    public boolean shouldScan(BaseQuery key){
        return key.getBaseSplit().getPredicates().isEmpty();
    }

    @Override
    public BaseRecordBatch getRecordBatchForQueryFromSource(BaseQuery key) {//assume that a list of basePredicates always has only one hashKey and one/zero rangeKeys
        DynamoQuery dynamoQuery = (DynamoQuery) key;
        BaseSplit baseSplit = key.getBaseSplit();
        Optional<ConnectorSession> session = Optional.of(key.getConnectorSession());
        AmazonDynamoDB dynamo = getClient(baseSplit.getTableName().getSchemaName(), session);
        DynamoRecordBatch dynamoRecordBatch;

        if(shouldScan(key)){
            ScanRequest scan = new ScanRequest(baseSplit.getTableName().getTableName());

            dynamoQuery.getOffset().ifPresent(scan::setExclusiveStartKey);

            ScanResult result = dynamo.scan(scan);
            dynamoRecordBatch = new DynamoRecordBatch(result.getItems().stream().map(DynamoRecord::new).collect(Collectors.toList()), false, result.getLastEvaluatedKey());
        }else{
            QueryRequest query = new QueryRequest(baseSplit.getTableName().getTableName());
            dynamoQuery.getLimit().ifPresent(query::setLimit);
            dynamoQuery.getOffset().ifPresent(query::setExclusiveStartKey);
            applyPredicatesToQuery(query, baseSplit, session);

            QueryResult result = dynamo.query(query);
            dynamoRecordBatch = new DynamoRecordBatch(result.getItems().stream().map(DynamoRecord::new).collect(Collectors.toList()), false, result.getLastEvaluatedKey());
        }

        updateQueryBatch(dynamoQuery, dynamoRecordBatch);
        return dynamoRecordBatch;
    }
}
