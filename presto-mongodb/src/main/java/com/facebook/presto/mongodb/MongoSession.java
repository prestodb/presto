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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.mongodb.ObjectIdType.OBJECT_ID;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class MongoSession
{
    private static final Logger log = Logger.get(MongoSession.class);
    private static final List<String> SYSTEM_TABLES = Arrays.asList("system.indexes", "system.users", "system.version");

    private static final String TABLE_NAME_KEY = "table";
    private static final String FIELDS_KEY = "fields";
    private static final String FIELDS_NAME_KEY = "name";
    private static final String FIELDS_ALIAS_KEY = "alias";
    private static final String FIELDS_TYPE_KEY = "type";
    private static final String FIELDS_HIDDEN_KEY = "hidden";

    private static final String OR_OP = "$or";
    private static final String AND_OP = "$and";
    private static final String NOT_OP = "$not";
    private static final String NOR_OP = "$nor";

    private static final String EQ_OP = "$eq";
    private static final String NOT_EQ_OP = "$ne";
    private static final String EXISTS_OP = "$exists";
    private static final String GTE_OP = "$gte";
    private static final String GT_OP = "$gt";
    private static final String LT_OP = "$lt";
    private static final String LTE_OP = "$lte";
    private static final String IN_OP = "$in";
    private static final String NOTIN_OP = "$nin";

    private final TypeManager typeManager;
    private final MongoClient client;

    private final String schemaCollection;
    private final int cursorBatchSize;

    private final LoadingCache<SchemaTableName, MongoTableHandle> tableCache;

    private final String implicitPrefix;

    public MongoSession(TypeManager typeManager, MongoClient client, MongoClientConfig config)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.client = requireNonNull(client, "client is null");
        this.schemaCollection = config.getSchemaCollection();
        this.cursorBatchSize = config.getCursorBatchSize();
        this.implicitPrefix = config.getImplicitRowFieldPrefix();

        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(1, HOURS)  // TODO: Configure
                .build(new CacheLoader<SchemaTableName, MongoTableHandle>()
                {
                    @Override
                    public MongoTableHandle load(SchemaTableName key)
                            throws TableNotFoundException
                    {
                        return loadTable(key);
                    }
                });
    }

    public void shutdown()
    {
        client.close();
    }

    /**
     * @return lower-case schema names
     */
    public List<String> getAllSchemas()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();

        for (String name : client.listDatabaseNames()) {
            builder.add(name.toLowerCase(ENGLISH));
        }

        return builder.build();
    }

    /**
     * @param schema lower-case schema name
     * @return A set of lower-case table names of the given schema
     * @throws SchemaNotFoundException
     */
    public Set<String> getAllTables(String schema)
            throws SchemaNotFoundException
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();

        for (String name : client.getDatabase(getDatabaseName(schema)).listCollectionNames()) {
            if (name.equals(schemaCollection) || SYSTEM_TABLES.contains(name)) {
                continue;
            }

            builder.add(name.toLowerCase(ENGLISH));
        }

        return builder.build();
    }

    public MongoTableHandle getTable(SchemaTableName table)
            throws TableNotFoundException
    {
        return getCacheValue(tableCache, table, TableNotFoundException.class);
    }

    public List<MongoColumnHandle> getColumns(MongoTableHandle table)
    {
        Document tableMeta = getTableMetadata(table);

        ImmutableList.Builder<MongoColumnHandle> columnHandles = ImmutableList.builder();

        for (Document columnMetadata : getColumnMetadata(tableMeta)) {
            MongoColumnHandle columnHandle = buildColumnHandle(columnMetadata);
            columnHandles.add(columnHandle);
        }

        return columnHandles.build();
    }

    public List<MongoIndex> getIndexes(MongoTableHandle table)
            throws TableNotFoundException
    {
        return MongoIndex.parse(getCollection(table).listIndexes());
    }

    public void createTable(MongoTableHandle table, List<MongoColumnHandle> columns)
    {
        createTableMetadata(table, columns);
        // collection is created implicitly
    }

    public void dropTable(MongoTableHandle table)
    {
        deleteTableMetadata(table);
        getCollection(table).drop();

        tableCache.invalidate(table.getSchemaTableName());
    }

    public void renameTable(MongoTableHandle table, SchemaTableName newTableName)
    {
        getCollection(table).renameCollection(new MongoNamespace(newTableName.getSchemaName(), newTableName.getTableName()));

        tableCache.invalidate(table.getSchemaTableName());
    }

    private MongoColumnHandle buildColumnHandle(Document columnMeta)
    {
        String name = columnMeta.getString(FIELDS_NAME_KEY);
        String alias = columnMeta.getString(FIELDS_ALIAS_KEY);
        String typeString = columnMeta.getString(FIELDS_TYPE_KEY);
        boolean hidden = columnMeta.getBoolean(FIELDS_HIDDEN_KEY, false);

        Type type = typeManager.getType(TypeSignature.parseTypeSignature(typeString));

        return new MongoColumnHandle(name, alias, type, hidden);
    }

    private List<Document> getColumnMetadata(Document doc)
    {
        return (List<Document>) doc.getOrDefault(FIELDS_KEY, ImmutableList.of());
    }

    public MongoCollection<Document> getCollection(MongoTableHandle table)
    {
        return getCollection(table.getDatabaseName(), table.getCollectionName());
    }

    private MongoCollection<Document> getSchemaCollection(String dbName)
    {
        return getCollection(dbName, schemaCollection);
    }

    private MongoCollection<Document> getCollection(String schema, String table)
    {
        return client.getDatabase(schema).getCollection(table);
    }

    private static <K, V, E extends Exception> V getCacheValue(LoadingCache<K, V> cache, K key, Class<E> exceptionClass)
            throws E
    {
        try {
            return cache.get(key);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            Throwable t = e.getCause();
            Throwables.propagateIfInstanceOf(t, exceptionClass);
            throw Throwables.propagate(t);
        }
    }

    public MongoCursor<Document> execute(MongoSplit split, List<MongoColumnHandle> columns)
    {
        Document output = new Document();
        for (MongoColumnHandle column : columns) {
            output.append(column.getName(), 1);
        }
        MongoCollection<Document> collection = getCollection(split.getTable());
        FindIterable<Document> iterable = collection.find(buildQuery(split.getTupleDomain())).projection(output);

        if (cursorBatchSize != 0) {
            iterable.batchSize(cursorBatchSize);
        }

        return iterable.iterator();
    }

    @VisibleForTesting
    static Document buildQuery(TupleDomain<ColumnHandle> tupleDomain)
    {
        Document query = new Document();
        if (tupleDomain.getDomains().isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
                MongoColumnHandle column = (MongoColumnHandle) entry.getKey();
                query.putAll(buildPredicate(column, entry.getValue()));
            }
        }

        return query;
    }

    private static Document buildPredicate(MongoColumnHandle column, Domain domain)
    {
        String name = column.getName();
        Type type = column.getType();
        if (domain.getValues().isNone() && domain.isNullAllowed()) {
            return documentOf(name, isNullPredicate());
        }
        if (domain.getValues().isAll() && !domain.isNullAllowed()) {
            return documentOf(name, isNotNullPredicate());
        }

        List<Object> singleValues = new ArrayList<>();
        List<Document> disjuncts = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                singleValues.add(range.getSingleValue());
            }
            else {
                Document rangeConjuncts = new Document();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.put(GT_OP, range.getLow().getValue());
                            break;
                        case EXACTLY:
                            rangeConjuncts.put(GTE_OP, range.getLow().getValue());
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low Marker should never use BELOW bound: " + range);
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High Marker should never use ABOVE bound: " + range);
                        case EXACTLY:
                            rangeConjuncts.put(LTE_OP, range.getHigh().getValue());
                            break;
                        case BELOW:
                            rangeConjuncts.put(LT_OP, range.getHigh().getValue());
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                verify(!rangeConjuncts.isEmpty());
                disjuncts.add(rangeConjuncts);
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(documentOf(EQ_OP, translateValue(singleValues.get(0), type)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(documentOf(IN_OP, singleValues.stream()
                    .map(value -> translateValue(value, type))
                    .collect(toList())));
        }

        if (domain.isNullAllowed()) {
            disjuncts.add(isNullPredicate());
        }

        return orPredicate(disjuncts.stream()
                .map(disjunct -> new Document(name, disjunct))
                .collect(toList()));
    }

    private static Object translateValue(Object source, Type type)
    {
        if (source instanceof Slice) {
            if (type instanceof ObjectIdType) {
                return new ObjectId(((Slice) source).getBytes());
            }
            else {
                return ((Slice) source).toStringUtf8();
            }
        }

        return source;
    }

    private static Document documentOf(String key, Object value)
    {
        return new Document(key, value);
    }

    private static Document orPredicate(List<Document> values)
    {
        checkState(!values.isEmpty());
        if (values.size() == 1) {
            return values.get(0);
        }
        return new Document(OR_OP, values);
    }

    private static Document isNullPredicate()
    {
        return documentOf(EXISTS_OP, true).append(EQ_OP, null);
    }

    private static Document isNotNullPredicate()
    {
        return documentOf(NOT_EQ_OP, null);
    }

    // Internal Schema management
    private Document getTableMetadata(MongoTableHandle table)
    {
        MongoCollection<Document> schema = getSchemaCollection(table.getDatabaseName());

        Document metadata = schema
                .find(documentOf(TABLE_NAME_KEY, table.getCollectionName())).first();

        if (metadata == null) {
            metadata = new Document(TABLE_NAME_KEY, table.getCollectionName());
            metadata.append(FIELDS_KEY, guessTableFields(table));

            schema.createIndex(new Document(TABLE_NAME_KEY, 1), new IndexOptions().unique(true));
            schema.insertOne(metadata);
        }

        return metadata;
    }

    private void createTableMetadata(MongoTableHandle table, List<MongoColumnHandle> columns)
    {
        Document metadata = new Document(TABLE_NAME_KEY, table.getCollectionName());

        ArrayList<Document> fields = new ArrayList<>();
        if (!columns.stream().anyMatch(c -> c.getName().equals("_id"))) {
            fields.add(new MongoColumnHandle("_id", null, OBJECT_ID, true).getDocument());
        }

        fields.addAll(columns.stream()
                .map(MongoColumnHandle::getDocument)
                .collect(toList()));

        metadata.append(FIELDS_KEY, fields);

        MongoCollection<Document> schema = getSchemaCollection(table.getDatabaseName());
        schema.createIndex(new Document(TABLE_NAME_KEY, 1), new IndexOptions().unique(true));
        schema.insertOne(metadata);
    }

    private boolean deleteTableMetadata(MongoTableHandle table)
    {
        DeleteResult result = getSchemaCollection(table.getDatabaseName())
                .deleteOne(documentOf(TABLE_NAME_KEY, table.getCollectionName()));

        return result.getDeletedCount() == 1;
    }

    private List<Document> guessTableFields(MongoTableHandle table)
    {
        Document doc = getCollection(table).find().first();
        if (doc == null) {
            // no records at the collection
            throw new PrestoException(NOT_SUPPORTED, "Empty collection");
        }

        ImmutableList.Builder<Document> builder = ImmutableList.builder();

        for (String key : doc.keySet()) {
            Object value = doc.get(key);
            Optional<TypeSignature> fieldType = guessFieldType(value);
            if (fieldType.isPresent()) {
                Document metadata = new Document();
                metadata.append(FIELDS_NAME_KEY, key);
                metadata.append(FIELDS_ALIAS_KEY, key.toLowerCase(ENGLISH));
                metadata.append(FIELDS_TYPE_KEY, fieldType.get().toString());
                metadata.append(FIELDS_HIDDEN_KEY,
                        key.equals("_id") && fieldType.get().equals(OBJECT_ID.getTypeSignature()));

                builder.add(metadata);
            }
            else {
                log.debug("Unable to guess field type from %s : %s", value == null ? "null" : value.getClass().getName(), value);
            }
        }

        return builder.build();
    }

    private Optional<TypeSignature> guessFieldType(Object value)
    {
        if (value == null) {
            return Optional.empty();
        }

        TypeSignature typeSignature = null;
        if (value instanceof String) {
            typeSignature = createUnboundedVarcharType().getTypeSignature();
        }
        else if (value instanceof Integer || value instanceof Long) {
            typeSignature = BIGINT.getTypeSignature();
        }
        else if (value instanceof Boolean) {
            typeSignature = BOOLEAN.getTypeSignature();
        }
        else if (value instanceof Float || value instanceof Double) {
            typeSignature = DOUBLE.getTypeSignature();
        }
        else if (value instanceof Date) {
            typeSignature = TIMESTAMP.getTypeSignature();
        }
        else if (value instanceof ObjectId) {
            typeSignature = OBJECT_ID.getTypeSignature();
        }
        else if (value instanceof List) {
            List<Optional<TypeSignature>> subTypes = ((List<?>) value).stream()
                    .map(this::guessFieldType)
                    .collect(toList());

            if (subTypes.isEmpty() || subTypes.stream().anyMatch(t -> !t.isPresent())) {
                return Optional.empty();
            }

            Set<TypeSignature> signatures = subTypes.stream().map(t -> t.get()).collect(toSet());
            if (signatures.size() == 1) {
                typeSignature = new TypeSignature(StandardTypes.ARRAY, signatures.stream()
                        .map(s -> TypeSignatureParameter.of(s))
                        .collect(Collectors.toList()));
            }
            else {
                // TODO: presto cli doesn't handle empty field name row type yet
                typeSignature = new TypeSignature(StandardTypes.ROW,
                        IntStream.range(0, subTypes.size())
                                .mapToObj(idx -> TypeSignatureParameter.of(
                                        new NamedTypeSignature(String.format("%s%d", implicitPrefix, idx + 1), subTypes.get(idx).get())))
                                .collect(toList()));
            }
        }
        else if (value instanceof Document) {
            List<TypeSignatureParameter> parameters = new ArrayList<>();

            for (String key : ((Document) value).keySet()) {
                Optional<TypeSignature> fieldType = guessFieldType(((Document) value).get(key));
                if (!fieldType.isPresent()) {
                    return Optional.empty();
                }

                parameters.add(TypeSignatureParameter.of(new NamedTypeSignature(key, fieldType.get())));
            }
            typeSignature = new TypeSignature(StandardTypes.ROW, parameters);
        }

        return Optional.ofNullable(typeSignature);
    }

    private String getDatabaseName(String lname)
            throws SchemaNotFoundException
    {
        for (String name : client.listDatabaseNames()) {
            if (name.equalsIgnoreCase(lname)) {
                return name;
            }
        }

        throw new SchemaNotFoundException(lname);
    }

    private MongoTableHandle loadTable(SchemaTableName table)
            throws TableNotFoundException
    {
        String databaseName;
        try {
            databaseName = getDatabaseName(table.getSchemaName());
        }
        catch (SchemaNotFoundException e) {
            throw new TableNotFoundException(table);
        }

        Map<SchemaTableName, MongoTableHandle> tables = new HashMap<>();
        for (String collectionName : client.getDatabase(databaseName).listCollectionNames()) {
            MongoTableHandle tableHandle = new MongoTableHandle(databaseName, collectionName);
            tables.put(tableHandle.getSchemaTableName(), tableHandle);
        }
        tableCache.putAll(tables);

        MongoTableHandle tableHandle = tables.get(table);
        if (tableHandle == null) {
            throw new TableNotFoundException(table);
        }

        return tableHandle;
    }
}
