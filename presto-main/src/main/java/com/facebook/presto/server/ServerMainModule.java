/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.importer.ForImportManager;
import com.facebook.presto.importer.ImportManager;
import com.facebook.presto.importer.LocalShardManager;
import com.facebook.presto.importer.NodeWorkerQueue;
import com.facebook.presto.metadata.DatabaseShardManager;
import com.facebook.presto.metadata.DatabaseStorageManager;
import com.facebook.presto.metadata.ForMetadata;
import com.facebook.presto.metadata.ForShardManager;
import com.facebook.presto.metadata.ForStorageManager;
import com.facebook.presto.metadata.ImportMetadata;
import com.facebook.presto.metadata.InformationSchemaData;
import com.facebook.presto.metadata.InformationSchemaMetadata;
import com.facebook.presto.metadata.InternalMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NativeMetadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.operator.ForExchange;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.split.DataStreamManager;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.ImportClientFactory;
import com.facebook.presto.split.ImportDataStreamProvider;
import com.facebook.presto.split.InternalDataStreamProvider;
import com.facebook.presto.split.NativeDataStreamProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanFragmentSourceProvider;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.http.client.HttpClientBinder;
import io.airlift.json.JsonBinder;
import org.antlr.runtime.RecognitionException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Singleton;
import java.io.IOException;

import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class ServerMainModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(QueryResource.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);

        binder.bind(TaskResource.class).in(Scopes.SINGLETON);
        binder.bind(TaskManager.class).to(SqlTaskManager.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        binder.bind(TaskScheduler.class).in(Scopes.SINGLETON);

        binder.bind(PagesMapper.class).in(Scopes.SINGLETON);

        HttpClientBinder.httpClientBinder(binder).bindHttpClient("exchange", ForExchange.class).withTracing();
        HttpClientBinder.httpClientBinder(binder).bindHttpClient("scheduler", ForScheduler.class).withTracing();
        binder.bind(PlanFragmentSourceProvider.class).to(HackPlanFragmentSourceProvider.class).in(Scopes.SINGLETON);

        binder.bind(StorageManager.class).to(DatabaseStorageManager.class).in(Scopes.SINGLETON);
        binder.bind(DataStreamProvider.class).to(DataStreamManager.class).in(Scopes.SINGLETON);
        binder.bind(NativeDataStreamProvider.class).in(Scopes.SINGLETON);
        binder.bind(ImportDataStreamProvider.class).in(Scopes.SINGLETON);

        binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(NativeMetadata.class).in(Scopes.SINGLETON);

        binder.bind(InternalMetadata.class).in(Scopes.SINGLETON);
        binder.bind(InternalDataStreamProvider.class).in(Scopes.SINGLETON);
        binder.bind(InformationSchemaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(InformationSchemaData.class).in(Scopes.SINGLETON);

        binder.bind(ImportClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(ImportMetadata.class).in(Scopes.SINGLETON);

        binder.bind(SplitManager.class).in(Scopes.SINGLETON);


        // fragment serialization
        jsonCodecBinder(binder).bindJsonCodec(QueryFragmentRequest.class);

        JsonBinder.jsonBinder(binder).addSerializerBinding(Expression.class).toInstance(new JsonSerializer<Expression>()
        {
            @Override
            public void serialize(Expression expression, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                    throws IOException
            {
                jsonGenerator.writeString(ExpressionFormatter.toString(expression));
            }
        });

        JsonBinder.jsonBinder(binder).addDeserializerBinding(Expression.class).toInstance(new JsonDeserializer<Expression>()
        {
            @Override
            public Expression deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                    throws IOException
            {
                try {
                    return SqlParser.createExpression(jsonParser.readValueAs(String.class));
                }
                catch (RecognitionException e) {
                    throw Throwables.propagate(e);
                }
            }
        });

        JsonBinder.jsonBinder(binder).addDeserializerBinding(FunctionCall.class).toInstance(new JsonDeserializer<FunctionCall>()
        {
            @Override
            public FunctionCall deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                    throws IOException
            {
                try {
                    return (FunctionCall) SqlParser.createExpression(jsonParser.readValueAs(String.class));
                }
                catch (RecognitionException e) {
                    throw Throwables.propagate(e);
                }
            }
        });


        discoveryBinder(binder).bindSelector("presto");
        discoveryBinder(binder).bindSelector("hive-metastore");

        binder.bind(NodeManager.class).in(Scopes.SINGLETON);
        binder.bind(NodeWorkerQueue.class).in(Scopes.SINGLETON);
        binder.bind(ShardManager.class).to(DatabaseShardManager.class).in(Scopes.SINGLETON);
        binder.bind(ImportManager.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("importer", ForImportManager.class).withFilter(NodeIdUserAgentRequestFilter.class);
        binder.bind(LocalShardManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardResource.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(ShardImport.class);

        discoveryBinder(binder).bindHttpAnnouncement("presto");
    }

    @Provides
    @Singleton
    @ForStorageManager
    public IDBI createStorageManagerDBI()
    {
        // TODO: configuration
        return new DBI("jdbc:h2:file:var/presto-data/db/StorageManager;DB_CLOSE_DELAY=-1;MVCC=TRUE");
    }

    @Provides
    @Singleton
    @ForMetadata
    public IDBI createMetadataDBI()
    {
        // TODO: configuration
        return new DBI("jdbc:h2:file:var/presto-data/db/Metadata;DB_CLOSE_DELAY=-1;MVCC=TRUE");
    }

    @Provides
    @Singleton
    @ForShardManager
    public IDBI createShardManagerDBI()
    {
        // TODO: configuration
        return new DBI("jdbc:h2:file:var/presto-data/db/ShardManager;DB_CLOSE_DELAY=-1;MVCC=TRUE");
    }
}
