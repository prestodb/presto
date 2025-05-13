package com.facebook.presto.example.prac;

import com.facebook.airlift.configuration.ConfigBinder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;

import javax.inject.Inject;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

/**
 * @Author LTR
 * @Date 2025/4/15 11:18
 * @注释
 */
public class ExamplePracModule implements Module {

    @Override
    public void configure(Binder binder) {
        //1.config
        configBinder(binder).bindConfig(ExamplePracConfig.class);
        //2.json反序列化器，将http返回的json序列化后的转化为presto自己的Type，Type是对int、char等的抽象
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        //3.JsonCodeC是Json的序列化与反序列化？
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(ExamplePracTable.class));
        //4.connector
        binder.bind(ExamplePracConnector.class).in(Singleton.class);
        //5.todo:
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(parseTypeSignature(value));
        }
    }
}
