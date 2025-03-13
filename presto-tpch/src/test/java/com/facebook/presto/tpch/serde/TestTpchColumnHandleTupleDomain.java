package com.facebook.presto.tpch.serde;

import com.facebook.presto.common.experimental.ColumnHandleAdapter;
import com.facebook.presto.common.experimental.ThriftTupleDomainSerde;
import com.facebook.presto.common.experimental.auto_gen.ThriftTupleDomain;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;

public class TestTpchColumnHandleTupleDomain
{
    @Test
    void testTpchColumnTupleDomain()
    {
        ColumnHandle colA = new TpchColumnHandle("A", BIGINT);
        ColumnHandle colB = new TpchColumnHandle("B", BIGINT);
        ColumnHandle colC = new TpchColumnHandle("C", INTEGER);
        ColumnHandle colD = new TpchColumnHandle("D", INTEGER);
        VarcharType varcharType = createVarcharType(4);
        ColumnHandle colE = new TpchColumnHandle("E", varcharType);

        Map<ColumnHandle, Domain> tupleDomain = new HashMap<>();
        tupleDomain.put(colA, Domain.singleValue(BIGINT, 23L));
        tupleDomain.put(colB, Domain.multipleValues(BIGINT, ImmutableList.of(0L, 1L)));
        tupleDomain.put(colC, Domain.notNull(INTEGER));
        tupleDomain.put(colD, Domain.onlyNull(INTEGER));
        tupleDomain.put(colE, Domain.singleValue(varcharType, Slices.utf8Slice("test")));

        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(tupleDomain);
        ThriftTupleDomain thriftTupleDomain = predicate.toThrift(new ThriftTupleDomainSerde<ColumnHandle>()
        {
            @Override
            public byte[] serialize(ColumnHandle obj)
            {
                return ColumnHandleAdapter.serialize(obj);
            }
        });

        try {
            TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());

            byte[] bytes = serializer.serialize(thriftTupleDomain);
            ThriftTupleDomain deserializedDomain = new ThriftTupleDomain();
            deserializer.deserialize(deserializedDomain, bytes);
            TupleDomain<ColumnHandle> deserilizedPredicate = TupleDomain.fromThrift(deserializedDomain, new ThriftTupleDomainSerde<ColumnHandle>()
            {
                @Override
                public ColumnHandle deserialize(byte[] bytes)
                {
                    return (ColumnHandle) ColumnHandleAdapter.deserialize(bytes);
                }
            });

            assertEquals(deserilizedPredicate, predicate);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
