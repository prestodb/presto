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
package com.facebook.presto.common.type;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.codec.internal.reflection.ReflectionThriftCodecFactory;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.function.Function;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestTypeSignatureParameterUnionSerde
{
    private static final ThriftCatalog COMMON_CATALOG = new ThriftCatalog();
    private static final ThriftCodecManager COMPILER_READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), COMMON_CATALOG, ImmutableSet.of());
    private static final ThriftCodecManager COMPILER_WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false), COMMON_CATALOG, ImmutableSet.of());
    private static final ThriftCodec<TypeSignatureParameterUnion> COMPILER_READ_CODEC = COMPILER_READ_CODEC_MANAGER.getCodec(TypeSignatureParameterUnion.class);
    private static final ThriftCodec<TypeSignatureParameterUnion> COMPILER_WRITE_CODEC = COMPILER_WRITE_CODEC_MANAGER.getCodec(TypeSignatureParameterUnion.class);
    private static final ThriftCodecManager REFLECTION_READ_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), COMMON_CATALOG, ImmutableSet.of());
    private static final ThriftCodecManager REFLECTION_WRITE_CODEC_MANAGER = new ThriftCodecManager(new ReflectionThriftCodecFactory(), COMMON_CATALOG, ImmutableSet.of());
    private static final ThriftCodec<TypeSignatureParameterUnion> REFLECTION_READ_CODEC = REFLECTION_READ_CODEC_MANAGER.getCodec(TypeSignatureParameterUnion.class);
    private static final ThriftCodec<TypeSignatureParameterUnion> REFLECTION_WRITE_CODEC = REFLECTION_WRITE_CODEC_MANAGER.getCodec(TypeSignatureParameterUnion.class);
    private static final TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);
    private static final TypeSignature FAKE_TYPE_SIGNATURE = new TypeSignature("FAKE_BASE");
    private TypeSignatureParameterUnion typeSignatureParameterUnion;

    @BeforeMethod
    public void setUp()
    {
        typeSignatureParameterUnion = new TypeSignatureParameterUnion(FAKE_TYPE_SIGNATURE);
    }

    @DataProvider
    public Object[][] codecCombinations()
    {
        return new Object[][] {
                {COMPILER_READ_CODEC, COMPILER_WRITE_CODEC},
                {COMPILER_READ_CODEC, REFLECTION_WRITE_CODEC},
                {REFLECTION_READ_CODEC, COMPILER_WRITE_CODEC},
                {REFLECTION_READ_CODEC, REFLECTION_WRITE_CODEC}
        };
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeBinaryProtocol(ThriftCodec<TypeSignatureParameterUnion> readCodec, ThriftCodec<TypeSignatureParameterUnion> writeCodec)
            throws Exception
    {
        TypeSignatureParameterUnion typeSignatureParameterUnion = getRoundTripSerialize(readCodec, writeCodec, TBinaryProtocol::new);
        assertSerde(typeSignatureParameterUnion);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTCompactProtocol(ThriftCodec<TypeSignatureParameterUnion> readCodec, ThriftCodec<TypeSignatureParameterUnion> writeCodec)
            throws Exception
    {
        TypeSignatureParameterUnion typeSignatureParameterUnion = getRoundTripSerialize(readCodec, writeCodec, TCompactProtocol::new);
        assertSerde(typeSignatureParameterUnion);
    }

    @Test(dataProvider = "codecCombinations")
    public void testRoundTripSerializeTFacebookCompactProtocol(ThriftCodec<TypeSignatureParameterUnion> readCodec, ThriftCodec<TypeSignatureParameterUnion> writeCodec)
            throws Exception
    {
        TypeSignatureParameterUnion typeSignatureParameterUnion = getRoundTripSerialize(readCodec, writeCodec, TFacebookCompactProtocol::new);
        assertSerde(typeSignatureParameterUnion);
    }

    private TypeSignatureParameterUnion getRoundTripSerialize(ThriftCodec<TypeSignatureParameterUnion> readCodec, ThriftCodec<TypeSignatureParameterUnion> writeCodec, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(typeSignatureParameterUnion, protocol);
        return readCodec.read(protocol);
    }

    private void assertSerde(TypeSignatureParameterUnion typeSignatureParameterUnion)
    {
        TypeSignature typeSignature = typeSignatureParameterUnion.getTypeSignature();
        assertEquals(typeSignature, FAKE_TYPE_SIGNATURE);
    }
}
