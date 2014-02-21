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
package com.facebook.presto.ml;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.String.format;

public final class ModelUtils
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    private static final int VERSION_OFFSET = 0;
    private static final int HASH_OFFSET = VERSION_OFFSET + SIZE_OF_INT;
    private static final int ALGORITHM_OFFSET = HASH_OFFSET + 32;
    private static final int HYPERPARAMETER_LENGTH_OFFSET = ALGORITHM_OFFSET + SIZE_OF_INT;
    private static final int HYPERPARAMETERS_OFFSET = HYPERPARAMETER_LENGTH_OFFSET +  SIZE_OF_INT;

    private static final int CURRENT_FORMAT_VERSION = 1;

    // These ids are serialized to disk. Do not change them.
    @VisibleForTesting
    static final BiMap<? extends Class<? extends Model>, Integer> MODEL_SERIALIZATION_IDS = ImmutableBiMap.of(
            SvmClassifier.class, 1,
            SvmRegressor.class, 2,
            FeatureVectorUnitNormalizer.class, 3,
            ClassifierFeatureTransformer.class, 4,
            RegressorFeatureTransformer.class, 5);

    private ModelUtils()
    {
    }

    /**
     * Serializes the model using the following format
     * int: format version
     * byte[32]: SHA256 hash of all following data
     * int: id of algorithm
     * int: length of hyperparameters section
     * byte[]: hyperparameters (currently not used)
     * long: length of data section
     * byte[]: model data
     *
     * note: all multibyte values are in little endian
     */
    public static Slice serialize(Model model)
    {
        checkNotNull(model, "model is null");
        Integer id = MODEL_SERIALIZATION_IDS.get(model.getClass());
        checkNotNull(id, "id is null");
        int size = HYPERPARAMETERS_OFFSET;

        // hyperparameters aren't implemented yet
        byte[] hyperparameters = new byte[0];
        size += hyperparameters.length;

        int dataLengthOffset = size;
        size += SIZE_OF_LONG;
        int dataOffset = size;
        byte[] data = model.getSerializedData();
        size += data.length;

        Slice slice = Slices.allocate(size);
        slice.setInt(VERSION_OFFSET, CURRENT_FORMAT_VERSION);
        slice.setInt(ALGORITHM_OFFSET, id);
        slice.setInt(HYPERPARAMETER_LENGTH_OFFSET, hyperparameters.length);
        slice.setBytes(HYPERPARAMETERS_OFFSET, hyperparameters);
        slice.setLong(dataLengthOffset, data.length);
        slice.setBytes(dataOffset, data);

        byte[] modelHash = Hashing.sha256().hashBytes(slice.getBytes(ALGORITHM_OFFSET, slice.length() - ALGORITHM_OFFSET)).asBytes();
        checkState(modelHash.length == 32, "sha256 hash code expected to be 32 bytes");
        slice.setBytes(HASH_OFFSET, modelHash);

        return slice;
    }

    public static HashCode modelHash(Slice slice)
    {
        return HashCode.fromBytes(slice.getBytes(HASH_OFFSET, 32));
    }

    public static Model deserialize(byte[] data)
    {
        return deserialize(Slices.wrappedBuffer(data));
    }

    public static Model deserialize(Slice slice)
    {
        int version = slice.getInt(VERSION_OFFSET);
        checkArgument(version == CURRENT_FORMAT_VERSION, format("Unsupported version: %d", version));

        byte[] modelHashBytes = slice.getBytes(HASH_OFFSET, 32);
        HashCode expectedHash = HashCode.fromBytes(modelHashBytes);
        HashCode actualHash = Hashing.sha256().hashBytes(slice.getBytes(ALGORITHM_OFFSET, slice.length() - ALGORITHM_OFFSET));
        checkArgument(actualHash.equals(expectedHash), "model hash does not match data");

        int id = slice.getInt(ALGORITHM_OFFSET);
        Class<? extends Model> algorithm = MODEL_SERIALIZATION_IDS.inverse().get(id);
        checkNotNull(algorithm, "Unsupported algorith %d", id);

        int hyperparameterLength = slice.getInt(HYPERPARAMETER_LENGTH_OFFSET);

        byte[] hyperparameterBytes = slice.getBytes(HYPERPARAMETERS_OFFSET, hyperparameterLength);

        int dataLengthOffset = HYPERPARAMETERS_OFFSET + hyperparameterLength;
        long dataLength = slice.getLong(dataLengthOffset);

        int dataOffset = dataLengthOffset + SIZE_OF_LONG;
        byte[] data = slice.getBytes(dataOffset, (int) dataLength);

        try {
            Method deserialize = algorithm.getMethod("deserialize", byte[].class);
            return (Model) deserialize.invoke(null, new Object[] {data});
        }
        catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    public static byte[] serializeModels(Model...models)
    {
        List<byte[]> serializedModels = new ArrayList<>();
        int size = SIZE_OF_INT + SIZE_OF_INT * models.length;

        for (Model model : models) {
            byte[] bytes = serialize(model).getBytes();
            size += bytes.length;
            serializedModels.add(bytes);
        }

        Slice slice = Slices.allocate(size);
        slice.setInt(0, models.length);
        for (int i = 0; i < models.length; i++) {
            slice.setInt(SIZE_OF_INT * (i + 1), serializedModels.get(i).length);
        }

        int offset = SIZE_OF_INT + SIZE_OF_INT * models.length;
        for (byte[] bytes : serializedModels) {
            slice.setBytes(offset, bytes);
            offset += bytes.length;
        }

        return slice.getBytes();
    }

    public static List<Model> deserializeModels(byte[] bytes)
    {
        Slice slice = Slices.wrappedBuffer(bytes);
        int numModels = slice.getInt(0);

        int offset = SIZE_OF_INT + SIZE_OF_INT * numModels;
        ImmutableList.Builder<Model> models = ImmutableList.builder();
        for (int i = 0; i < numModels; i++) {
            int length = slice.getInt(SIZE_OF_INT * (i + 1));
            models.add(deserialize(slice.getBytes(offset, length)));
            offset += length;
        }

        return models.build();
    }

    //TODO: instead of having this function, we should add feature extractors that extend Model and extract features from Strings
    public static FeatureVector jsonToFeatures(Slice json)
    {
        Map<Integer, Double> features = new HashMap<>();
        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new RuntimeException("Bad row. Expected a json object");
            }

            while (true) {
                JsonToken token = parser.nextValue();
                if (token == null) {
                    throw new RuntimeException("Bad row. Expected a json object");
                }
                if (token == JsonToken.END_OBJECT) {
                    break;
                }
                int key = Integer.parseInt(parser.getCurrentName());
                double value = parser.getDoubleValue();
                features.put(key, value);
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        catch (RuntimeException e) {
            throw new RuntimeException(format("Bad features: %s", json.toStringUtf8()), e);
        }

        return new FeatureVector(features);
    }
}
