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
package com.facebook.presto.server.smile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.json.JsonBinder.jsonBinder;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestSmileSupport
{
    private static final Car CAR = new Car()
            .setMake("BMW")
            .setModel("M3")
            .setYear(2011)
            .setPurchased(new DateTime().withZone(UTC))
            .setNotes("sweet!")
            .setNameList(superDuper("d*a*i*n"));

    private ObjectMapper objectMapper;

    @BeforeClass
    public void setUp()
    {
        Injector injector = Guice.createInjector(new SmileModule(),
                binder -> {
                    jsonBinder(binder).addSerializerBinding(SuperDuperNameList.class).toInstance(ToStringSerializer.instance);
                    jsonBinder(binder).addDeserializerBinding(SuperDuperNameList.class).to(SuperDuperNameListDeserializer.class);
                });
        objectMapper = injector.getInstance(Key.get(ObjectMapper.class, ForSmile.class));
    }

    @Test
    public void testSmileCodecFactoryBinding()
    {
        Injector injector = Guice.createInjector(new SmileModule());
        SmileCodecFactory codecFactory = injector.getInstance(SmileCodecFactory.class);
        SmileCodec<Person> personJsonCodec = codecFactory.smileCodec(Person.class);
        Person person = new Person();
        person.setName("name");
        person.setLastName(Optional.of("lastname"));
        Person actual = personJsonCodec.fromSmile(personJsonCodec.toBytes(person));
        assertEquals(actual, person);
    }

    @Test
    public void testObjectMapper()
            throws IOException
    {
        byte[] bytes = objectMapper.writeValueAsBytes(CAR);
        Car car = objectMapper.readValue(bytes, CAR.getClass());
        assertEquals(car, CAR);
    }

    @Test
    public void testFieldDetection()
            throws Exception
    {
        Map<String, Object> actual = createCarMap();

        // notes is not annotated so should not be included
        // color is null so should not be included
        assertEquals(actual.keySet(), ImmutableSet.of("make", "model", "year", "purchased", "nameList"));
    }

    @Test
    public void testDateTimeRendered()
            throws Exception
    {
        Map<String, Object> actual = createCarMap();

        assertEquals(actual.get("purchased"), ISODateTimeFormat.dateTime().print(CAR.getPurchased()));
    }

    @Test
    public void testGuavaRoundTrip()
            throws Exception
    {
        List<Integer> list = ImmutableList.of(3, 5, 8);

        byte[] json = objectMapper.writeValueAsBytes(list);
        List<Integer> actual = objectMapper.readValue(json, new TypeReference<ImmutableList<Integer>>() {});

        assertEquals(actual, list);
    }

    @Test
    public void testIgnoreUnknownFields()
            throws Exception
    {
        Map<String, Object> data = new HashMap<>(createCarMap());

        // add an unknown field
        data.put("unknown", "bogus");

        // Jackson should deserialize the object correctly with the extra unknown data
        assertEquals(objectMapper.readValue(objectMapper.writeValueAsBytes(data), Car.class), CAR);
    }

    @Test
    public void testPropertyNamesFromParameterNames()
            throws Exception
    {
        NoJsonPropertiesInJsonCreator value = new NoJsonPropertiesInJsonCreator("first value", "second value");
        NoJsonPropertiesInJsonCreator mapped = objectMapper.readValue(objectMapper.writeValueAsBytes(value), NoJsonPropertiesInJsonCreator.class);
        assertEquals(mapped.getFirst(), "first value");
        assertEquals(mapped.getSecond(), "second value");
    }

    @Test
    public void testJsonValueAndStaticFactoryMethod()
            throws Exception
    {
        JsonValueAndStaticFactoryMethod value = JsonValueAndStaticFactoryMethod.valueOf("some value");
        JsonValueAndStaticFactoryMethod mapped = objectMapper.readValue(objectMapper.writeValueAsBytes(value), JsonValueAndStaticFactoryMethod.class);
        assertEquals(mapped.getValue(), "some value");
    }

    private Map<String, Object> createCarMap()
            throws IOException
    {
        return objectMapper.readValue(objectMapper.writeValueAsBytes(CAR), new TypeReference<Object>() {});
    }

    /**
     * Classes below are copied from airlift as is. Once smile support is released in airlift
     * smile support in Presto should be entirely removed.
     */
    public static class Car
    {
        // These fields are public to make sure that Jackson is ignoring them
        public String make;
        public String model;
        public int year;
        public DateTime purchased;

        // property that will be null to verify that null fields are not rendered
        public String color;

        // non-json property to verify that auto-detection is disabled
        public String notes;

        // property that requires special serializer and deserializer
        public SuperDuperNameList nameList;

        @JsonProperty
        public String getMake()
        {
            return make;
        }

        @JsonProperty
        public Car setMake(String make)
        {
            this.make = make;
            return this;
        }

        @JsonProperty
        public String getModel()
        {
            return model;
        }

        @JsonProperty
        public Car setModel(String model)
        {
            this.model = model;
            return this;
        }

        @JsonProperty
        public int getYear()
        {
            return year;
        }

        @JsonProperty
        public Car setYear(int year)
        {
            this.year = year;
            return this;
        }

        @JsonProperty
        public DateTime getPurchased()
        {
            return purchased;
        }

        @JsonProperty
        public Car setPurchased(DateTime purchased)
        {
            this.purchased = purchased;
            return this;
        }

        @JsonProperty
        public String getColor()
        {
            return color;
        }

        @JsonProperty
        public Car setColor(String color)
        {
            this.color = color;
            return this;
        }

        @JsonProperty
        public SuperDuperNameList getNameList()
        {
            return nameList;
        }

        @JsonProperty
        public Car setNameList(SuperDuperNameList nameList)
        {
            this.nameList = nameList;
            return this;
        }

        // this field should not be written

        public String getNotes()
        {
            return notes;
        }

        public Car setNotes(String notes)
        {
            this.notes = notes;
            return this;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Car)) {
                return false;
            }

            Car car = (Car) o;

            if (year != car.year) {
                return false;
            }
            if (color != null ? !color.equals(car.color) : car.color != null) {
                return false;
            }
            if (make != null ? !make.equals(car.make) : car.make != null) {
                return false;
            }
            if (model != null ? !model.equals(car.model) : car.model != null) {
                return false;
            }
            if (nameList != null ? !nameList.equals(car.nameList) : car.nameList != null) {
                return false;
            }
            if (purchased != null ? !purchased.equals(car.purchased) : car.purchased != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = make != null ? make.hashCode() : 0;
            result = 31 * result + (model != null ? model.hashCode() : 0);
            result = 31 * result + year;
            result = 31 * result + (purchased != null ? purchased.hashCode() : 0);
            result = 31 * result + (color != null ? color.hashCode() : 0);
            result = 31 * result + (nameList != null ? nameList.hashCode() : 0);
            return result;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("make", make)
                    .add("model", model)
                    .add("year", year)
                    .add("purchased", purchased)
                    .add("color", color)
                    .add("notes", notes)
                    .add("nameList", nameList)
                    .toString();
        }
    }

    public static class NoJsonPropertiesInJsonCreator
    {
        private final String first;
        private final String second;

        @JsonCreator
        public NoJsonPropertiesInJsonCreator(/* no @JsonProperty here*/ String first, String second)
        {
            this.first = first;
            this.second = second;
        }

        @JsonProperty
        public String getFirst()
        {
            return first;
        }

        @JsonProperty
        public String getSecond()
        {
            return second;
        }
    }

    public static class JsonValueAndStaticFactoryMethod
    {
        private final String value;

        @JsonCreator
        public static JsonValueAndStaticFactoryMethod valueOf(String value)
        {
            return new JsonValueAndStaticFactoryMethod(value);
        }

        private JsonValueAndStaticFactoryMethod(String value)
        {
            this.value = requireNonNull(value, "value is null");
        }

        @JsonValue
        public String getValue()
        {
            return value;
        }
    }

    public static class SuperDuperNameList
    {
        private List<String> name;

        private SuperDuperNameList(String superDuperNameList)
        {
            this(superDuperNameList, null);
        }

        private SuperDuperNameList(String superDuperNameList, Object stopJacksonFromUsingStringConstructor)
        {
            this.name = ImmutableList.copyOf(Splitter.on('*').split(superDuperNameList));
        }

        public List<String> getName()
        {
            return name;
        }

        @Override
        public String toString()
        {
            return Joiner.on("*").join(name);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SuperDuperNameList)) {
                return false;
            }

            SuperDuperNameList that = (SuperDuperNameList) o;

            if (!name.equals(that.name)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return name.hashCode();
        }
    }

    public static final class SuperDuperNameListDeserializer
            extends StdScalarDeserializer<SuperDuperNameList>
    {
        public SuperDuperNameListDeserializer()
        {
            super(SuperDuperNameList.class);
        }

        @Override
        public SuperDuperNameList deserialize(JsonParser jp, DeserializationContext context)
                throws IOException
        {
            JsonToken token = jp.getCurrentToken();
            if (token == JsonToken.VALUE_STRING) {
                return new SuperDuperNameList(jp.getText(), null);
            }
            context.handleUnexpectedToken(handledType(), jp);
            throw JsonMappingException.from(jp, null);
        }
    }

    public static SuperDuperNameList superDuper(String superDuperNameList)
    {
        return new SuperDuperNameList(superDuperNameList, null);
    }
}
