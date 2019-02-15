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
package com.facebook.presto.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class Employee
{
    private String name;
    private double salary;
    private boolean isEmployee;
    private Optional<String> address;

    static void validatePersonJsonCodec(JsonCodec<Employee> jsonCodec)
    {
        // create object with null address
        Employee expected = new Employee().setName("John").setEmployee(true);

        String json = jsonCodec.toJson(expected);
        assertFalse(json.contains("address"));
        assertEquals(jsonCodec.fromJson(json), expected);

        byte[] bytes = jsonCodec.toJsonBytes(expected);
        assertEquals(jsonCodec.fromJson(bytes), expected);

        // create object with missing address
        expected.setAddress(Optional.empty());

        json = jsonCodec.toJson(expected);
        assertFalse(json.contains("address"));
        Employee fromJson = jsonCodec.fromJson(json);
        assertNull(fromJson.getAddress());
        fromJson.setAddress(Optional.empty());
        assertEquals(fromJson, expected);

        // create object with present address
        expected.setAddress(Optional.of("address1"));

        json = jsonCodec.toJson(expected);
        assertTrue(json.contains("address"));
        assertEquals(jsonCodec.fromJson(json), expected);

        bytes = jsonCodec.toJsonBytes(expected);
        assertEquals(jsonCodec.fromJson(bytes), expected);
    }

    static void validatePersonListJsonCodec(JsonCodec<List<Employee>> jsonCodec)
    {
        ImmutableList<Employee> expected = ImmutableList.of(
                new Employee().setName("John").setEmployee(true),
                new Employee().setName("Mark").setEmployee(true),
                new Employee().setName("Tom").setEmployee(false));

        String json = jsonCodec.toJson(expected);
        assertEquals(jsonCodec.fromJson(json), expected);

        byte[] bytes = jsonCodec.toJsonBytes(expected);
        assertEquals(jsonCodec.fromJson(bytes), expected);
    }

    static void validatePersonMapJsonCodec(JsonCodec<Map<String, Employee>> jsonCodec)
    {
        ImmutableMap<String, Employee> expected = ImmutableMap.<String, Employee>builder()
                .put("John", new Employee().setName("John").setEmployee(true))
                .put("Mark", new Employee().setName("Mark").setEmployee(true))
                .put("Tom", new Employee().setName("Tom").setEmployee(false))
                .build();

        String json = jsonCodec.toJson(expected);
        assertEquals(jsonCodec.fromJson(json), expected);

        byte[] bytes = jsonCodec.toJsonBytes(expected);
        assertEquals(jsonCodec.fromJson(bytes), expected);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Employee setName(String name)
    {
        this.name = name;
        return this;
    }

    @JsonProperty
    public double getSalary()
    {
        return salary;
    }

    @JsonProperty
    public Employee setSalary(double salary)
    {
        this.salary = salary;
        return this;
    }

    @JsonProperty
    public boolean isEmployee()
    {
        return isEmployee;
    }

    @JsonProperty
    public Employee setEmployee(boolean employee)
    {
        isEmployee = employee;
        return this;
    }

    @JsonProperty
    public Optional<String> getAddress()
    {
        return address;
    }

    @JsonProperty
    public Employee setAddress(Optional<String> address)
    {
        this.address = address;
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Employee employee = (Employee) o;
        return Double.compare(employee.salary, salary) == 0 &&
                isEmployee == employee.isEmployee &&
                Objects.equals(name, employee.name) &&
                Objects.equals(address, employee.address);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, salary, isEmployee, address);
    }
}
