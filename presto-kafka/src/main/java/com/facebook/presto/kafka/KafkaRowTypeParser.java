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

package com.facebook.presto.kafka;

import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class KafkaRowTypeParser
{
    @Inject
    private KafkaRowTypeParser() {}

    public static List<String> names = new ArrayList<>();

    public static List<String> typeStrings = new ArrayList<>();

    private static String rowTypePattern = "ROW<(.*?)>\\((.*?)\\)";

    private static Pattern p = Pattern.compile(rowTypePattern);   // the pattern to search for

    /**
     * Utils class to build a new RowType (if not existing) based on parameters
     * @param rowTypeString A string that describes parameter names and types.
     *                      The string is provided by kafka_topic.json file.
     * @param typeManager A build-in class that manages Standard types
     * @return A new RowType built from rowTypeString
     */
    public static Type getNestedRowType(String rowTypeString, TypeManager typeManager)
    {
        if (!rowTypeString.matches(rowTypePattern)) {
            return typeManager.getType(parseTypeSignature(rowTypeString));
        }

        parse(rowTypeString);

        List<RowType.Field> fields = new ArrayList<>();

        ListIterator<String> it = typeStrings.listIterator();
        while (it.hasNext()) {
            int ind = it.nextIndex();
            Type fieldType = getNestedRowType(it.next(), typeManager);
            RowType.Field newField = new RowType.Field(Optional.of(names.get(ind)), fieldType);
            fields.add(newField);
        }

        Type newType = RowType.from(fields);

        return newType;
    }

    public static void parse(String typeString)
    {
        Matcher m = p.matcher(typeString);
        checkArgument(m.find(), String.format("Wrong RowType string format: %s", typeString));
        typeStrings = parseParamsString(m.group(1));
        names = Arrays.asList(m.group(2).replaceAll("[\\s']", "").split(","));
        requireNonNull(typeStrings, "Parameter types missing");
        requireNonNull(names, "Parameter names missing");
    }

    /**
     * Parse nested parameters string to a list
     * @param parameterString A string of nested parameters extracted from RowType string
     * @return
     */
    public static List<String> parseParamsString(String parameterString)
    {
        List<String> parameters = new ArrayList<>();
        int parameterStart = 0;
        int bracketCount = 0;

        for (int i = 0; i < parameterString.length(); i++) {
            char c = parameterString.charAt(i);
            if (c == '<') {
                bracketCount++;
            }
            else if (c == '>') {
                bracketCount--;
            }
            else if (c == ',') {
                if (bracketCount == 0) {
                    parameters.add(parameterString.substring(parameterStart, i));
                    parameterStart = i + 1;
                }
            }
            else if (i == (parameterString.length() - 1)) {
                checkArgument(bracketCount == 0, String.format("Wrong brackets in %s", parameterString));
                parameters.add(parameterString.substring(parameterStart));
            }
        }
        return parameters;
    }
}
