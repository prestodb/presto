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
package com.facebook.presto.spi.function;

import com.facebook.presto.common.Subfield;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SubfieldPathTransformationFunctions
{
    private SubfieldPathTransformationFunctions() {}
    /**
     * Adds <code>allSubscripts</code> on top of the path for every subfield in 'subfields'.
     *
     * @param subfields set of Subfield to transform
     * @return transformed copy of the input set of subfields with <code>allSubscripts</code>.
     */
    public static Set<Subfield> prependAllSubscripts(Set<Subfield> subfields)
    {
        return subfields.stream().map(subfield -> new Subfield(subfield.getRootName(),
                        Collections.unmodifiableList(
                                Stream.concat(
                                        Arrays.asList(Subfield.allSubscripts()).stream(),
                                        subfield.getPath().stream()).collect(Collectors.toList()))))
                .collect(Collectors.toSet());
    }

    /**
     * Transformation function that overrides all lambda subfields from outer functions with the single subfield with <code>allSubscripts</code> in its path.
     * Essentially, it instructs to include all subfields of the array element or map value. This function is most commonly used with the function that
     * returns the entire value from its input or accesses input values internally.
     *
     * @return one subfield with <code>allSubscripts</code> in its path.
     */
    public static Set<Subfield> allSubfieldsRequired(Set<Subfield> subfields)
    {
        if (subfields.isEmpty()) {
            return Collections.unmodifiableSet(Stream.of(new Subfield("", Arrays.asList(Subfield.allSubscripts()))).collect(Collectors.toSet()));
        }
        return subfields;
    }

    /**
     * Transformation function that removes any previously accessed subfields. This function is most commonly used with the function that do not return values from its input.
     *
     * @return empty set.
     */
    public static Set<Subfield> clearRequiredSubfields(Set<Subfield> ignored)
    {
        return Collections.emptySet();
    }

    /**
     * Removes the second path element from every subfield in 'subfields'.
     *
     * @param subfields set of Subfield to transform
     * @return transformed copy of the input set of subfields with removed the second path element.
     */
    public static Set<Subfield> removeSecondPathElement(Set<Subfield> subfields)
    {
        return subfields.stream().map(subfield -> new Subfield(subfield.getRootName(),
                        Collections.unmodifiableList(
                                Stream.concat(Arrays.asList(subfield.getPath().get(0)).stream(), subfield.getPath().stream().skip(2)).collect(Collectors.toList()))))
                .collect(Collectors.toSet());
    }
}
