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
package com.facebook.presto.sql.planner.iterative.properties;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Represents a collection of primary or unique key constraints that hold for a final or
 * intermediate result set produced by a PlanNode.
 */
public class KeyProperty
{
    private final Set<Key> keys;

    public KeyProperty()
    {
        this.keys = new HashSet<>();
    }

    public KeyProperty(Set<Key> keys)
    {
        this.keys = keys;
    }

    /**
     * Determines if one key property is more general than another.
     * A key property is more general than another if it can satisfy any key requirement the other can satisfy.
     *
     * @param otherKeyProperty
     * @return True keyProperty is more general than otherKeyProperty or False otherwise.
     */
    public boolean moreGeneral(KeyProperty otherKeyProperty)
    {
        requireNonNull(otherKeyProperty, "otherKeyProperty is null");
        return ((keys.isEmpty() && otherKeyProperty.keys.isEmpty()) ||
                (otherKeyProperty.keys.stream().allMatch(k -> satisfiesKeyRequirement(k))));
    }

    /**
     * Determines if this key property satisfies a key requirement.
     * This is true if any of the keys in the collection satisfies the key requirement.
     *
     * @param keyRequirement
     * @return True if keyRequirement is satisfied by this key property or False otherwise.
     */
    public boolean satisfiesKeyRequirement(Key keyRequirement)
    {
        requireNonNull(keyRequirement, "keyRequirement is null");
        return keys.stream().anyMatch(k -> k.keySatisifiesRequirement(keyRequirement));
    }

    /**
     * Adds a set of keys to this key property.
     *
     * @param keys
     */
    public void addKeys(Set<Key> keys)
    {
        requireNonNull(keys, "keys is null");
        keys.stream().forEach(k -> addKey(k));
    }

    /**
     * Adds the keys from the provided key property to this key property.
     *
     * @param keyProperty
     */
    public void addKeys(KeyProperty keyProperty)
    {
        requireNonNull(keyProperty, "keyProperty is null");
        addKeys(keyProperty.keys);
    }

    /**
     * Adds a new key to this key property.
     *
     * @param key
     */
    public void addKey(Key key)
    {
        requireNonNull(key, "key is null");
        addNonRedundantKey(key);
    }

    /**
     * Adds a key to this key property while enforcing the constraint that no
     * key is redundant with respect to another.
     * E.g. if {orderkey} was an existing key then the key {orderkey, orderpriority}
     * would represent a redundant key. The inverse is true, an existing key
     * can be removed by a new key it if is redundant with respect to the new key.
     *
     * @param newKey
     */
    private void addNonRedundantKey(Key newKey)
    {
        requireNonNull(newKey, "newKey is null");
        Set<Key> removedKeys = new HashSet<>();
        for (Key key : keys) {
            //if the new key >= key don't add it
            if (key.keySatisifiesRequirement(newKey)) {
                return;
            }

            //if the new key <= key1 remove existing key. note that if this is true the new key will be added as it
            //cannot be a superset of another key2 otherwise key2 <= key1 which violates the key property invariant
            if (newKey.keySatisifiesRequirement(key)) {
                removedKeys.add(key);
            }
        }
        //new key not >= existing key
        keys.add(newKey);
        keys.removeAll(removedKeys);
    }

    /**
     * Reduces key property to a concise cannonical form wherein each individual key is
     * reduced to a canonical form by removing redundant variables and replacing any remaining variables
     * with their equivalence class heads. Moreover, no keys in the normalized key
     * property are redundant with respect to the others.
     * Note that if any key is fully bound to constants an empty result is
     * returned, signaling that at most a single record is in the result set constrained
     * by this key property.
     *
     * @param equivalenceClassProperty
     * @return A normalized version of this key property or empty if any key is fully bound to constants.
     */
    public Optional<KeyProperty> normalize(EquivalenceClassProperty equivalenceClassProperty)
    {
        requireNonNull(equivalenceClassProperty, "equivalenceClassProperty is null");
        KeyProperty result = new KeyProperty();
        for (Key key : this.keys) {
            Optional<Key> normalizedKey = key.normalize(equivalenceClassProperty);
            if (!normalizedKey.isPresent()) {
                return Optional.empty();
            }
            else {
                result.addKey(normalizedKey.get());
            }
        }
        return Optional.of(result);
    }

    /**
     * Returns a projected version of this key property.
     * Variables in each key are mapped to output variables in the context beyond the project operation.
     * It is possible that this operation projects all keys from the key property.
     *
     * @param inverseVariableMappings
     * @return A projected version of this key property.
     */
    public KeyProperty project(LogicalPropertiesImpl.InverseVariableMappingsWithEquivalence inverseVariableMappings)
    {
        requireNonNull(inverseVariableMappings, "inverseVariableMappings is null");
        KeyProperty result = new KeyProperty();
        keys.stream().forEach(key -> {
            Optional<Key> projectedKey = key.project(inverseVariableMappings);
            if (projectedKey.isPresent()) {
                result.addKey(projectedKey.get());
            }
        });
        return result;
    }

    /**
     * Empties all keys from the key property.
     */
    public void empty()
    {
        keys.clear();
    }

    /**
     * Returns a version of this key property wherein each key is concatenated with all keys in the provided key property
     * A concatenated key property results from a join operation where concatenated keys of the left and
     * right join inputs form unique constraints on the join result.
     *
     * @param toConcatKeyProp
     * @return a version of this key concatenated with the provided key.
     */
    public KeyProperty concat(KeyProperty toConcatKeyProp)
    {
        requireNonNull(toConcatKeyProp, "toConcatKeyProp is null");
        KeyProperty result = new KeyProperty();
        for (Key thisKey : this.keys) {
            for (Key toConcatKey : toConcatKeyProp.keys) {
                result.addKey(thisKey.concat(toConcatKey));
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("keys", String.join(",", keys.stream().map(Key::toString).collect(Collectors.toList())))
                .toString();
    }
}
