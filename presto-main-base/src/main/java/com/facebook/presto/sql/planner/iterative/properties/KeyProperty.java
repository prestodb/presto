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

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.iterative.properties.Key.concatKeys;
import static com.facebook.presto.sql.planner.iterative.properties.Key.getNormalizedKey;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Represents a collection of primary or unique key constraints that hold for a final or
 * intermediate result set produced by a PlanNode.
 */
public final class KeyProperty
{
    private final Set<Key> keys;

    public KeyProperty()
    {
        this.keys = ImmutableSet.of();
    }

    public KeyProperty(Set<Key> keys)
    {
        this.keys = ImmutableSet.copyOf(requireNonNull(keys, "keys is null"));
    }

    public KeyProperty(KeyProperty keyProperty)
    {
        this.keys = keyProperty.getKeys();
    }

    public Set<Key> getKeys()
    {
        return keys;
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
        return ((keys.isEmpty() && otherKeyProperty.keys.isEmpty()) ||
                (otherKeyProperty.keys.stream().allMatch(this::satisfiesKeyRequirement)));
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
        return keys.stream().anyMatch(k -> k.keySatisifiesRequirement(keyRequirement));
    }

    /**
     * Adds the keys from the provided key property to this key property.
     *
     * @param thisKeyProperty
     * @param otherKeyProperty
     */
    public static KeyProperty combineKeys(KeyProperty thisKeyProperty, KeyProperty otherKeyProperty)
    {
        return combineKeys(thisKeyProperty.keys, otherKeyProperty.keys);
    }

    /**
     * Adds otherKeys to thisKeys.
     *
     * @param thisKeys
     * @param otherKeys
     */
    public static KeyProperty combineKeys(Set<Key> thisKeys, Set<Key> otherKeys)
    {
        Set<Key> newKeys = new HashSet(thisKeys);
        for (Key key : otherKeys) {
            newKeys = combineKey(newKeys, key);
        }
        return new KeyProperty(newKeys);
    }

    /**
     * Adds a newKey to keys while enforcing the constraint that no
     * key is redundant with respect to another.
     * E.g. if {orderkey} was an existing key then the key {orderkey, orderpriority}
     * would represent a redundant key. The inverse is true, an existing key
     * can be removed by a new key it if it is redundant with respect to the newKey.
     *
     * @param keys
     * @param newKey
     */
    public static Set<Key> combineKey(Set<Key> keys, Key newKey)
    {
        Set<Key> combinedKeys = new HashSet<>(keys);
        for (Key key : keys) {
            //if the new key >= key don't add it
            if (key.keySatisifiesRequirement(newKey)) {
                return keys;
            }
            //if the new key <= key1 remove existing key. note that if this is true the new key will be added as it
            //cannot be a superset of another key2 otherwise key2 <= key1 which violates the key property invariant
            if (newKey.keySatisifiesRequirement(key)) {
                combinedKeys.remove(key);
            }
        }
        combinedKeys.add(newKey);
        return combinedKeys;
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
    public static Optional<KeyProperty> getNormalizedKeyProperty(KeyProperty keyProperty, EquivalenceClassProperty equivalenceClassProperty)
    {
        Set<Key> newKeys = new HashSet<>();
        for (Key key : keyProperty.keys) {
            Optional<Key> normalizedKey = getNormalizedKey(key, equivalenceClassProperty);
            if (!normalizedKey.isPresent()) {
                return Optional.empty();
            }
            else {
                newKeys = combineKey(newKeys, normalizedKey.get());
            }
        }
        return Optional.of(new KeyProperty(newKeys));
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
        Set<Key> newKeys = new HashSet<>();

        for (Key key : keys) {
            Optional<Key> projectedKey = key.project(inverseVariableMappings);
            if (projectedKey.isPresent()) {
                newKeys = combineKey(newKeys, projectedKey.get());
            }
        }
        return new KeyProperty(newKeys);
    }

    /**
     * Returns a version of this key property wherein each key is concatenated with all keys in the provided key property
     * A concatenated key property results from a join operation where concatenated keys of the left and
     * right join inputs form unique constraints on the join result.
     *
     * @param toConcatKeyProp
     * @return a version of this key concatenated with the provided key.
     */
    public static KeyProperty concatKeyProperty(KeyProperty thisKeyProperty, KeyProperty toConcatKeyProp)
    {
        Set<Key> newKeys = new HashSet<>();

        for (Key thisKey : thisKeyProperty.keys) {
            for (Key toConcatKey : toConcatKeyProp.keys) {
                newKeys = combineKey(newKeys, concatKeys(thisKey, toConcatKey));
            }
        }
        return new KeyProperty(newKeys);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("keys", keys.stream().map(Key::toString).collect(Collectors.joining(",")))
                .toString();
    }
}
