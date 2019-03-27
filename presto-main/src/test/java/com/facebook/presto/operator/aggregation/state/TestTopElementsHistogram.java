package com.facebook.presto.operator.aggregation.state;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.tools.javac.util.List;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestTopElementsHistogram {

    @Test
    public void testNull() {
        TopElementsHistogram<String> histogram = new TopElementsHistogram<>(40, 0.01, 0.99, 1);
        histogram.add(Arrays.asList(null,"b",null,"a",null,"b"));
        assertEquals(histogram.getTopElements(), ImmutableMap.of("b", 2L));
    }

    @Test
    public void testSimple() {
        TopElementsHistogram<String> histogram = new TopElementsHistogram<>(30, 0.01, 0.99, 1);
        histogram.add(Arrays.asList("a","b","c","a","a","b"));
        assertEquals(histogram.getTopElements(), ImmutableMap.of("a", 3L, "b", 2L));
    }

    @Test
    public void testTopElements_5() {
        TopElementsHistogram<Character> histogram = new TopElementsHistogram<Character>(5, 0.01, 0.99, 1);
        populate_0(histogram);
        assertEquals(histogram.getTopElements().keySet(), new HashSet<Character>(Arrays.asList('m','n','o','p','q','r','s','t','u','v')));
    }

    @Test
    public void testTopElements_7() {
        TopElementsHistogram<Character> histogram = new TopElementsHistogram<Character>(7, 0.01, 0.99, 1);
        populate_0(histogram);
        assertEquals(histogram.getTopElements(), ImmutableMap.of('r', 175L, 's', 185L, 't', 195L, 'u', 205L, 'v', 215L));
    }

    public static void populate_0(TopElementsHistogram<Character> histogram){
        histogram.add('a', 5);
        histogram.add('b', 15);
        histogram.add('c', 25);
        histogram.add('d', 35);
        histogram.add('e', 45);
        histogram.add('f', 55);
        histogram.add('g', 65);
        histogram.add('h', 75);
        histogram.add('i', 85);
        histogram.add('j', 95);
        histogram.add('k', 105);
        histogram.add('l', 115);
        histogram.add('m', 125);
        histogram.add('n', 135);
        histogram.add('o', 145);
        histogram.add('p', 155);
        histogram.add('q', 165);
        histogram.add('r', 175);
        histogram.add('s', 185);
        histogram.add('t', 195);
        histogram.add('u', 205);
        histogram.add('v', 215);
    }

    @Test
    public void testFalsePositive(){
        TopElementsHistogram<String> histogram = new TopElementsHistogram<String>(1.5, 0.01, 0.99, 1);
        histogram.add("a", 2);
        histogram.add("b", 3);
        histogram.add("c", 5);

        for(int i=0; i<190; i++){
            String s= RandomStringUtils.randomAlphabetic(5);
            histogram.add(s);
        }

        assertEquals(histogram.getRowsProcessed(), 200);
        assertNull(histogram.getTopElements().get("a"));  //a should not make it to top elements
        assertEquals((long)histogram.getTopElements().get("b"), 3);
        assertEquals((long)histogram.getTopElements().get("c"), 5);
    }

    @Test
    public void testSerialize(){
        TopElementsHistogram<Character> in = new TopElementsHistogram<Character>(5, 0.01, 0.99, 1);
        populate_0(in);
        TopElementsHistogram<Character> out = new TopElementsHistogram(in.serialize());
        System.out.println(in.getTopElements().toString());
        System.out.println(out.getTopElements().toString());
        //TODO why assert fails when "toString" is removed?
        assertEquals(in.getTopElements().toString(), out.getTopElements().toString());
    }
}
