package com.facebook.presto.operator.aggregation.state;


import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestTopElementsHistogram {

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
        assertEquals(histogram.getTopElements().keySet(), new HashSet<Character>(Arrays.asList('r','s','t','u','v')));
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
        System.out.println(histogram.getTopElements());
    }

}
