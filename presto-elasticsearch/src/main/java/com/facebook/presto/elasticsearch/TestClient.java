
package com.facebook.presto.elasticsearch;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;


import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static io.airlift.json.JsonCodec.*;

//import org.json.JSONException;
//import org.json.JSONObject;

public class TestClient {



    public static void main( String[] args ) throws InterruptedException, IOException, ExecutionException {

        //getSpecificFields();
        //getJsonKeys_IndexType();


        System.out.println("Hello World!");
    }


}
