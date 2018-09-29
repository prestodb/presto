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
package com.facebook.presto.kafkastream;

import com.google.common.base.Joiner;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import io.airlift.log.Logger;
import net.minidev.json.JSONArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Class is used to flatten the json data. This will only select the values
 * specified by jsonPaths.
 */
public final class JsonFlattener
{
    private static final Logger logger = Logger.get(JsonFlattener.class);
    private final String jsonName;
    private final List<String> jsonPaths;
    private CharSequence separator = ",";
    private static final Map<String, Map<String, Set<String>>> jsonPathMapCache = new ConcurrentHashMap<>();

    /**
     * @param jsonName name of the json which is used to cache map structure created using json paths.
     * @param jsonPaths list of json paths. @see <a href="https://github.com/json-path/JsonPath">this</a> for json path.
     */
    public JsonFlattener(String jsonName, List<String> jsonPaths)
    {
        requireNonNull(jsonPaths, "json path list should not be null");
        checkArgument(!jsonPaths.isEmpty(), "json path list should not be empty");
        this.jsonName = jsonName;
        this.jsonPaths = jsonPaths;
    }

    public JsonFlattener(String jsonName, List<String> jsonPaths, CharSequence separator)
    {
        this(jsonName, jsonPaths);
        this.separator = separator;
    }

    /**
     * Flattens json passed as argument. It will have values specified by {@link JsonFlattener#jsonPaths}.
     * {@link JsonFlattener#separator} will be used to separate the column values.
     *
     * @param json input json that needs to be flattened.
     * @return list of raw values.
     */
    public List<List<String>> flatten(String json)
    {
        if (json == null || json.trim().isEmpty()) {
            // return empty list
            return new ArrayList<>(0);
        }
        Map<String, Set<String>> objectMap = getJsonPathMap();
        Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);
        List<Record> finalRecords = new ArrayList<>();
        if (document instanceof JSONArray) {
            JSONArray array = (JSONArray) document;
            array.forEach((eachDocument) -> {
                Optional<List<Record>> records = flattenObject(eachDocument, objectMap);
                if (records.isPresent()) {
                    finalRecords.addAll(records.get());
                }
            });
        }
        else {
            Optional<List<Record>> records = flattenObject(document, objectMap);
            if (records.isPresent()) {
                finalRecords.addAll(records.get());
            }
        }
        finalRecords.forEach(Record::print);
        return finalRecords.stream().map((record -> record.toList(this.jsonPaths))).collect(Collectors
                .toList());
    }

    private Optional<List<Record>> flattenObject(Object document, Map<String, Set<String>> objectMap)
    {
        TreeNode rootNode = new TreeNode(document, "$", TreeNode.NodeType.OBJECT, "$");
        generateTree(objectMap, rootNode);
        return Optional.ofNullable(createRecords(rootNode));
    }

    /**
     * Get json path map from cache if present or creates a new json path ma. If json name is null
     * it will always create new one. It won't use caching.
     * <p>
     * Caching is added to avoid generating json path map for same set of json paths.
     */
    private Map<String, Set<String>> getJsonPathMap()
    {
        if (jsonName != null) {
            Map<String, Set<String>> jsonPathMap = jsonPathMapCache.get(jsonName);
            if (jsonPathMap != null) {
                return jsonPathMap;
            }
            else {
                return createJsonPathMap();
            }
        }
        else {
            return createJsonPathMap();
        }
    }

    /**
     * Creates map structure which will have key as the json path element and value as set of elements
     * which are under key according to json paths.
     * e.g for $.Id an $.Name will have $ as the key and set of {Id,Name} as the value.
     */
    private Map<String, Set<String>> createJsonPathMap()
    {
        logger.debug("Creating json path map for jsonName : %s", jsonName);
        Map<String, Set<String>> jsonPathMap = new HashMap<>();
        jsonPathMap.put("$", new LinkedHashSet<>());
        for (String path : jsonPaths) {
            String[] components = path.split("\\.(?![^\\(]*[\\)])");
            String parentComponent = "$";
            for (int i = 1; i < components.length; i++) {
                if (!jsonPathMap.containsKey(parentComponent)) {
                    Set<String> childSet = new LinkedHashSet<>();
                    childSet.add(components[i]);
                    jsonPathMap.put(parentComponent, childSet);
                }
                else {
                    jsonPathMap.get(parentComponent).add(components[i]);
                }
                parentComponent += ".".concat(components[i]);
            }
        }
        // If json name is null , json map won't use caching.
        if (jsonName != null) {
            jsonPathMapCache.putIfAbsent(jsonName, jsonPathMap);
        }
        logger.debug("JsonPath map : %s", jsonPathMap);
        return jsonPathMap;
    }

    /**
     * Generates the tree which maps to json structure which will be used then to traverse and generate the records.
     * Here, tree is n-ary tree which represents json. Tree node can be of 3 types which can be Object, array or any
     * primitive value.
     * <p>
     * If node is of type is Object, it represents JsonObject and it has corresponding JsonObject as value.
     * If node is of type array, it as list of JsonObjects as the value of the node where each JsonObject represent
     * each object in JsonArray.
     * If node is of type primitive, it should be leaf node and represents record of the flattened json.
     *
     * @param jsonPathMap jsonPathMap generated using {@link JsonFlattener#getJsonPathMap()} ()}
     * @param rootNode currently running node in tree.
     * @return number of records under the node.
     */
    private int generateTree(Map<String, Set<String>> jsonPathMap, TreeNode rootNode)
    {
        logger.debug("Generating tree for flattening json");
        Set<String> childNodeList = jsonPathMap.get(rootNode.getNodePath());

        int levelRecordCount = 1;
        if (childNodeList != null && !childNodeList.isEmpty()) {
            for (String childNodeName : childNodeList) {
                boolean isArray;
                Object value;
                // Process json path group. json path group is wrapped in ()
                if (childNodeName.startsWith("(") && childNodeName.endsWith(")")) {
                    String subJsonPaths = childNodeName.substring(1, childNodeName.length() - 1);
                    isArray = true;
                    value = processJsonPathGroup(rootNode, subJsonPaths);
                }
                else {
                    isArray = childNodeName.endsWith("[*]");
                    value = processSingleJsonPath(rootNode, childNodeName);
                }

                String childNodeJsonPath = rootNode.getNodePath().concat(".").concat(childNodeName);

                TreeNode childNode = new TreeNode(value, childNodeName, TreeNode.NodeType.OBJECT, childNodeJsonPath);
                rootNode.addChildNode(childNode);

                int records = 0;

                if (isArray) {
                    // If array, visit each of the object in JsonArray and add it to the value of the tree node.
                    List<TreeNode> arrayNodes = new ArrayList<>();
                    childNode.setValue(arrayNodes);
                    childNode.setNodeType(TreeNode.NodeType.ARRAY);

                    if (value != null && !((JSONArray) value).isEmpty()) {
                        JSONArray jsonArray = (JSONArray) value;
                        for (Object eachJsonObject : jsonArray) {
                            boolean isObject = jsonPathMap.get(childNode.getNodePath()) != null;
                            TreeNode arrayNode = new TreeNode(eachJsonObject, childNodeName, TreeNode.NodeType.OBJECT,
                                    childNode.getNodePath());
                            arrayNodes.add(arrayNode);
                            int nodeRecords;
                            if (!isObject) {
                                nodeRecords = 1;
                                // Node does not have any child , hence it should be premitive
                                arrayNode.setNodeType(TreeNode.NodeType.PREMITIVE);
                            }
                            else {
                                nodeRecords = generateTree(jsonPathMap, arrayNode);
                            }
                            arrayNode.setRecordCount(nodeRecords);
                            records += nodeRecords;
                        }
                    }
                    else {
                        TreeNode arrayNode = new TreeNode(null, childNodeName, TreeNode.NodeType.PREMITIVE, childNode
                                .getNodePath());
                        arrayNodes.add(arrayNode);
                        int nodeRecords = generateTree(jsonPathMap, arrayNode);
                        arrayNode.setRecordCount(nodeRecords);
                        records += nodeRecords;
                    }

                    childNode.setRecordCount(records);
                }
                else {
                    boolean isObject = jsonPathMap.get(childNode.getNodePath()) != null;
                    if (isObject) {
                        int nodeRecords = generateTree(jsonPathMap, childNode);
                        childNode.setRecordCount(nodeRecords);
                        records = nodeRecords;
                    }
                    else {
                        childNode.setNodeType(TreeNode.NodeType.PREMITIVE);
                        childNode.setRecordCount(1);
                        records = 1;
                    }
                }

                levelRecordCount *= records;
            }
        }
        return levelRecordCount;
    }

    /**
     * Process json path group e.g $.(A[*], A[*]..b, B[*])
     * Json path group is added to get set of object/values related to multiple json paths
     * and put it under one root node.
     */
    private Object processJsonPathGroup(TreeNode rootNode, String childNodeName)
    {
        String[] jsonPaths = childNodeName.split(",");
        JSONArray jsonArray = null;
        for (String jsonPath : jsonPaths) {
            String finalJsonPath = "$.".concat(jsonPath);
            Object eachValue = getValueAtJsonPath(rootNode, finalJsonPath);
            if (eachValue != null) {
                if (jsonArray == null) {
                    jsonArray = new JSONArray();
                }
                if (eachValue instanceof JSONArray) {
                    jsonArray.addAll((JSONArray) eachValue);
                }
                else {
                    jsonArray.add(eachValue);
                }
            }
        }
        return jsonArray;
    }

    /**
     * Process single json path under root node.
     */
    private Object processSingleJsonPath(TreeNode rootNode, String childNodeName)
    {
        String jsonPath = "$.".concat(childNodeName);
        return getValueAtJsonPath(rootNode, jsonPath);
    }

    private Object getValueAtJsonPath(TreeNode node, String jsonPath)
    {
        Object value = null;
        try {
            if (node.getValue() != null) {
                value = JsonPath.read(node.getValue(), jsonPath);
            }
        }
        catch (PathNotFoundException exp) {
            // catch the exception so that we can put null as the value
        }
        return value;
    }

    /**
     * Create the records by traversing the whole tree in depth first manner and uses bottom up approach.
     *
     * @param rootNode root node of the tree generated using {@link JsonFlattener#generateTree(Map, TreeNode)}
     * @return list of flattened records which is represented as {@link JsonFlattener.Record}
     */
    private List<Record> createRecords(TreeNode rootNode)
    {
        logger.debug("Creating records for json");
        List<TreeNode> childNodes = rootNode.getChildNodes();

        for (TreeNode childNode : childNodes) {
            if (childNode.getNodeType() == TreeNode.NodeType.ARRAY) {
                // This is array of nodes, hence we need tovisit each of the array node and create records for each.
                List<TreeNode> arrayNodes = (List<TreeNode>) childNode.getValue();
                List<Record> recordsForArrayNodes = new ArrayList<>();
                for (TreeNode arrayNode : arrayNodes) {
                    List<Record> records;
                    if (arrayNode.getChildNodes() == null && arrayNode.getNodeType() != TreeNode.NodeType.OBJECT) {
                        // set initial capacity to 1 as there is only one value to be added.
                        records = new ArrayList<>(1);
                        records.add(new Record(arrayNode.getNodePath(), arrayNode.getValue()));
                    }
                    else {
                        records = createRecords(arrayNode);
                    }
                    arrayNode.setRecords(records);
                    // Records get added for each of the node in the array.
                    recordsForArrayNodes.addAll(records);
                }
                childNode.setRecords(recordsForArrayNodes);
            }
            else {
                if (childNode.getChildNodes() == null) {
                    // This is leaf node, hence required for our flattened record.
                    //if (childNode.getNodeType() != TreeNode.NodeType.OBJECT) {
                    childNode.addRecord(new Record(childNode.getNodePath(), childNode.getValue()));
                    //}
                }
                else {
                    // This is single object node. Create record by traversing childs under this node.
                    List<Record> records = createRecords(childNode);
                    childNode.setRecords(records);
                }
            }
        }

        // Travers each element at the same level and do cross join of records of each node.
        List<Record> recordList = new ArrayList<>();
        int prevRecordCount = 1;
        for (TreeNode childNode : childNodes) {
            List<Record> newRecordList = new ArrayList<>();
            for (int j = 0; j < prevRecordCount; j++) {
                for (int k = 0; k < childNode.getRecordCount(); k++) {
                    // if it is first record, then we don't need to copy the record while merging as it is already
                    // there.
                    Record record = recordList.isEmpty() ? new Record() : ((k == 0) ? recordList.get(j) :
                            new Record().mergeRecord(recordList.get(j)));
                    if (childNode.getRecords() != null) {
                        record.mergeRecord(childNode.getRecords().get(k));
                    }
                    newRecordList.add(record);
                }
            }
            prevRecordCount = newRecordList.isEmpty() ? prevRecordCount : newRecordList.size();
            recordList = newRecordList;
        }
        if (rootNode.getNodeName().equals("$")) {
            System.out.println(rootNode.getNodeName());
        }
        return recordList;
    }

    /**
     * Class to represent the node of the tree representing json.
     */
    private static final class TreeNode
    {
        Object value;
        int recordCount;
        String nodeName;
        String nodePath;
        NodeType nodeType;
        List<TreeNode> childNodes;

        // list of records for the node
        List<Record> records;

        enum NodeType
        {
            ARRAY,
            OBJECT,
            PREMITIVE
        }

        TreeNode(Object value, String nodeName, NodeType nodeType)
        {
            this(nodeName, nodeType);
            this.value = value;
        }

        TreeNode(String nodeName, NodeType nodeType)
        {
            this.nodeName = nodeName;
            this.nodeType = nodeType;
        }

        TreeNode(Object value, String nodeName, NodeType nodeType, String nodePath)
        {
            this(value, nodeName, nodeType);
            this.nodePath = nodePath;
        }

        String getNodePath()
        {
            return nodePath;
        }

        void setNodePath(String nodePath)
        {
            this.nodePath = nodePath;
        }

        Object getValue()
        {
            return value;
        }

        void setValue(Object value)
        {
            this.value = value;
        }

        String getNodeName()
        {
            return nodeName;
        }

        void setNodeName(String nodeName)
        {
            this.nodeName = nodeName;
        }

        NodeType getNodeType()
        {
            return nodeType;
        }

        void setNodeType(NodeType nodeType)
        {
            this.nodeType = nodeType;
        }

        List<TreeNode> getChildNodes()
        {
            return childNodes;
        }

        void addChildNode(TreeNode dataNode)
        {
            if (this.childNodes == null || this.childNodes.isEmpty()) {
                this.childNodes = new ArrayList<>();
            }
            this.childNodes.add(dataNode);
        }

        void setChildNodes(List<TreeNode> nextNodes)
        {
            this.childNodes = nextNodes;
        }

        int getRecordCount()
        {
            return recordCount;
        }

        void setRecordCount(int recordCount)
        {
            this.recordCount = recordCount;
        }

        List<Record> getRecords()
        {
            return records;
        }

        void setRecords(List<Record> records)
        {
            this.records = records;
        }

        void addRecord(Record record)
        {
            if (this.records == null) {
                this.records = new ArrayList<>();
            }
            this.records.add(record);
        }
    }

    /**
     * Class to represent the raw of the flattened json.
     */
    private static final class Record
    {
        Map<String, Object> cells;

        Record()
        {
            cells = new LinkedHashMap<>();
        }

        Record(String columName, Object columnValue)
        {
            this.cells = new LinkedHashMap<>();
            if (columnValue == null) {
                columnValue = "";
            }
            this.cells.put(columName, columnValue);
        }

        Map<String, Object> getCells()
        {
            return cells;
        }

        Record mergeRecord(Record record)
        {
            this.cells.putAll(record.getCells());
            return this;
        }

        void addCell(String columName, Object columnValue)
        {
            if (this.cells == null) {
                this.cells = new LinkedHashMap<>();
            }
            this.cells.put(columName, columnValue);
        }

        void print()
        {
            System.out.print(cells + " ");
            System.out.println();
        }

        String toString(char separator)
        {
            if (cells == null || cells.values() == null) {
                return null;
            }
            return Joiner.on(separator).join(cells.values());
        }

        /**
         * Convert to list of cell values with sequence specified in jsonPaths.
         *
         * @param jsonPaths - he sequence in which we want cell values to be in string.
         * @return
         */
        List<String> toList(List<String> jsonPaths)
        {
            if (cells == null || cells.values().isEmpty()) {
                return null;
            }
            return jsonPaths.stream()
                    .map(jsonPath -> cells.get(jsonPath) != null ? String.valueOf(cells.get(jsonPath)) : "")
                    .collect(Collectors.toList());
        }

        /**
         * Convert to string with cell values separated with {@code separator}
         *
         * @param separator - separator to be used to connect cell values
         * @param jsonPaths - the sequence in which we want cell values to be in string.
         * @return
         */
        String toString(CharSequence separator, List<String> jsonPaths)
        {
            if (cells == null || cells.values().isEmpty()) {
                return null;
            }
            return jsonPaths.stream().map(jsonPath -> {
                StringBuilder valueBuilder = new StringBuilder();
                // Returning value as empty there is no value associated with it.
                String value = cells.get(jsonPath) != null ? String.valueOf(cells.get(jsonPath)) : "";
                // Wrapping value under " " as there is , involved
                valueBuilder.append("\"").append(value).append("\"");
                return valueBuilder.toString();
            }).collect(Collectors.joining(separator));
        }

        @Override
        public String toString()
        {
            return toString(',');
        }
    }
}
