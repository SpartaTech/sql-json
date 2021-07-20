package com.github.spartatech.sqljson.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.spartatech.sqljson.exception.ExceptionWrapper;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;

/**
 * Utility methods for Json handling.
 */
public class JsonUtility {

    private static final Logger log = LoggerFactory.getLogger(JsonUtility.class);

    /**
     * Finds a given path (element in the json).
     * Recursion.
     *
     * @param currentJson current json element
     * @param traversalPath current traversal path
     * @return Object found for the traversalPath
     */
    public static JsonNode findElementInJson(JsonNode currentJson, String completeExpression, String[] traversalPath) {
        try {
            if (traversalPath.length == 0) {
                if (currentJson.isMissingNode()) {
                    throw new SQLException("Cannot find element '" + completeExpression + "'");
                }
                return currentJson;
            } else {
                final ArrayNode result = new ArrayNode(new JsonNodeFactory(false));
                log.trace("Traversing through elements '{}'", Arrays.toString(traversalPath));

                if  (currentJson instanceof ArrayNode) {
                    currentJson.forEach(pathNode -> {
                        JsonNode node = pathNode.path(traversalPath[0]);
                        if (node.isMissingNode()) {
                            throw ExceptionWrapper.of(new SQLException("Cannot find element '" + traversalPath[0] + "' from '" + completeExpression + "'"));
                        }

                        result.add(findElementInJson(node, completeExpression, Arrays.copyOfRange(traversalPath, 1, traversalPath.length)));
                    });
                    return result;
                } else {
                    JsonNode node = findElementInJson(currentJson.path(traversalPath[0]), completeExpression, Arrays.copyOfRange(traversalPath, 1, traversalPath.length));
                    if (node.isMissingNode()) {
                        throw new SQLException("Cannot find element '" + traversalPath[0] + "' from '" + completeExpression + "'");
                    }

                    return node;
                }
            }
        } catch (SQLException e) {
            throw ExceptionWrapper.of(e);
        }
    }

    /**
     * Converts a Json into a HashMap with the results
     * @param json json node to be flattened
     * @return {@code LinkedHashMap<String, JsonNode>} elements found
     */
    public static LinkedHashMap<String, JsonNode> flattenJsonFields (JsonNode json) {
        final LinkedHashMap<String, JsonNode> result = new LinkedHashMap<>();
        json.fields().forEachRemaining(col -> result.put(col.getKey(), col.getValue()));
        return result;
    }
}
