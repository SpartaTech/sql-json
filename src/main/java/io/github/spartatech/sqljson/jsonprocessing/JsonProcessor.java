package io.github.spartatech.sqljson.jsonprocessing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.github.spartatech.sqljson.SqlJsonConfig;
import io.github.spartatech.sqljson.exception.ExceptionWrapper;
import io.github.spartatech.sqljson.util.JsonUtility;
import io.github.spartatech.sqljson.vo.JsonQueryClause;
import io.github.spartatech.sqljson.vo.JsonResultSet;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Processes the query against given Json
 * and generates de results
 */
public class JsonProcessor {
    private static final Logger log = LoggerFactory.getLogger(JsonProcessor.class);

    private final JsonNode json;
    private final JsonQueryClause query;
    private final SqlJsonConfig config;

    /**
     * Constructor receiving JSON and query.
     *
     * @param json  json to be queried
     * @param query SQL to be executed
     */
    public JsonProcessor(JsonNode json, JsonQueryClause query, SqlJsonConfig config) {
        this.json = json;
        this.query = query;
        this.config = config;
    }

    /**
     * Main processor method.
     *
     * @return List of results found
     * @throws Exception in case of any failure
     */
    public JsonResultSet process() throws Exception {
        try {
            //find table
            final JsonNode table = findElementInJson();

            //filter list
            final List<JsonNode> filtered = filter(table);

            //select only items requested
            final List<LinkedHashMap<String, JsonNode>> narrowedData = narrowResultElements(filtered);

            //Convert to resultSet
            JsonResultSet.JsonResultSetBuilder result = convertDataToResultSetBuilder(narrowedData);

            //Apply distinct
            if (query.isDistinctResults()) {
                result.applyDistinct();
            }

            return result.build();
        } catch (ExceptionWrapper e) {
            throw e.unwrap();
        }
    }

    /**
     * Converts from Internal data structure into JsonResultSet.
     *
     * @param narrowedData internal data representation
     * @return JsonResultSet
     */
    private JsonResultSet.JsonResultSetBuilder convertDataToResultSetBuilder(List<LinkedHashMap<String, JsonNode>> narrowedData) {
        final JsonResultSet.JsonResultSetBuilder builder = JsonResultSet.JsonResultSetBuilder.instance();
        if (narrowedData.size() > 0) {
            final Set<String> headers = narrowedData.get(0).keySet();
            builder.setHeaders(headers);
            narrowedData.forEach(it -> {
                final List<JsonNode> row = new ArrayList<>();
                headers.forEach(colName -> row.add(it.get(colName)));
                builder.addRow(row);
            });
        }
        return builder;
    }

    /**
     * Return the selected columns.
     * Star will return all fields
     * . (dot) will return a json
     *
     * @param elements all elements to filter columns
     * @return List rows, where rows are a map of cols
     * @throws SQLSyntaxErrorException in case selectors are invalid
     */
    public List<LinkedHashMap<String, JsonNode>> narrowResultElements(List<JsonNode> elements) throws SQLSyntaxErrorException {
        if (query.getReturningFields().get(0).toString().equals("\".\"") && query.getReturningFields().size() > 1) {
            throw new SQLSyntaxErrorException("Selectors '.' cannot be combined with anything else");
        }
        if (query.getReturningFields().size() == 1
                && query.getReturningFields().get(0).toString().equals("\".\"")) {
            return elements.stream()
                    .map(item -> new LinkedHashMap<>(Map.of(".", item)))
                    .collect(Collectors.toList());
        }

        return elements.stream()
                .map(row -> {
                    final LinkedHashMap<String, JsonNode> newRow = new LinkedHashMap<>();
                    for (SelectItem field : query.getReturningFields()) {
                        final SelectClauseExpressionEvaluator evaluator = new SelectClauseExpressionEvaluator(row, config);
                        field.accept(evaluator);
                        newRow.putAll(evaluator.getResult());

//                        if (field instanceof AllColumns) {
//                            newRow.putAll(flattenJsonFields(row));
//                        } else if (field instanceof SelectExpressionItem) {
////                            final SelectExpressionItem fieldEx = (SelectExpressionItem) field;
////                            if (!(fieldEx.getExpression() instanceof Column)) {
////                                throw new ExpressionNotSupportedException(fieldEx.getClass() + " in select");
////                            }
////                            final Column column = (Column)fieldEx.getExpression();
////                            final String fieldName = column.getFullyQualifiedName();
////                            final JsonNode value = JsonUtility.findElementInJson(row, fieldName, fieldName.split("\\."));
////
////                            if (value.isMissingNode()) {
////                                throw ExceptionWrapper.of(
////                                        new SQLException("Field \"" + field + "\" not found in the json result"));
////                            }
////                            final String alias;
////                            if (fieldEx.getAlias() != null) {
////                                alias = fieldEx.getAlias().getName();
////                            } else {
////                                alias = fieldName;
////                            }
////
////                            newRow.put(alias, value);
//                        }
                    }
                    return newRow;
                }).collect(Collectors.toList());
    }


    /**
     * Filters table elements using query parameters.
     *
     * @param table to be filtered
     * @return Filtered elements
     */
    private List<JsonNode> filter(JsonNode table) {
        if (table instanceof ArrayNode) {
            final ArrayNode items = (ArrayNode) table;
            final List<JsonNode> result = new ArrayList<>();
            for (JsonNode item : items) {
                filter(item).stream()
                        .filter(Objects::nonNull)
                        .forEachOrdered(result::add);
            }
            return result;
        } else {
            if (query.getFilters() == null) {
                return List.of(table);
            } else {
                final WhereClauseExpressionEvaluator ev = new WhereClauseExpressionEvaluator(table);
                query.getFilters().accept(ev);
                if (ev.isKeep()) {
                    return List.of(table);
                } else {
                    return List.of();
                }
            }
        }
    }

    /**
     * Finds a table (element in the json). From inital query.
     * Entry-point.
     *
     * @return Object for the table
     */
    private JsonNode findElementInJson() {
        log.debug("Finding element in Json for table: {}", query.getRootElement());
        final String[] pathTokens = query.getRootElement().equals(".")
                ? new String[0]
                : query.getRootElement().split("\\.");

        return JsonUtility.findElementInJson(json, query.getRootElement(), pathTokens, false);
    }
}
