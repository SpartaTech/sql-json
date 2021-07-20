package com.github.spartatech.sqljson.jsonprocessing;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.spartatech.sqljson.exception.ExceptionWrapper;
import com.github.spartatech.sqljson.exception.ExpressionNotSupportedException;
import com.github.spartatech.sqljson.util.JsonUtility;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.sql.SQLException;
import java.util.LinkedHashMap;

/**
 * Evaluates expressions on Select part of Query.
 */
public class SelectClauseExpressionEvaluator extends ExpressionVisitorAdapter {

    private final JsonNode node;
    private final LinkedHashMap<String, JsonNode> result = new LinkedHashMap<>();


    public SelectClauseExpressionEvaluator(JsonNode node) {
        this.node = node;
    }

    public LinkedHashMap<String, JsonNode> getResult() {
        return result;
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        final String alias;
        if (selectExpressionItem.getAlias() != null) {
            alias = selectExpressionItem.getAlias().getName();
        } else if (selectExpressionItem.getExpression() instanceof Column) {
            alias = ((Column) selectExpressionItem.getExpression()).getFullyQualifiedName();
        } else {
            alias = selectExpressionItem.getExpression().toString();
        }

        final SingleItemSelectValueExpressionEvaluator itemEv = new SingleItemSelectValueExpressionEvaluator();
        selectExpressionItem.accept(itemEv);

        result.put(alias, itemEv.getValue());
    }

    @Override
    public void visit(AllColumns allColumns) {
        result.putAll(JsonUtility.flattenJsonFields(node));
    }

    /**
     * This sub class evaluates single value columns to
     * result in a JsonNode value.
     */
    private class SingleItemSelectValueExpressionEvaluator extends ExpressionVisitorAdapter {

        private JsonNode value;

        public JsonNode getValue() {
            return value;
        }

        @Override
        public void visit(Column column) {
            final String fieldName = column.getFullyQualifiedName();
            final JsonNode value = JsonUtility.findElementInJson(node, fieldName, fieldName.split("\\."));

            if (value.isMissingNode()) {
                throw ExceptionWrapper.of(
                        new SQLException("Field \"" + fieldName + "\" not found in the json result"));
            }

            this.value = value;
        }

        @Override
        public void visit(Function function) {
            throw new ExpressionNotSupportedException("Function select ("+function.toString()+ ")");
        }

        @Override
        public void visit(AllTableColumns allTableColumns) {
            throw new ExpressionNotSupportedException("Wildcard select ("+allTableColumns.toString()+ ")");
        }


    }
}
