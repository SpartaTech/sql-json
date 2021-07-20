package io.github.spartatech.sqljson.vo;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;
import java.util.stream.Collectors;

public class JsonQueryClause {

    private String rootElement;
    private Expression filters;
    private boolean distinctResults;

    private List<SelectItem> returningFields;

    public String getRootElement() {
        return rootElement;
    }

    public void setRootElement(String rootElement) {
        this.rootElement = rootElement;
    }

    public Expression getFilters() {
        return filters;
    }

    public void setFilters(Expression filters) {
        this.filters = filters;
    }

    public boolean isDistinctResults() {
        return distinctResults;
    }

    public void setDistinctResults(boolean distinctResults) {
        this.distinctResults = distinctResults;
    }

    public List<SelectItem> getReturningFields() {
        return returningFields;
    }

    public void setReturningFields(List<SelectItem> returningFields) {
        this.returningFields = returningFields;
    }



    @Override
    public String toString() {
        return new StringBuilder("JsonQueryClause{")
                .append("rootElement='").append(rootElement).append('\'')
                .append(", filters=").append(filters)
                .append(", returningFields=")
                .append(returningFields.stream()
                        .map(SelectItem::toString)
                        .collect(Collectors.joining(", ")))
                .append('}')
                .toString();
    }
}
