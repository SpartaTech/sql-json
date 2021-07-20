package io.github.spartatech.sqljson.vo;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ResultRow {
    private List<JsonNode> columns = new ArrayList<>();

    protected ResultRow(List<JsonNode> columns) {
        this.columns = columns;
    }

    public JsonNode getColumn(int columnIndex) {
        return columns.get(columnIndex);
    }

    public List<JsonNode> getColumns() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResultRow resultRow = (ResultRow) o;
        return this.toString().equals(resultRow.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(toString());
    }

    @Override
    public String toString() {
        return "{" + columns.stream()
                .map(JsonNode::toString)
                .collect(Collectors.joining("},{"))
                +"}";
    }
}
