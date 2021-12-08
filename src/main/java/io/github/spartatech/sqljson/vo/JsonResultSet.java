package io.github.spartatech.sqljson.vo;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class JsonResultSet {

    private LinkedHashSet<String> header;
    private List<ResultRow> rows;

    private JsonResultSet(LinkedHashSet<String> header, List<ResultRow> rows) {
        this.header = header;
        this.rows = rows;
    }

    private Integer currentRow = -1;

    public boolean next() {
        if (currentRow+1 < rows.size()) {
            currentRow ++;
            return true;
        } else {
            return false;
        }
    }

    public boolean previous() {
        if (currentRow-1 >= 0) {
            currentRow --;
            return true;
        } else {
            return false;
        }
    }

    public ResultRow getRow(int rowNumber) {
        return rows.get(rowNumber);
    }

    public JsonNode getColumn(int columnIndex) {
        return rows.get(currentRow).getColumn(columnIndex);
    }

    public JsonNode getColumn(String columnLabel) throws SQLException {
        return getColumn(indexForLabel(columnLabel));
    }

    public LinkedHashSet<String> getColumnNames() {
        return this.header;
    }

    public ResultRow getAllColumns() {
        return rows.get(currentRow);
    }
    
    public int size() {
        return rows.size();
    }

    /* ResultSet derived methods */

    public String getString(int columnIndex) {
        return rows.get(currentRow).getColumn(columnIndex).textValue();
    }

    public boolean getBoolean(int columnIndex) {
        return rows.get(currentRow).getColumn(columnIndex).asBoolean();
    }

    public byte getByte(int columnIndex) {
        return ((Integer) rows.get(currentRow).getColumn(columnIndex).asInt()).byteValue();
    }

    public short getShort(int columnIndex) {
        return ((Integer)rows.get(currentRow).getColumn(columnIndex).asInt()).shortValue();
    }

    public int getInt(int columnIndex) {
        return rows.get(currentRow).getColumn(columnIndex).asInt();
    }

    public long getLong(int columnIndex) {
        return rows.get(currentRow).getColumn(columnIndex).asLong();
    }

    public float getFloat(int columnIndex) {
        return ((Double)rows.get(currentRow).getColumn(columnIndex).asDouble()).floatValue();
    }

    public double getDouble(int columnIndex) {
        return rows.get(currentRow).getColumn(columnIndex).asDouble();
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) {
        return BigDecimal.valueOf(rows.get(currentRow).getColumn(columnIndex).asLong(), scale);
    }

    public byte[] getBytes(int columnIndex) {
        return rows.get(currentRow).getColumn(columnIndex).asText().getBytes(StandardCharsets.UTF_8);
    }

    public Date getDate(int columnIndex) throws SQLException {
        throw new SQLException("getDate not yet implemented");
    }

    public Time getTime(int columnIndex) throws SQLException {
        throw new SQLException("getTime not yet implemented");
    }

    public Timestamp getTimestamp(int columnIndex)  throws SQLException {
        throw new SQLException("getTimestamp not yet implemented");
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLException("getAsciiStream not yet implemented");
    }

    public InputStream getUnicodeStream(int columnIndex) {
        return null;
    }
    
    public InputStream getBinaryStream(int columnIndex) {
        return null;
    }

    public String getString(String columnLabel) throws SQLException {
        return rows.get(currentRow).getColumn(indexForLabel(columnLabel)).asText();
    }
    
    public boolean getBoolean(String columnLabel) throws SQLException {
        return rows.get(currentRow).getColumn(indexForLabel(columnLabel)).asBoolean();
    }
    
    public byte getByte(String columnLabel) throws SQLException {
        return ((Integer)rows.get(currentRow).getColumn(indexForLabel(columnLabel)).asInt()).byteValue();
    }
    
    public short getShort(String columnLabel) throws SQLException {
        return ((Integer)rows.get(currentRow).getColumn(indexForLabel(columnLabel)).asInt()).shortValue();
    }
    
    public int getInt(String columnLabel) throws SQLException {
        return rows.get(currentRow).getColumn(indexForLabel(columnLabel)).asInt();
    }

    public Long getLong(String columnLabel) throws SQLException {
        final JsonNode node = rows.get(currentRow).getColumn(indexForLabel(columnLabel));
        if (node.isNull()) {
            return null;
        } else {
            return node.asLong();
        }
    }

    public float getFloat(String columnLabel) throws SQLException {
        return ((Double)rows.get(currentRow).getColumn(indexForLabel(columnLabel)).asDouble()).floatValue();
    }

    public double getDouble(String columnLabel) throws SQLException {
        return rows.get(currentRow).getColumn(indexForLabel(columnLabel)).asDouble();
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return BigDecimal.valueOf(rows.get(currentRow).getColumn(indexForLabel(columnLabel)).asLong(), scale);
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        return rows.get(currentRow).getColumn(indexForLabel(columnLabel)).asText().getBytes(StandardCharsets.UTF_8);
    }

    public Date getDate(String columnLabel) throws SQLException {
        throw new SQLException("getDate not yet implemented");
    }

    public Time getTime(String columnLabel) throws SQLException {
        throw new SQLException("getTime not yet implemented");
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw new SQLException("getTimestamp not yet implemented");
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLException("getAsciiStream not yet implemented");
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLException("getUnicodeStream not yet implemented");
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLException("getBinaryStream not yet implemented");
    }

    public int findColumn(String columnLabel) throws SQLException {
        return indexForLabel(columnLabel);
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        final JsonNode val = rows.get(currentRow).getColumn(columnIndex);
        if (val.isLong()) {
            return BigDecimal.valueOf(val.asLong());
        } else if (val.isDouble()) {
            return BigDecimal.valueOf(val.asDouble());
        }
        throw new SQLException(columnIndex + " cannot be converted to BigDecimal");
    }

    
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        final JsonNode val = rows.get(currentRow).getColumn(indexForLabel(columnLabel));
        if (val.isLong()) {
            return BigDecimal.valueOf(val.asLong());
        } else if (val.isDouble()) {
            return BigDecimal.valueOf(val.asDouble());
        }
        throw new SQLException(columnLabel + " cannot be converted to BigDecimal");
    }
    
    private int indexForLabel(String columnLabel) throws SQLException {
        int index = 0;
        for (String h : header) {
            if (h.equals(columnLabel)) {
                return index;
            } else {
                index ++;
            }
        }
        throw new SQLException("Column " + columnLabel + " not found");
    }

    /**
     * Builder class to create JsonResultSet.
     * Has logic to order/distinct the results.
     */
    public static class JsonResultSetBuilder {
        private static Logger log = LoggerFactory.getLogger(JsonResultSetBuilder.class);

        private LinkedHashSet<String> header = new LinkedHashSet<>();
        private List<ResultRow> rows = new ArrayList<>();

        public static JsonResultSetBuilder instance() {
            return new JsonResultSetBuilder();
        }

        public JsonResultSetBuilder addHeader(String headerColumn) {
            header.add(headerColumn);
            return this;
        }

        public JsonResultSetBuilder setHeaders(Set<String> header) {
            this.header = new LinkedHashSet<>(header);
            return this;
        }

        public JsonResultSetBuilder addRow(List<JsonNode> row) {
            rows.add(new ResultRow(row));
            return this;
        }

        public void applyDistinct() {
            log.debug("Applying distinct to result");
            rows = rows.stream().distinct().collect(Collectors.toList());
        }


        public JsonResultSet build() {
            return new JsonResultSet(header, rows);
        }
    }
}
