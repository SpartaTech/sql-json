package com.github.spartatech.sqljson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.spartatech.sqljson.jsonprocessing.JsonProcessor;
import com.github.spartatech.sqljson.sqlparse.SqlParser;
import com.github.spartatech.sqljson.vo.JsonQueryClause;
import com.github.spartatech.sqljson.vo.JsonResultSet;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class SqlJson {

    private final JsonNode json;

    public SqlJson(String json) throws IOException {
        this.json = new ObjectMapper().readTree(new ByteArrayInputStream(json.getBytes()));
    }

    public SqlJson(InputStream json) throws IOException {
        this.json = new ObjectMapper().readTree(json);
    }

    public SqlJson(File json) throws IOException {
        this.json = new ObjectMapper().readTree(json);
    }

    public JsonResultSet queryAsJSONObject(String sql) throws Exception {
        final JsonQueryClause query = new SqlParser(sql).parseQuery();
        return new JsonProcessor(json, query).process();

    }
}
