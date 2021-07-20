package io.github.spartatech.sqljson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.spartatech.sqljson.jsonprocessing.JsonProcessor;
import io.github.spartatech.sqljson.sqlparse.SqlParser;
import io.github.spartatech.sqljson.vo.JsonQueryClause;
import io.github.spartatech.sqljson.vo.JsonResultSet;

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
