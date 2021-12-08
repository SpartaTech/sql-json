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
    private final SqlJsonConfig config;

    public SqlJson(String json) throws IOException {
        this.json = new ObjectMapper().readTree(new ByteArrayInputStream(json.getBytes()));
        this.config = SqlJsonConfigurer.instance().toConfig();
    }

    public SqlJson(InputStream json) throws IOException {
        this.json = new ObjectMapper().readTree(json);
        this.config = SqlJsonConfigurer.instance().toConfig();
    }

    public SqlJson(File json) throws IOException {
        this.json = new ObjectMapper().readTree(json);
        this.config = SqlJsonConfigurer.instance().toConfig();
    }

    public SqlJson(String json, SqlJsonConfigurer config) throws IOException {
        this.json = new ObjectMapper().readTree(new ByteArrayInputStream(json.getBytes()));
        this.config = config.toConfig();

    }

    public SqlJson(InputStream json, SqlJsonConfigurer config) throws IOException {
        this.json = new ObjectMapper().readTree(json);
        this.config = config.toConfig();
    }

    public SqlJson(File json, SqlJsonConfigurer config) throws IOException {
        this.json = new ObjectMapper().readTree(json);
        this.config = config.toConfig();
    }


    public JsonResultSet queryAsJSONObject(String sql) throws Exception {
        final JsonQueryClause query = new SqlParser(sql).parseQuery();
        return new JsonProcessor(json, query, config).process();

    }
}
