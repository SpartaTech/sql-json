package io.github.spartatech.sqljson.jsonprocessing;

import io.github.spartatech.sqljson.SqlJson;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import io.github.spartatech.sqljson.vo.JsonResultSet;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for JsonProcessor.
 */
public class JsonProcessorTest {

    private static final String DANIEL_ROW = "{\"name\":\"Daniel\",\"age\":38,\"weight\":161.5,\"vaccinated\":true,\"birthdate\":\"1982-11-30\",\"lastModified\":\"2012-04-23T18:25:43.511Z\"}";
    private static final String JOHN_ROW   = "{\"name\":\"John\",\"age\":41,\"weight\":180,\"vaccinated\":false,\"birthdate\":\"1978-01-30\",\"lastModified\":\"1998-04-23T18:25:43.511Z\"}";


    @Test
    public void test_invalid_wildcard_plus_column_selector() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select \".\", name from items where name = 'Daniel'";

        final SqlJson sqlj = new SqlJson(json);
        final var ex = assertThrows(SQLException.class, () ->sqlj.queryAsJSONObject(sql));

        assertEquals("Selectors '.' cannot be combined with anything else", ex.getMessage());
    }

    @ParameterizedTest
    @CsvSource({
            "simple-scenario,invalid,Cannot find element 'invalid'",
            "simple-scenario,items.invalid,Cannot find element 'invalid' from 'items.invalid'",
            "multiple-list-scenario,levels.elements.invalid,Cannot find element 'invalid' from 'levels.elements.invalid'",
            "multiple-list-scenario,levels.name.invalid,Cannot find element 'levels.name.invalid'"
    })
    public void test_invalid_table(String jsonFile, String table, String errorMessage) throws Exception {
        final String json = loadFromFile(jsonFile);
        final String sql = "select * from "+ table+" where name = 'Daniel'";

        final SqlJson sqlj = new SqlJson(json);
        final var ex = assertThrows(SQLException.class, () ->sqlj.queryAsJSONObject(sql));

        assertEquals(errorMessage, ex.getMessage());
    }

    @Test
    public void simple_equals_scenario_with_filter_return_star() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select * from items where name = 'Daniel'";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
        assertEquals(38L, results.getLong("age"));
    }

    @ParameterizedTest
    @CsvSource({
            "name,age",
            "age,name",
    })
    public void simple_test_multi_column_order(String col1, String col2) throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select "+col1+", "+col2+" from items where name = 'Daniel'";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());

        assertEquals(1, results.size());
        final Iterator<String> iter = results.getColumnNames().iterator();
        assertEquals(col1, iter.next());
        assertEquals(col2, iter.next());
    }

    @Test
    public void simple_test_invalid_column() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select invalid from items where name = 'Daniel'";

        final SqlJson sqlj = new SqlJson(json);
        final var ex = assertThrows(SQLException.class, () -> sqlj.queryAsJSONObject(sql));
        assertEquals("Cannot find element 'invalid'", ex.getMessage());
    }

    @Test
    public void simple_equals_scenario_return_dot() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select \".\" from items";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(2, results.size());
        results.next();
        assertEquals(DANIEL_ROW, results.getColumn(".").toString());
        results.next();
        assertEquals(JOHN_ROW, results.getColumn(0).toString());
    }

    @Test
    public void simple_equals_date_scenario_return_dot() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select \".\" from items where birthdate = {d '1982-11-30'}";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals(DANIEL_ROW, results.getColumn(".").toString());
    }



    @Test
    public void simple_equals_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where name = 'Daniel'";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
    }

    @Test
    public void simple_scenario_test_aliases() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name, name as alternate_name, age from items where name = 'Daniel'";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
        assertEquals("Daniel", results.getString("alternate_name"));
        assertEquals(38, results.getLong("age"));
    }

    @Test
    public void simple_not_equals_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where name != 'Daniel'";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("John", results.getColumn("name").asText());
    }

    @Test
    public void simple_negate_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where not (name = 'Daniel')";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("John", results.getString("name"));
    }

    @Test
    public void simple_negate_as_bang_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where ! (name = 'Daniel')";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("John", results.getString(0));
    }

    @Test
    public void simple_greater_than_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where age > 37";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(2, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
        results.next();
        assertEquals("John", results.getString("name"));
    }

    @Test
    public void simple_greater_than_equals_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where age >= 38";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(2, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
        results.next();
        assertEquals("John", results.getString("name"));
    }

    @Test
    public void simple_less_than_equals_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where age <= 38";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
    }

    @Test
    public void simple_less_than_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where age < 39";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
    }

    @Test
    public void simple_between_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where age between 37 and 39";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
    }

    @Test
    public void simple_between_scenario_conversion_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where weight between 160 and 179.9";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
    }

    @Test
    public void invalid_between_with_non_numeric_column() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where name between 160 and 179.9";

        final SqlJson sqlj = new SqlJson(json);
        final var ex = assertThrows(SQLException.class, () ->sqlj.queryAsJSONObject(sql));
        assertEquals("cannot perform between for String", ex.getMessage());
    }

    @Test
    public void simple_parenthesis_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where (age = 38 and name = 'Daniel') or (age = 38 and name = 'John')";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
    }

    @Test
    public void simple_parenthesis2_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where (age = 38 and name = 'Daniel') and (age = 38 and name = 'John')";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(0, results.size());
    }

    @Test
    public void simple_and_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where age = 38 and name = 'Daniel'";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
    }

    @Test
    public void simple_or_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select \".\" from items where name = 'John' or name = 'Daniel'";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(2, results.size());
        results.next();
        assertEquals(DANIEL_ROW, results.getColumn(".").toString());
        results.next();
        assertEquals(JOHN_ROW, results.getColumn(".").toString());
    }

    @Test
    public void simple_in_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where name in ('John', 'Daniel')";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(2, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
        results.next();
        assertEquals("John", results.getColumn("name").asText());
    }

    @Test
    public void simple_in_scenario_2_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where name in ('Daniel')";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
    }

    @Test
    public void simple_not_in_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where name not in ('Daniel')";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("John", results.getString("name"));
    }

    @Test
    public void addition() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where age = (1+37)";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
    }

    @Test
    public void subtraction() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where 37.6 = 38 - 0.4";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(2, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
        results.next();
        assertEquals("John", results.getString("name"));
    }

    @Test
    public void division() throws Exception {
        final String json = loadFromFile("simple-scenario");
        final String sql = "select name from items where 37.4 = 38.5/2.5 ";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(0, results.size());
    }

    @Test
    public void multi_list_in_scenario_with_filter_return_columns() throws Exception {
        final String json = loadFromFile("in-list-scenario");
        final String sql = "select name from items where 'IT' in departments";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("Daniel", results.getString("name"));
    }

    @Test
    public void multi_list_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select name from levels.elements";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(4, results.size());
        results.next();
        assertEquals("Level1Element1", results.getString("name"));
        results.next();
        assertEquals("Level1Element2", results.getString("name"));
        results.next();
        assertEquals("Level2Element1", results.getRow(2).getColumn(0).asText());
        results.next();
        assertEquals("Level2Element2", results.getRow(3).getColumn(0).asText());

    }

    @Test
    public void multi_column_filter_any_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAny(elements.name) = 'Level1Element1'";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("level1", results.getString("name"));
    }

    @Test
    public void multi_column_filter_all_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAll(elements.name) = 'Level1Element1'";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(0, results.size());
    }

    @Test
    public void multi_column_greater_than_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAll(elements.order) > 1";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
    }

    @Test
    public void multi_column_greater_than_equals_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAll(elements.order) >= 1";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(2, results.size());
    }

    @Test
    public void multi_column_less_than_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAll(elements.order) <2";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(0, results.size());
    }

    @Test
    public void multi_column_less_than_equals_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAll(elements.order) <=2";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
    }


    @Test
    public void multi_column_not_equals_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAll(elements.order) != 2";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
    }

    @Test
    public void multi_column_in_match_all_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAll(elements.order) in (1, 2)";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
    }

    @Test
    public void multi_column_in_match_any_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAny(elements.order) in (1, 2)";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
    }

    @Test
    public void multi_column_in_result_column() throws Exception {
        final String json = loadFromFile("simple-scenario_nested_json");
        final String sql = "select name, body.hair.color, body.hair.color as hairColor from items where name = 'Daniel'";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(1, results.size());
        results.next();
        assertEquals("brown", results.getString("body.hair.color"));
        assertEquals("brown", results.getString("hairColor"));
    }

    @Test
    public void multi_column_in_match_2_any_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAny(elements.order) in (1, 3)";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(2, results.size());
    }

    @Test
    public void multi_column_between_match_any_scenario_return_columns() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAny(elements.order) between 1 and 3";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(2, results.size());
    }

    @Test
    public void invalid_type_multi_column_match_any() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAny(elements.name) = 2";

        final SqlJson sqlj = new SqlJson(json);
        final SQLException ex = assertThrows(SQLException.class, () -> sqlj.queryAsJSONObject(sql));
        assertEquals("Invalid types in expression Level1Element1(String) = 2(Long)", ex.getMessage());
    }

    @Test
    public void invalid_type_multi_column_match_all() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAll(elements.name) = 2";

        final SqlJson sqlj = new SqlJson(json);
        final SQLException ex = assertThrows(SQLException.class, () -> sqlj.queryAsJSONObject(sql));
        assertEquals("Invalid types in expression Level1Element1(String) = 2(Long)", ex.getMessage());
    }

    @Test
    public void invalid_double_operation_in_multi_column() throws Exception {
        final String json = loadFromFile("multiple-list-scenario");
        final String sql = "select * from levels where matchAll(elements.name) = matchAll(elements.name)";

        final SqlJson sqlj = new SqlJson(json);
        final SQLException ex = assertThrows(SQLException.class, () -> sqlj.queryAsJSONObject(sql));
        assertEquals("MultiColumn on both sides of expression not allowed.", ex.getMessage());

    }

    @Test
    public void root_filtering_us_states() throws Exception {
        final String json = loadFromFile("us-states");
        final String sql = "select * from \".\" where abbreviation in ('FL', 'GA')";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(2, results.size());
        results.next();
        assertEquals("Florida", results.getString("name"));
        results.next();
        assertEquals("Georgia", results.getString("name"));
    }

    @Test
    public void root_filtering_us_cities() throws Exception {
        final String json = loadFromFile("us-cities");
        final String sql = "select * from \".\" where state = 'Florida'";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(375, results.size());

    }

    @Test
    public void root_filtering_us_cities_distinct() throws Exception {
        final String json = loadFromFile("us-cities");
        final String sql = "select distinct state from \".\" where state in ('Florida', 'Georgia')";

        final SqlJson sqlj = new SqlJson(json);
        final JsonResultSet results = sqlj.queryAsJSONObject(sql);

        assertEquals(2, results.size());
        results.next();
        assertEquals("Georgia", results.getString("state"));
        results.next();
        assertEquals("Florida", results.getString("state"));

    }



    private String loadFromFile(String filename) throws IOException {
        return IOUtils.resourceToString("./test-json/"+filename + ".json", Charset.defaultCharset(), this.getClass().getClassLoader());
    }
}
