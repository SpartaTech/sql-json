package io.github.spartatech.sqljson.sqlparse;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import io.github.spartatech.sqljson.vo.JsonQueryClause;

import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

import static io.github.spartatech.sqljson.util.StringUtility.unquote;

/**
 * Class responsible to Parse the SQL query and convert to
 * JsonQueryClause.
 */
public class SqlParser {

    private final String query;

    /**
     * Constructor.
     *
     * @param query to be parsed
     */
    public SqlParser(String query) {
        this.query = query;
    }

    /**
     * Parses the query and return it to be processed.
     *
     * @return JsonQueryClause
     * @throws SQLSyntaxErrorException in case parse fails
     * @throws SQLFeatureNotSupportedException in case trying to query multiple tables
     */
    public JsonQueryClause parseQuery() throws SQLSyntaxErrorException, SQLFeatureNotSupportedException {
        final Statement stmt;
        try {
            stmt = CCJSqlParserUtil.parse(query);
        } catch (JSQLParserException e) {
            throw new SQLSyntaxErrorException(e);
        }

        final Select selectStatement = (Select) stmt;
        final TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();

        final JsonQueryClause result = new JsonQueryClause();

        if (selectStatement.getSelectBody() instanceof PlainSelect) {
            final PlainSelect ps = (PlainSelect) selectStatement.getSelectBody();
            result.setFilters(ps.getWhere());
            result.setDistinctResults(ps.getDistinct() != null);
            result.setReturningFields(ps.getSelectItems());
        }

        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
        if (tableList.size() != 1) {
            throw new SQLFeatureNotSupportedException("Only allowed selecting one element as table");
        }

        result.setRootElement(unquote(tableList.get(0)));

        return result;
    }
}
