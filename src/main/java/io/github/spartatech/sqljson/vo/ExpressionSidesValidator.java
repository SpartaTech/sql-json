package io.github.spartatech.sqljson.vo;

import java.sql.SQLException;

@FunctionalInterface
public interface ExpressionSidesValidator {

    void validate(Object obj) throws SQLException;
}
