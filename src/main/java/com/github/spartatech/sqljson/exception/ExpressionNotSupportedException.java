package com.github.spartatech.sqljson.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception in case an expression not supported was used in the query.
 */
public class ExpressionNotSupportedException extends RuntimeException {

    private static final Logger log = LoggerFactory.getLogger(ExpressionNotSupportedException.class);

    private String expression;

    /**
     * Constructor.
     *
     * @param expression expression attempted to be used
     */
    public ExpressionNotSupportedException(String expression) {
        super("Expression " + expression + " not yet supported");
        this.expression = expression;
    }

    public String getExpression() {
        return expression;
    }

    /**
     * Error for when a expression is not yet supported.
     * @param expression expression not handled
     * @return Exception
     */
    public static ExpressionNotSupportedException fromExpression(String expression) {
        log.warn("Attempt to use {}", expression);
        return new ExpressionNotSupportedException(expression);
    }
}
