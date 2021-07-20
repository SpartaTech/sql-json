package com.github.spartatech.sqljson.exception;

public class ExceptionWrapper extends RuntimeException {
    private final Exception realException;

    public ExceptionWrapper(Exception realException) {
        this.realException = realException;
    }

    public Exception unwrap() {
        return realException;
    }

    public static ExceptionWrapper of(Exception realException) {
        return new ExceptionWrapper(realException);
    }
}
