package io.github.spartatech.sqljson;

public class SqlJsonConfig {
    private final boolean strictResultRowExistence;

    protected SqlJsonConfig(boolean strictResultRowExistence) {
        this.strictResultRowExistence = strictResultRowExistence;
    }

    public boolean isStrictResultRowExistence() {
        return strictResultRowExistence;
    }
}
