package io.github.spartatech.sqljson;

public class SqlJsonConfigurer {
    private boolean strictResultRowExistence = false;

    private SqlJsonConfigurer() {

    }

    public static SqlJsonConfigurer instance() {
        return new SqlJsonConfigurer();
    }

    public SqlJsonConfigurer strictResultRowExistence() {
        this.strictResultRowExistence = true;
        return this;
    }

    protected SqlJsonConfig toConfig() {
        return new SqlJsonConfig(strictResultRowExistence);
    }
}
