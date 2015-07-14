package com.netflix.scheduledactions.persistence.cassandra;

/**
 * @author sthadeshwar
 */
public class CassandraException extends RuntimeException {
    public CassandraException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
