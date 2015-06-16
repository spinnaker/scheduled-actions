package com.netflix.scheduledactions.exceptions;

/**
 * @author sthadeshwar
 */
public class ExecutionNotFoundException extends Exception {

    public ExecutionNotFoundException(String message) {
        super(message);
    }

    public ExecutionNotFoundException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
