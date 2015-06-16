package com.netflix.scheduledactions.exceptions;

/**
 * @author sthadeshwar
 */
public class ActionOperationException extends RuntimeException {

    public ActionOperationException(String message) {
        super(message);
    }

    public ActionOperationException(String message, Throwable throwable) {
        super(message, throwable);
    }
}