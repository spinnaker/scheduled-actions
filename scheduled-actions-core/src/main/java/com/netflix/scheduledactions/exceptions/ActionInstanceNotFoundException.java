package com.netflix.scheduledactions.exceptions;

/**
 * @author sthadeshwar
 */
public class ActionInstanceNotFoundException extends Exception {

    public ActionInstanceNotFoundException(String message) {
        super(message);
    }

    public ActionInstanceNotFoundException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
