package com.netflix.scheduledactions.exceptions;

import com.netflix.scheduledactions.Status;

/**
 * Thrown when there is an exception encountered while executing {@code Action}
 * @author sthadeshwar
 */
public class ExecutionException extends RuntimeException {

    private final Status status;

    public ExecutionException(String message) {
        super(message);
        this.status = Status.FAILED;
    }

    public ExecutionException(String message, Throwable throwable) {
        super(message, throwable);
        this.status = Status.FAILED;
    }

    public ExecutionException(Throwable throwable) {
        super(throwable);
        this.status = Status.FAILED;
    }

    public ExecutionException(String message, Throwable throwable, Status status) {
        super(message, throwable);
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }
}
