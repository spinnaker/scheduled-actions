package com.netflix.scheduledactions;

/**
 * @author sthadeshwar
 * Date: 1/12/15
 * Time: 2:31 PM
 */
public enum Status {
    CANCELLED("CANCELLED", true), COMPLETED("COMPLETED", true), FAILED("FAILED", true), TIMED_OUT("TIMED_OUT", true), SKIPPED("SKIPPED", true),
    SCHEDULED("SCHEDULED", false), IN_PROGRESS("IN_PROGRESS", false), DISABLED("DISABLED", false),
    ENABLED("ENABLED", false);

    private final String status;
    private final boolean complete;
    private String message;         // Below two non-final fields are there for a purpose
    private String initiator;

    Status(String status, boolean complete) {
        this.status = status;
        this.complete = complete;
    }

    public String getStatus() {
        return status;
    }

    public boolean isComplete() {
        return complete;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getInitiator() {
        return initiator;
    }

    public void setInitiator(String initiator) {
        this.initiator = initiator;
    }
}
