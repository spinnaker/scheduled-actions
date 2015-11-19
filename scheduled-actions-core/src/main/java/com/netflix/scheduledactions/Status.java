/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.scheduledactions;

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
