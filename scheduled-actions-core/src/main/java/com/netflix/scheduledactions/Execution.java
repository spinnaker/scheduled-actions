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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Execution implements Comparable<Execution> {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss z";
    private static final String DATE_FORMAT_TIMEZONE = "America/Los_Angeles";

    private final String executorId;
    private final String actionInstanceId;
    private final long createdTime;
    private String id;
    private Date startTime;
    private Date endTime;
    private Status status;
    private List<LogEntry> log;
    @JsonIgnore
    private final Logger logger = new Logger();

    @JsonCreator
    public Execution(@JsonProperty("executorId") String executorId,
                     @JsonProperty("actionInstanceId") String actionInstanceId) {
        this.executorId = executorId;
        this.actionInstanceId = actionInstanceId;
        this.createdTime = System.nanoTime();   // Used just for comparing
    }

    public String getExecutorId() {
        return executorId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public String getActionInstanceId() {
        return actionInstanceId;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<LogEntry> getLog() {
        if (log != null) Collections.sort(log);
        return log;
    }

    public Logger getLogger() {
        return logger;
    }

    public boolean isBefore(Execution execution) {
        return this.createdTime - execution.getCreatedTime() < 0;
    }

    public String getFormattedStartTime() {
        return startTime != null ? getFormattedDate(startTime) : "";
    }

    public String getFormattedEndTime() {
        return endTime != null ? getFormattedDate(endTime) : "";
    }

    public String toString() {
        return String.format("%s|%s", id, createdTime);
    }

    @Override
    public int compareTo(Execution o) {
        if (o != null) {
            return this.createdTime - o.getCreatedTime() < 0 ? -1 :
                   this.createdTime - o.getCreatedTime() > 0 ?  1 : 0;
        }
        return 0;
    }

    public class Logger {
        private Logger() {
            log = Collections.synchronizedList(new ArrayList<LogEntry>());
        }

        public void info(String info) {
            log.add(new LogEntry(getFormattedDate(), info));
        }

        public void warn(String warn) {
            log.add(new LogEntry(getFormattedDate(), warn));
        }

        public void error(String error) {
            log.add(new LogEntry(getFormattedDate(), error));
        }

        public void error(String error, Throwable throwable) {
            StringWriter stringWriter = new StringWriter();
            throwable.printStackTrace(new PrintWriter(stringWriter));
            log.add(new LogEntry(getFormattedDate(), String.format("%s : %s", error, stringWriter.toString())));
        }
    }

    private static DateFormat newDateFormat() {
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        dateFormat.setTimeZone(TimeZone.getTimeZone(DATE_FORMAT_TIMEZONE));
        return dateFormat;
    }

    private static String getFormattedDate() {
        return newDateFormat().format(new Date());
    }

    private static String getFormattedDate(Date date) {
        return newDateFormat().format(date);
    }

    public static class LogEntry implements Comparable<LogEntry> {
        private final long logEntryTime;
        private final String timestamp;
        private final String message;

        @JsonCreator
        public LogEntry(@JsonProperty("timestamp") String timestamp,
                        @JsonProperty("message") String message) {
            this.logEntryTime = System.currentTimeMillis();
            this.timestamp = timestamp;
            this.message = message;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public int compareTo(LogEntry that) {
            if (that != null) {
                return this.logEntryTime == that.logEntryTime ?  0 :
                       this.logEntryTime  < that.logEntryTime ? -1 : 1;
            }
            return 0;
        }
    }
}
