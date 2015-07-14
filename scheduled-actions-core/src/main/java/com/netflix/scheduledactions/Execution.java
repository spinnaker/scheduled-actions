package com.netflix.scheduledactions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * @author sthadeshwar
 */
public class Execution {

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
        this.createdTime = System.nanoTime();   // Used just for comparing in isBefore()
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
        return log;
    }

    public Logger getLogger() {
        return logger;
    }

    public boolean isBefore(Execution execution) {
        return this.createdTime - execution.getCreatedTime() < 0;
    }

    public String toString() {
        return String.format("%s|%s", id, createdTime);
    }

    public class Logger {
        private final String DATE_FORMAT = "dd/MM/yyyy HH:mm:ss";

        private Logger() {
            log = Collections.synchronizedList(new ArrayList<LogEntry>());
        }

        public void info(String info) {
            log.add(new LogEntry(new SimpleDateFormat(DATE_FORMAT).format(new Date()), info));
        }

        public void warn(String warn) {
            log.add(new LogEntry(new SimpleDateFormat(DATE_FORMAT).format(new Date()), warn));
        }

        public void error(String error) {
            log.add(new LogEntry(new SimpleDateFormat(DATE_FORMAT).format(new Date()), error));
        }

        public void error(String error, Throwable throwable) {
            StringWriter stringWriter = new StringWriter();
            throwable.printStackTrace(new PrintWriter(stringWriter));
            log.add(new LogEntry(new SimpleDateFormat(DATE_FORMAT).format(new Date()),
                String.format("%s : %s", new SimpleDateFormat(DATE_FORMAT).format(new Date()), error, stringWriter.toString())));
        }
    }

    public static class LogEntry {
        private final String timestamp;
        private final String message;

        @JsonCreator
        public LogEntry(@JsonProperty("timestamp") String timestamp,
                        @JsonProperty("message") String message) {
            this.timestamp = timestamp;
            this.message = message;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public String getMessage() {
            return message;
        }
    }
}
