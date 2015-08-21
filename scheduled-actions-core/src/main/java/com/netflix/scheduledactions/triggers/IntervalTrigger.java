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

package com.netflix.scheduledactions.triggers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.fenzo.triggers.TriggerUtils;
import com.netflix.scheduledactions.Context;
import rx.functions.Action1;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author sthadeshwar
 */
public class IntervalTrigger implements Trigger {

    public static final String ISO_8601_TIME_PREFIX = "PT";
    public static final String ISO_8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX";

    public static enum TimeUnit {
        SECONDS("S"), MINUTES("M"), HOURS("H");

        private final String timeUnitSuffix;

        TimeUnit(String timeUnitSuffix) { this.timeUnitSuffix = timeUnitSuffix; }

        public String getTimeUnitSuffix() { return timeUnitSuffix; }
    }

    private final String iso8601Interval;
    private final int repeatCount;

    @JsonCreator
    public IntervalTrigger(@JsonProperty("iso8601Interval") String iso8601Interval,
                           @JsonProperty("repeatCount") int repeatCount) {
        this.iso8601Interval = iso8601Interval;
        this.repeatCount = repeatCount;
    }

    public IntervalTrigger(int interval, TimeUnit intervalUnit, Date startAt) {
        this(interval, intervalUnit, -1, startAt);
    }

    public IntervalTrigger(int interval, TimeUnit intervalUnit, int repeatCount, Date startAt) {
        if (interval <= 0) {
            throw new IllegalArgumentException(String.format("Invalid interval %s specified for the IntervalTrigger", interval));
        }
        DateFormat dateFormat = new SimpleDateFormat(ISO_8601_DATE_FORMAT);
        if (startAt == null) startAt = new Date();
        this.iso8601Interval = String.format(
            "%s/%s%s%s", dateFormat.format(startAt), ISO_8601_TIME_PREFIX, interval, intervalUnit.getTimeUnitSuffix()
        );
        this.repeatCount = repeatCount;
    }

    @Override
    public void validate() throws IllegalArgumentException {
        TriggerUtils.validateISO8601Interval(iso8601Interval);
    }

    @Override
    public com.netflix.fenzo.triggers.Trigger<Context> createFenzoTrigger(Context context,
                                                                          Class<? extends Action1<Context>> action) {
        return new com.netflix.fenzo.triggers.IntervalTrigger<>(
            this.iso8601Interval, this.repeatCount, context.getName(), context, Context.class, action
        );
    }
}
