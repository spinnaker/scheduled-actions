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

import java.util.Date;

public class CronTrigger implements Trigger {

    private final String cronExpression;
    private final Date startAt;
    private final String timeZoneId;

    public CronTrigger(String cronExpression) {
        this(cronExpression, "America/Los_Angeles", new Date());
    }

    @JsonCreator
    public CronTrigger(@JsonProperty("cronExpression") String cronExpression,
                       @JsonProperty("timeZoneId") String timeZoneId,
                       @JsonProperty("startAt") Date startAt) {
        this.cronExpression = cronExpression;
        this.timeZoneId = timeZoneId;
        this.startAt = startAt;
    }

    @Override
    public void validate() throws IllegalArgumentException {
        TriggerUtils.validateCronExpression(this.cronExpression);
    }

    @Override
    public com.netflix.fenzo.triggers.Trigger<Context> createFenzoTrigger(Context context,
                                                                          Class<? extends Action1<Context>> action) {
        return new com.netflix.fenzo.triggers.CronTrigger<>(
            this.cronExpression, this.timeZoneId, this.startAt, context.getName(), context, Context.class, action
        );
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public Date getStartAt() {
        return startAt;
    }

    @Override
    public String toString() {
        return "CronTrigger (" + cronExpression + ')';
    }
}
