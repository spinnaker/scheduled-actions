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

package com.netflix.scheduledactions.web.controllers;

import com.google.common.collect.ImmutableMap;
import com.netflix.fenzo.triggers.TriggerUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Controller that does different {@link com.netflix.scheduledactions.triggers.Trigger} attributes validation
 * @author sthadeshwar
 */
@RestController
public class ValidationController {

    @RequestMapping(value = "/validateCronExpression", method = RequestMethod.GET)
    @ResponseStatus(value = HttpStatus.OK)
    public Map<String,String> validateCronExpression(@RequestParam String cronExpression) {
        try {
            TriggerUtils.validateCronExpression(cronExpression);
            return ImmutableMap.<String,String>builder().put("response", "Cron expression is valid").build();
        } catch (IllegalArgumentException e) {
            throw new InvalidCronExpressionException(
                String.format("Cron expression '%s' is not valid: %s", cronExpression, e.getMessage())
            );
        }
    }

    @RequestMapping(value = "/validateISO8601Interval", method = RequestMethod.GET)
    @ResponseStatus(value = HttpStatus.OK)
    public Map<String,String> validateISO8601Interval(@RequestParam String iso8601Interval) {
        try {
            TriggerUtils.validateISO8601Interval(iso8601Interval);
            return ImmutableMap.<String,String>builder().put("response", "ISO8601 interval format is valid").build();
        } catch (IllegalArgumentException e) {
            throw new InvalidISO8601IntervalException(
                String.format("ISO8601 interval format '%s' is not valid: %s", iso8601Interval, e.getMessage())
            );
        }
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public static class InvalidCronExpressionException extends IllegalArgumentException {
        public InvalidCronExpressionException(String message) {
            super(message);
        }
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public static class InvalidISO8601IntervalException extends IllegalArgumentException {
        public InvalidISO8601IntervalException(String message) {
            super(message);
        }
    }


}
