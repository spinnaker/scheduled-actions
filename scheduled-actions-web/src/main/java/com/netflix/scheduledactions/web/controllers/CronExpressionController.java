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
 * A controller class to simply do a cron expression validation
 * @author sthadeshwar
 */
@RestController
public class CronExpressionController {

    @RequestMapping(value = "/validateCronExpression", method = RequestMethod.GET)
    @ResponseStatus(value = HttpStatus.OK)
    public Map<String,String> actionInstances(@RequestParam String cronExpression) {
        try {
            TriggerUtils.validateCronExpression(cronExpression);
            return ImmutableMap.<String,String>builder().put("response", "Cron expression is valid").build();
        } catch (IllegalArgumentException e) {
            throw new InvalidCronExpressionException(
                String.format("Cron expression '%s' is not valid: %s", cronExpression, e.getMessage())
            );
        }
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public static class InvalidCronExpressionException extends IllegalArgumentException {
        public InvalidCronExpressionException(String message) {
            super(message);
        }
    }


}
