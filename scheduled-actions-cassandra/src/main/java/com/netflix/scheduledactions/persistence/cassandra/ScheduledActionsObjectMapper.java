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

package com.netflix.scheduledactions.persistence.cassandra;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import rx.functions.Action1;

/**
 * @author sthadeshwar
 */
public class ScheduledActionsObjectMapper extends ObjectMapper {

    public ScheduledActionsObjectMapper() {
        configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true);
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.ANY);
        setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
        addMixInAnnotations(Action1.class, Action1Mixin.class);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    public abstract static class Action1Mixin {}
}
