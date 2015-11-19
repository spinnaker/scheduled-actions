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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class Context {
    private String actionInstanceId;
    private final String name;
    private final String group;
    private final Map<String, String> parameters;

    @JsonCreator
    public Context(@JsonProperty("actionInstanceId") String actionInstanceId,
                   @JsonProperty("name") String name,
                   @JsonProperty("group") String group,
                   @JsonProperty("parameters") Map<String, String> parameters) {
        this.actionInstanceId = actionInstanceId;
        this.name = name;
        this.group = group;
        this.parameters = parameters;
    }

    public void setActionInstanceId(String actionInstanceId) {
        this.actionInstanceId = actionInstanceId;
    }

    public String getActionInstanceId() {
      return actionInstanceId;
    }

    public String getName() {
        return name;
      }

    public String getGroup() {
      return group;
    }

    public Map<String, String> getParameters() {
      return parameters;
    }

    @Override
    public String toString() {
        return String.format(
            "Context: (actionInstance: %s, name: %s, group: %s, parameters: %s)", actionInstanceId, name, group, parameters
        );
    }
}
