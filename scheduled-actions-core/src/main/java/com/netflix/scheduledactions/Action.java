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

import java.util.List;

/**
 * Any class that implements the {@code Action} interface can be executed as a {@code SimpleTrigger}
 */
public interface Action {
    /**
     * An action can override the current event status and return whatever status it wants to
     */
    public Status getStatus();

    /**
     * An action can override the trigger owners.
     * For example, for a given deployment action, the list of committers could be the owners of the action
     */
    public List<String> getOwners();

    /**
     * An action can override the trigger watchers
     * For example, for a given deployment action, the list of designated on-calls could be the watchers of the action
     */
    public List<String> getWatchers();

    /**
     * Executes the action
     * @throws Exception
     */
    public void execute(Context context, Execution execution) throws Exception;

}
