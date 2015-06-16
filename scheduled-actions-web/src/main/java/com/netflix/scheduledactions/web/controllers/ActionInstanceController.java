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

import com.netflix.scheduledactions.ActionInstance;
import com.netflix.scheduledactions.TriggeredActionsOperator;
import com.netflix.scheduledactions.Execution;
import com.netflix.scheduledactions.exceptions.ExecutionNotFoundException;
import com.netflix.scheduledactions.exceptions.ActionInstanceNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 *
 * @author sthadeshwar
 */
@RestController
@ConditionalOnBean(TriggeredActionsOperator.class)
public class ActionInstanceController {

    @Autowired
    TriggeredActionsOperator triggeredActionsOperator;

    @RequestMapping(value = "/registerActionInstance", method = RequestMethod.POST)
    public String registerActionInstance(@RequestBody ActionInstance actionInstance) {
        return triggeredActionsOperator.registerActionInstance(actionInstance);
    }

    @RequestMapping(value = "/actions/{id}/execute", method = RequestMethod.POST)
    public Execution executeAction(@PathVariable String id) throws ActionInstanceNotFoundException {
        return triggeredActionsOperator.execute(id);
    }

    @RequestMapping(value = "/actions/executions/{id}/cancel", method = RequestMethod.POST)
    public void cancelExecution(@PathVariable String id) throws ActionInstanceNotFoundException, ExecutionNotFoundException {
        triggeredActionsOperator.cancel(id);
    }

    @RequestMapping(value = "/actions", method = RequestMethod.GET)
    public List<ActionInstance> actionInstances(@RequestParam String actionInstanceGroup) {
        return triggeredActionsOperator.getActionInstances(actionInstanceGroup);
    }

    @RequestMapping(value = "/actions/{id}", method = RequestMethod.GET)
    public ActionInstance actionInstance(@PathVariable String id) {
        return triggeredActionsOperator.getActionInstance(id);
    }

    @RequestMapping(value = "/actions/{id}/executions", method = RequestMethod.GET)
    public List<Execution> executions(@PathVariable String id) {
        return triggeredActionsOperator.getExecutions(id);
    }

    @RequestMapping(value = "/actions/executions/{id}", method = RequestMethod.GET)
    public Execution execution(@PathVariable String id) {
        return triggeredActionsOperator.getExecution(id);
    }

    @RequestMapping(value = "/actions/{id}/disable", method = RequestMethod.PUT)
    public void disableActionInstance(@PathVariable String id) throws ActionInstanceNotFoundException {
        triggeredActionsOperator.disableActionInstance(id);
    }

    @RequestMapping(value = "/actions/{id}/enable", method = RequestMethod.PUT)
    public void enableActionInstance(@PathVariable String id) throws ActionInstanceNotFoundException {
        triggeredActionsOperator.enableActionInstance(id);
    }

    @RequestMapping(value = "/actions/{id}/delete", method = RequestMethod.DELETE)
    public void deleteActionInstance(@PathVariable String id) throws ActionInstanceNotFoundException {
        triggeredActionsOperator.deleteActionInstance(id);
    }
}
