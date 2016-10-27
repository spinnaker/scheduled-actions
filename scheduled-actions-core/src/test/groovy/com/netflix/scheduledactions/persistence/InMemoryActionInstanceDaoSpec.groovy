package com.netflix.scheduledactions.persistence

/*
 * Copyright 2016 Netflix, Inc.
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

import com.netflix.scheduledactions.ActionInstance
import rx.functions.Action1
import spock.lang.Specification

class InMemoryActionInstanceDaoSpec extends Specification {

  static class TestAction1 implements Action1<ActionInstance> {
    @Override
    void call(ActionInstance actionInstance) {}
  }

  def 'should create and update ActionInstance'() {
    given:
    def subject = new InMemoryActionInstanceDao()

    when:
    subject.createActionInstance(actionInstance.group, actionInstance)

    then:
    actionInstance == subject.getActionInstance(actionInstance.id)

    when:
    subject.updateActionInstance(actionInstance)

    then:
    actionInstance == subject.getActionInstance(actionInstance.id)

    when:
    def result = subject.getActionInstances(actionInstance.group)

    then:
    result == [actionInstance]

    where:
    actionInstance << [
      ActionInstance.newActionInstance()
        .withName("ID & Group defined")
        .withId(UUID.randomUUID().toString())
        .withGroup(UUID.randomUUID().toString())
        .build(),
      ActionInstance.newActionInstance()
        .withName("Group defined")
        .withGroup(UUID.randomUUID().toString())
        .build()
    ]

  }
}