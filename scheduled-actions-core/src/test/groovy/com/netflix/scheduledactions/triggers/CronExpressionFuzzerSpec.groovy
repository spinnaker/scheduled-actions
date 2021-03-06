/*
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.scheduledactions.triggers

import spock.lang.Specification
import spock.lang.Unroll

class CronExpressionFuzzerSpec extends Specification {

  @Unroll
  def "should fuzz expressions"() {
    when:
    def result = CronExpressionFuzzer.fuzz(triggerId, expression)

    then:
    result == expected

    where:
    triggerId | expression      || expected
    "abcd"    | "H * * * * *"   || "22 * * * * *"
    "abcde"   | "* * H * * *"   || "* * 15 * * *"
    "abcd"    | "H/5 * * * * *" || "22/5 * * * * *"
  }
}

