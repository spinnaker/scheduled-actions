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

package com.netflix.scheduledactions.persistence

import spock.lang.Shared
import spock.lang.Specification

class SnappyCodecSpec extends Specification {

    @Shared Codec codec = new SnappyCodec()

    void 'decompression of compressed bytes returns the original bytes'() {
        setup:
        String inputString = "Testing snappy compression and decompression"
        byte[] input = inputString.bytes

        when:
        byte[] compressed = codec.compress(input)

        then:
        noExceptionThrown()
        compressed != null
        !Arrays.equals(input, compressed)

        when:
        byte[] decompressed = codec.decompress(compressed)

        then:
        noExceptionThrown()
        decompressed != null
        Arrays.equals(input, decompressed)
    }

}