package com.netflix.scheduledactions.persistence.cassandra

import com.netflix.scheduledactions.persistence.Codec
import com.netflix.scheduledactions.persistence.SnappyCodec
import spock.lang.Shared
import spock.lang.Specification
/**
 *
 * @author sthadeshwar
 */
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