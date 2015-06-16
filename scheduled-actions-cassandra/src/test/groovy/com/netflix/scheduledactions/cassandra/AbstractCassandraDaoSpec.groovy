package com.netflix.scheduledactions.cassandra

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.scheduledactions.ActionInstance
import com.netflix.scheduledactions.Action
import com.netflix.astyanax.Keyspace
import com.netflix.scheduledactions.cassandra.AbstractCassandraDao
import spock.lang.Specification
/**
 *
 * @author sthadeshwar
 */
class AbstractCassandraDaoSpec extends Specification {

    void 'generic type is correctly computed for a domain class type (ActionInstance)'() {
        when:
        TestDao1 testDao = new TestDao1(null, null)

        then:
        testDao.parameterClass == ActionInstance.class
    }

    void 'column family name is correctly computed for a domain class type (ActionInstance)'() {
        when:
        TestDao1 testDao = new TestDao1(null, null)

        then:
        testDao.columnFamilyName == 'action_instance'
    }

    void 'column family name is correctly computed for a domain class type (Action)'() {
        when:
        TestDao2 testDao = new TestDao2(null, null)

        then:
        testDao.columnFamilyName == 'action'
    }

    void 'generic type is correctly computed for a primitive type (byte[])'() {
        when:
        TestDao3 testDao = new TestDao3(null, null, "action_state")

        then:
        testDao.parameterClass == byte[].class
    }

    void 'column family name is correctly computed for a primitive type (byte[])'() {
        when:
        TestDao3 testDao = new TestDao3(null, null, "action_state")

        then:
        testDao.columnFamilyName == 'action_state'
    }

    class TestDao1 extends AbstractCassandraDao<ActionInstance> {
        TestDao1(Keyspace keyspace, ObjectMapper objectMapper) {
            super(keyspace, objectMapper)
        }
    }

    class TestDao2 extends AbstractCassandraDao<Action> {
        TestDao2(Keyspace keyspace, ObjectMapper objectMapper) {
            super(keyspace, objectMapper)
        }
    }

    class TestDao3 extends AbstractCassandraDao<byte[]> {
        TestDao3(Keyspace keyspace, ObjectMapper objectMapper, String columnFamilyName) {
            super(keyspace, objectMapper, columnFamilyName)
        }
    }
}
