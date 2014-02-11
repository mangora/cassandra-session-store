package com.mangora.commons.tomcat;

import org.apache.catalina.core.StandardContext;
import org.apache.catalina.session.PersistentManager;
import org.apache.catalina.session.StandardSession;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * User: Cristiano
 * Date: 07/02/14
 * Time: 10.27
 */
public class CassandraSessionStoreTest {
    private static final String TEST_SERIALIZABLE = "testSerializable";
    private CassandraSessionStore cassandraSessionStore;
    private StandardSession toWriteSession;

    @Before
    public void setUp() throws Exception {
        cassandraSessionStore = new CassandraSessionStore();
        cassandraSessionStore.setManager(new PersistentManager());
        cassandraSessionStore.getManager().setContainer(new StandardContext());
        toWriteSession = new StandardSession(cassandraSessionStore.getManager());
        toWriteSession.setValid(true);
    }

    @Test
    public void testSessionSerialization() throws IOException {
        toWriteSession.setAttribute(TEST_SERIALIZABLE, new ArrayList<Object>());
        byte[] serializedSession = cassandraSessionStore.sessionToBytes(toWriteSession);
        StandardSession readSession = cassandraSessionStore.bytesToSession(serializedSession);
        assertNotNull(readSession);
        assertEquals(toWriteSession.getAttribute(TEST_SERIALIZABLE), readSession.getAttribute(TEST_SERIALIZABLE));
    }
}
