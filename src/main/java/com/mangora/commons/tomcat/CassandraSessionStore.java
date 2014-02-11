package com.mangora.commons.tomcat;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.catalina.Container;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.session.StoreBase;
import org.apache.catalina.util.CustomObjectInputStream;
import org.scale7.cassandra.pelops.*;
import org.scale7.cassandra.pelops.exceptions.NotFoundException;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool;
import org.scale7.cassandra.pelops.pool.IThriftPool;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: morpurgo
 * Date: 31/01/14
 * Time: 14.54
 */
public class CassandraSessionStore extends StoreBase {

    private IThriftPool pelopsPool;

    private String columnFamily;
    private String rowKey           =   "tomcatSessionKey";
    @SuppressWarnings("FieldCanBeLocal")
    private ObjectMapper mapper;

    private String nodes;
    private String consistencyLevel;
    private String keySpace;
    private Long ttlSeconds = -1L;

    public CassandraSessionStore() {

    }

    @Override
    public int getSize() throws IOException {
        final Selector selector = pelopsPool.createSelector();
        return selector.getColumnsFromRow(columnFamily, rowKey, false, ConsistencyLevel.valueOf(consistencyLevel)).size();
    }

    @Override
    public String[] keys() throws IOException {
        final Selector selector = pelopsPool.createSelector();
        List<Column> columnList = selector.getColumnsFromRow(columnFamily, rowKey, false, ConsistencyLevel.valueOf(consistencyLevel));
        List<String> keyList = new LinkedList<>();
        for (Column col : columnList) {
            keyList.add(Bytes.toUTF8(col.getName()));
        }
        return keyList.toArray(new String[keyList.size()]);
    }

    @Override
    public Session load(String id) throws ClassNotFoundException, IOException {
        Session session =   null;
        byte[] sessionBytes = getExpiringData(id);
        if (sessionBytes != null) {
            session = bytesToSession(sessionBytes);
        }

        return session;
    }

    @Override
    public void remove(String id) throws IOException {
        Mutator mutator = pelopsPool.createMutator();
        mutator.deleteColumn(columnFamily, rowKey, id);
    }

    @Override
    public void clear() throws IOException {
        RowDeletor rowDeletor = pelopsPool.createRowDeletor();
        rowDeletor.deleteRow(columnFamily, rowKey, ConsistencyLevel.valueOf(consistencyLevel));
    }

    @Override
    public void save(Session session) throws IOException {
        byte[] sessionBytes    = sessionToBytes((StandardSession) session);
        saveExpiringData(session.getId(), sessionBytes, ttlSeconds);
    }

    private void saveExpiringData(final String columnName, final byte[] value, final Long ttlSeconds) {
        final Mutator mutator = pelopsPool.createMutator(System.currentTimeMillis() * 1000, ttlSeconds.intValue());
        final Column column = mutator.newColumn(Bytes.fromUTF8(columnName), Bytes.fromByteArray(value), ttlSeconds.intValue());
        mutator.writeColumn(columnFamily, rowKey, column);
        mutator.execute(ConsistencyLevel.valueOf(consistencyLevel));
    }

    private byte[] getExpiringData(final String columnName) {
        final Selector selector = pelopsPool.createSelector();
        byte[] value = null;
        try {
            final Column col = selector.getColumnFromRow(columnFamily,rowKey,
                    Bytes.fromUTF8(columnName), ConsistencyLevel.valueOf(consistencyLevel));
            if (col != null) {
                value = col.getValue();
            }
        } catch (NotFoundException ignored) {
            // ignore
        }

        return value;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getNodes() {
        return nodes;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getConsistencyLevel() {
        return consistencyLevel;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setConsistencyLevel(String consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getKeySpace() {
        return keySpace;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setKeySpace(String keySpace) {
        this.keySpace = keySpace;
    }

    @Override
    protected synchronized void startInternal() throws LifecycleException {
        Cluster cluster =   new Cluster(nodes, 9160);
        pelopsPool = new CommonsBackedPool(cluster, keySpace);
        mapper = new ObjectMapper();

        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        super.startInternal();
    }

    public byte[] sessionToBytes(StandardSession standardSession) {
        byte[] result = new byte[0];

        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            standardSession.writeObjectData(objectOutputStream);
            objectOutputStream.close();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public StandardSession bytesToSession(byte[] bytes) throws IOException {
        Loader loader = null;
                ClassLoader classLoader = null;
        StandardSession session = (StandardSession) manager.createEmptySession();
        try {

            ObjectInputStream objectInputStream;
            Container container = manager.getContainer();
            if (container != null)
                loader = container.getLoader();
            if (loader != null)
                classLoader = loader.getClassLoader();
            if (classLoader != null)
                objectInputStream = new CustomObjectInputStream(new ByteArrayInputStream(bytes), classLoader);
            else
                objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
            session.setManager(manager);
            session.readObjectData(objectInputStream);
        } catch (IOException | ClassNotFoundException | ClassCastException e) {
            if(manager.getContainer().getLogger().isErrorEnabled()){
                manager.getContainer().getLogger().error(e, e);
            }
        }
        return session;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setTtlSeconds(Long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }
}
