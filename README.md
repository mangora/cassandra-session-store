cassandra-session-store
=======================

Implementation of Apache Tomcat Store interface that uses Apache Cassandra as session persistence layer

target
=======================

This small project aims to use Apache Cassandra as persistence layer to be able to store and retrieve user session data
for Apache Tomcat web applications and therefore allowing an HA mechanism that can be transparently plugged in, without
affecting the web application architecture.

pre-requisites
=======================

Even if this library does not affect the architecture of the web application, it is worth noticing that object put in
session should always be serializable pojos to avoid huge serialization and deserialization issues.
This version has been verified and tested for Apache Tomcat 7.

how to use
=======================

1. Assembly the package that comes with pom and copy the jar and all that it is contained in "lib" folder inside catalina
lib folder
2. Configure the context of web application to use PersistentManager and CassandraSessionSore (more on this later)
3. Add a KeySpace and at least one ColumnFamily to your Cassandra cluster
4. Restart Tomcat

context configuration
======================

Add store configuration to PersistentManager as follows:

<Context>
    <Manager className="org.apache.catalina.session.PersistentManager" maxIdleBackup="0" maxIdleSwap="0" minIdleSwap="0">
        <Store className="com.mangora.commons.tomcat.CassandraSessionStore" nodes="[comma separated list of nodes]"
            consistencyLevel="[cassandra consistency level]" keySpace="[keyspace name]" columnFamily="[column family name]"
            ttlSeconds=[ttlSeconds]/>
    </Manager>
</Context>

