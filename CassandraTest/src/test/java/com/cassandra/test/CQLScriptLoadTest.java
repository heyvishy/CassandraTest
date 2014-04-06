package com.cassandra.test;

import java.io.IOException;

import com.datastax.driver.core.ResultSet;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CQLScriptLoadTest {

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("sample.cql","TestKeyspace"));

    @Before
    public void setup() throws ConfigurationException, TTransportException, IOException{
    	 EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml");
    }
    
    
    @Test
    public void should_have_started_and_execute_cql_script() throws Exception {
        ResultSet result = cassandraCQLUnit.session.execute("select * from student WHERE id='key1'");
        assertThat(result.iterator().next().getString("name"), is("Shukla"));
    }
}
