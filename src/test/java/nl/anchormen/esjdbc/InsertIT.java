package nl.anchormen.esjdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.Test;

import nl.anchormen.sql4es.model.Utils;

public class InsertIT extends Sql4EsBase {

	private String index = "testindex";
	
	public InsertIT() throws Exception{
		super();
	}

	@Test
	public void withErrors() throws SQLException{
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		try{
			st.executeUpdate("INSERT INTO mytype (col1, col2) VALUES (1, 2.0, 'hi there')");
			assert(false);
		}catch(SQLException sqle){}
		
		try{
			st.executeUpdate("INSERT INTO mytype (col1, col2, col3) VALUES (1, 2.0)");
			assert(false);
		}catch(SQLException sqle){}
		
		try{
			st.executeUpdate("INSERT INTO mytype (col1, col2) VALUES (1, 2.0), (2, 4), (3, 6, 12)");
			assert(false);
		}catch(SQLException sqle){}

		try{
			st.executeUpdate("INSERT INTO mytype (_id, _type) VALUES ('index', 'type')");
			assert(false);
		}catch(SQLException sqle){}
		st.close();
	}
	
	@Test
	public void insertValues() throws SQLException{
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		int res = st.executeUpdate("INSERT INTO "+index+".mytype (_id, col1, col2, col3) VALUES ('myid', 1, 2.0, 'hi there')");
		assertEquals(1, res);
		flush();
		refresh();
		Utils.sleep(1000);
		Statement  st2 = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st2.executeQuery("Select * from mytype");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		int count = 0;
		while(rs.next()){
			assertEquals("myid", rs.getString("_id"));
			count++;
		}
		assertEquals(1, count);
		
		st.executeUpdate("INSERT INTO mytype (col1, col2, col3) VALUES (1, 2.0, 'hi there')");
		flush();
		refresh();
		Utils.sleep(500);
		st2 = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st2.executeQuery("Select * from mytype");
		rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		for(int i=1; i<=rsm.getColumnCount(); i++){
			if(rsm.getColumnName(i).equals("col1")) assertEquals(Types.BIGINT, rsm.getColumnType(i));
			if(rsm.getColumnName(i).equals("col2")) assertEquals(Types.DOUBLE, rsm.getColumnType(i));
			if(rsm.getColumnName(i).equals("col3")) assertEquals(Types.VARCHAR, rsm.getColumnType(i));
		}
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(2, rs.getFloat("col2"), 0.001);
		}
		assertEquals(2, count);
		
		
		st2.executeUpdate("INSERT INTO mytype (col1, col2, col3) VALUES (2, 4.0, 'hi there 4 you 2!')");
		flush();
		refresh();
		Utils.sleep(500);
		rs = st2.executeQuery("Select * from mytype");
		rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		for(int i=1; i<=rsm.getColumnCount(); i++){
			if(rsm.getColumnName(i).equals("col1")) assertEquals(Types.BIGINT, rsm.getColumnType(i));
			if(rsm.getColumnName(i).equals("col2")) assertEquals(Types.DOUBLE, rsm.getColumnType(i));
			if(rsm.getColumnName(i).equals("col3")) assertEquals(Types.VARCHAR, rsm.getColumnType(i));
		}
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(3, count);
		
		st2.executeUpdate("INSERT INTO mytype (col1, col2, col3) VALUES (3, 6.0, 'whatever what'), (4, 8.0, 'whatever what'), (5, 10.0, 'whatever what')");
		flush();
		refresh();
		Utils.sleep(1000);
		rs = st2.executeQuery("Select * from mytype");
		rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		for(int i=1; i<=rsm.getColumnCount(); i++){
			if(rsm.getColumnName(i).equals("col1")) assertEquals(Types.BIGINT, rsm.getColumnType(i));
			if(rsm.getColumnName(i).equals("col2")) assertEquals(Types.DOUBLE, rsm.getColumnType(i));
			if(rsm.getColumnName(i).equals("col3")) assertEquals(Types.VARCHAR, rsm.getColumnType(i));
		}
		count = 0;
		while(rs.next()){
			assertEquals(2*rs.getInt("col1"), rs.getDouble("col2"), 0.001);
			count++;
		}
		assertEquals(6, count);
		st.close();
		st2.close();
	}
/*
	@Test
	public void insertFromSelect() throws Exception{
		String type1 = "type1";
		createIndexTypeWithDocs(index, type1, true, 10);
		refresh();
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		int res = st.executeUpdate("INSERT INTO type2 SELECT intNum from "+type1);
		assertEquals(10, res);
		flush();
		refresh();
		Utils.sleep(1500);
		ResultSet rs = st.executeQuery("Select * from type2");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(4, rsm.getColumnCount());
		int count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(10, count);
		
		res = st.executeUpdate("INSERT INTO type3 SELECT numbers FROM "+type1);
		assertEquals(10, res);
		flush();
		refresh();
		Utils.sleep(1500);
		rs = st.executeQuery("Select * from type3");
		rsm = rs.getMetaData();
		assertEquals(4, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(10, count);
		
		res = st.executeUpdate("INSERT INTO type4 SELECT floatNum, nestedDoc FROM "+type1);
		assertEquals(10, res);
		flush();
		refresh();
		Utils.sleep(1500);
		rs = st.executeQuery("Select * from type4");
		rsm = rs.getMetaData();
		// result has 4 columns instead of the expected 5 because nestedDoc was NULL and is not inserted
		assertEquals(4, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(10, count);
		
		st.close();
	}
	
	@Test
	public void insertFromAggregation() throws Exception{
		String type1 = "type1";
		createIndexTypeWithDocs(index, type1, true, 10);
		refresh();
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		int res = st.executeUpdate("INSERT INTO type2 SELECT distinct bool, avg(intNum) as average from "+type1);
		assertEquals(2, res);
		flush();
		refresh();
		Utils.sleep(1500);
		ResultSet rs = st.executeQuery("Select * from type2");
		System.out.println(rs);
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(4, rsm.getColumnCount());
		int count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(10, count);
	}
	
	@Test
	public void insertNestedFromSelect() throws Exception{
		String type1 = "type1";
		createIndexTypeWithDocs(index, type1, true, 10, 3);
		refresh();
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		int res = st.executeUpdate("INSERT INTO type2 SELECT longNum, nestedDoc FROM "+type1);
		assertEquals(10, res);
		flush();
		refresh();
		Utils.sleep(5000);
		ResultSet rs = st.executeQuery("Select * from type2");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(16, rsm.getColumnCount());
		int count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(10, count);
	}
	*/
}
