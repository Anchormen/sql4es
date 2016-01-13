package nl.am.esjdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.After;
import org.junit.Test;

import nl.anchormen.sql4es.model.Utils;

public class CreateIT extends Sql4EsBase {

	private String index = "testindex";
	private String type = "type";
	
	public CreateIT() throws Exception {
		super();
	}
	/*
	@Test
	public void createSimple() throws Exception{
		createIndex(index);
		Connection conn = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test");
		Statement st = conn.createStatement();
		boolean res = st.execute("CREATE TABLE simpletype (myString \"type:String, index:not_analyzed\", myInt \"type:Integer\", myDate \"type:date\")");
		assert(!res);
		flush();
		refresh();
		DatabaseMetaData dmd = conn.getMetaData();
		ResultSet rs = dmd.getTables(null, index, "simpletype",null);
		int count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);
		rs.close();
		
		res = st.execute("INSERT INTO simpletype (myString, myInt, myDate) "
				+ "VALUES ('abc', 1, '2016-01-09T16:35:46'), ('def', 2, '2016-01-10T12:26:12'), ('ghi', 3, '2016-01-11T09:01:07')");
		assert(!res);
		flush();
		refresh();
		
		rs = st.executeQuery("SELECT * FROM simpletype");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(Types.VARCHAR, rsm.getColumnType(4));
		assertEquals(Types.DATE, rsm.getColumnType(5));
		assertEquals(Types.INTEGER, rsm.getColumnType(6));
		count = 0;
		while(rs.next()){
			count++;
			assert(rs.getInt("myInt") > 0);
			rs.getDate("myDate");
		}
		assertEquals(3, count);
	}

	@Test
	public void createAs() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		
		Connection conn = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test");
		Statement st = conn.createStatement();
		int res = st.executeUpdate("CREATE TABLE type2 AS SELECT * from "+type);
		assertEquals(10, res);
		flush();
		refresh();
		Utils.sleep(1000);
		
		ResultSet rs = st.executeQuery("SELECT * FROM type2");
		System.out.println(rs);
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(13, rsm.getColumnCount());
	}
	
	*/
	
}
