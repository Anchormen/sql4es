package nl.anchormen.esjdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;

import org.elasticsearch.test.ESIntegTestCase.*;
import org.junit.Test;

/**
 * Tests the DatabaseMetaData functionality such as the description of schemas(indexes), tables(types) and columns (fields)
 * 
 * @author cversloot
 *
 */
@ClusterScope(scope=Scope.TEST, numDataNodes=1)
public class DatabaseMetadataIT extends Sql4EsBase {
	
	public DatabaseMetadataIT() throws Exception {
		super();
	}

	@Test
	public void testReadingProperties() throws Exception{
		Driver driver = DriverManager.getDriver("jdbc:sql4es://localhost:9300/index");
		DriverPropertyInfo[] info = driver.getPropertyInfo("jdbc:sql4es://localhost:9300/index", null);
		assertEquals(8, info.length);
		for(DriverPropertyInfo i : info){
			if(i.name.equals("query.timeout.ms")) assertEquals("10000", i.value);
		}
		
		info = driver.getPropertyInfo("jdbc:sql4es://localhost:9300/index?query.cache.table=tmp", null);
		assertEquals(8, info.length);
		for(DriverPropertyInfo i : info){
			if(i.name.equals("query.cache.table")) assertEquals("tmp", i.value);
		}
	}
	
	@Test
	public void testReadingSchemas() throws Exception{
		createIndex("index1");
		createIndex("index2");
		refresh();
		Connection conn = DriverManager.getConnection("jdbc:sql4es://localhost:9300/index?test");
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getSchemas();
		int count = 0;
		while(rs.next()){
			assert(rs.getString(1).startsWith("index"));
			count++;
		}
		assertEquals(2, count);
	}
	
	@Test
	public void testReadingTables() throws Exception{
		createIndexTypeWithDocs("index1", "mytype", true, -1);
		Connection conn = DriverManager.getConnection("jdbc:sql4es://localhost:9300/index?test");
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getTables(null, "index1", null, null);
		int count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(3, count);
	}
	
	@Test
	public void testReadingColumnsFlat() throws Exception{
		createIndexTypeWithDocs("index1", "mytype", true, -1);
		Connection conn = DriverManager.getConnection("jdbc:sql4es://localhost:9300/index?test");
		DatabaseMetaData md = conn.getMetaData();
		
		ResultSet rs = md.getColumns(null, "index1", null, null);
		int count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(18, count);
		
		// reading empty type
		rs = md.getColumns(null, "index1", "_default_", "%");
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(3, count);
	}

	@Test
	public void testReadingColumnsNested() throws Exception{
		String index = "index2";
		String type = "type";
		createIndexTypeWithDocs(index, type, true, 1 , 2);
		Connection conn = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test");
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getColumns(null, "index2", null, null);
		int count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(38, count);
	}
}
