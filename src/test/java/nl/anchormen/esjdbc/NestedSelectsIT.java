package nl.anchormen.esjdbc;

import java.sql.Array;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Test;

import nl.anchormen.sql4es.model.Utils;

@ClusterScope(scope=Scope.TEST, numDataNodes=1)
public class NestedSelectsIT extends Sql4EsBase {

	private String index = "testindex";
	private String type = "testdocs";
	
	public NestedSelectsIT() throws Exception {
		super();
	}

	@Test
	public void testSingleObjectLateral() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10, 1);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select * from "+type+" where nestedDoc.intNum <= 3");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(26, rsm.getColumnCount());
		assertEquals(Types.VARCHAR, rsm.getColumnType(1));
		
		int count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(5, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select nestedDoc from "+type+" where nestedDoc.intNum <= 3");
		rsm = rs.getMetaData();
		assertEquals(12, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			count++;
			assert(rs.getInt(1) <= 3);
		}
		assertEquals(5, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select nestedDoc.intNum from "+type+" where nestedDoc.intNum <= 3");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			count++;
			assert(rs.getInt(1) <= 3);
		}
		assertEquals(5, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.intNum from "+type+" where nestedDoc.intNum <= 3");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(5, count);
	}
	
	@Test
	public void testMultipleObjectsLateral() throws Exception{
		createIndexTypeWithDocs(index, type, true, 5, 4);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select * from "+type);
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(37, rsm.getColumnCount());
		int count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(15, count);
		
		rs = st.executeQuery("select docs from "+type);
		rsm = rs.getMetaData();
		assertEquals(12, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			assert(rs.getObject("docs.intNum") instanceof Integer);
			count++;
		}
		assertEquals(15, count);
		
		rs = st.executeQuery("select docs.intNum from "+type);
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.BIGINT, rsm.getColumnType(1));
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(rs.getLong(1), rs.getInt("docs.intNum"));
		}
		assertEquals(15, count);

		rs = st.executeQuery("select docs.intNum as di, docs from "+type);
		rsm = rs.getMetaData();
		assertEquals(12, rsm.getColumnCount());
		assertEquals(Types.BIGINT, rsm.getColumnType(1));
		count = 0;
		while(rs.next()){
			assert(rs.getObject("docs.numbers") instanceof Array);
			assertEquals(rs.getObject(1), rs.getObject("di"));
			assertEquals(rs.getObject("docs.intNum"), rs.getObject("di"));
			count++;
		}
		assertEquals(15, count);
		
		rs = st.executeQuery("select docs.nestedDoc, docs.numbers from "+type+" where shortNum = 1");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			assertEquals(null, rs.getObject("docs.nestedDoc"));
			assertEquals(null, rs.getObject(1));
			count++;
		}
		assertEquals(3, count);
	}
	
	@Test
	public void testSingleObjectNested() throws Exception{
		createIndexTypeWithDocs(index, type, true, 5, 4);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test&"+Utils.PROP_RESULT_NESTED_LATERAL+"=false").createStatement();
		ResultSet rs = st.executeQuery("select * from "+type);
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(15, rsm.getColumnCount());
		int count = 0;
		while(rs.next()){
			assert(rs.getObject("nestedDoc") instanceof ResultSet);
			assert(rs.getObject("docs") instanceof ResultSet);
			ResultSet rsNested = (ResultSet)rs.getObject("docs");
			int count2 = 0;
			while(rsNested.next())count2++;
			assertEquals(3, count2);
			count++;
		}
		assertEquals(5, count);
		
		rs = st.executeQuery("select docs as d from "+type+" where shortNum >= 3 ");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.JAVA_OBJECT, rsm.getColumnType(1));
		count = 0;
		while(rs.next()){
			assert(rs.getObject("docs") instanceof ResultSet);
			assert(rs.getObject("d") instanceof ResultSet);
			ResultSet rsNested = (ResultSet)rs.getObject(1);
			int count2 = 0;
			while(rsNested.next())count2++;
			assertEquals(3, count2);
			count++;
		}
		assertEquals(2, count);
		
		rs = st.executeQuery("select docs.numbers as dn from "+type+" where shortNum >= 1 AND intNum < 4 ");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.JAVA_OBJECT, rsm.getColumnType(1));
		count = 0;
		while(rs.next()){
			assert(rs.getObject("docs") instanceof ResultSet);
			assertEquals(rs.getObject("docs"), rs.getObject("dn"));
			count++;
		}
		assertEquals(3, count);
		
		rs = st.executeQuery("select docs.shortNum, nestedDoc.intNum from "+type);
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		assertEquals(Types.JAVA_OBJECT, rsm.getColumnType(1));
		count = 0;
		while(rs.next()){
			assert(rs.getObject("nestedDoc") instanceof ResultSet);
			assert(rs.getObject("docs") instanceof ResultSet);
			count++;
		}
		assertEquals(5, count);
		
		rs = st.executeQuery("select nestedDoc.intNum, nestedDoc from "+type);
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.JAVA_OBJECT, rsm.getColumnType(1));
		count = 0;
		while(rs.next()){
			assert(rs.getObject("nestedDoc") instanceof ResultSet);
			ResultSet rs2 = (ResultSet)rs.getObject("nestedDoc");
			assertEquals(12, rs2.getMetaData().getColumnCount());
			count++;
		}
		assertEquals(5, count);
	}
	
}
