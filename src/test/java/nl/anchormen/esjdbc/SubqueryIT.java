package nl.anchormen.esjdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Test;

@ClusterScope(scope=Scope.TEST, numDataNodes=1)
public class SubqueryIT extends Sql4EsBase {

	private String index = "testindex";
	private String type = "testdocs";
	
	public SubqueryIT() throws Exception {
		super();
	}
	
	@Test
	public void testNestedSelects() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10, 0);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select * from (select * from "+type+") ");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(15, rsm.getColumnCount());
		assertEquals(Types.VARCHAR, rsm.getColumnType(1));
		int count = 0;
		while(rs.next())count++;
		assertEquals(10, count);
		rs.close();
		
		rs = st.executeQuery("select * from (select _score, * from "+type+") ");
		rsm = rs.getMetaData();
		assertEquals(16, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(10, count);
		rs.close();
		
		rs = st.executeQuery("select * from (select * from "+type+" limit 1)");
		count = 0;
		while(rs.next())count++;
		assertEquals(1, count);
		rs.close();
		
		rs = st.executeQuery("select _id, intNum, bool FROM (select * from "+type+" where intNum > 1)");
		rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(8, count);
		rs.close();
		
		rs = st.executeQuery("select * FROM (select _id, intNum, bool from "+type+" where intNum > 1)");
		rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(8, count);
		rs.close();
		
		rs = st.executeQuery("select _id, bool FROM (select _id, intNum, bool from "+type+" where intNum > 1) WHERE intNum < 8");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(6, count);
		rs.close();
		
		rs = st.executeQuery("select _id, bool FROM (select _id, intNum, bool from "+type+" where intNum > 1 order by intNum asc limit 5) WHERE intNum < 8");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(5, count);
		rs.close();
		
		rs = st.executeQuery("select _id, bool FROM (select _id, intNum, bool from "+type+" where intNum > 1) WHERE intNum < 8 limit 3");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(3, count);
		rs.close();
		
		rs = st.executeQuery("select _id, b boolean FROM (select _id, intNum, bool as b from "+type+" where intNum > 1 order by intNum asc limit 5) WHERE intNum < 8");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(5, count);
		rs.close();
		
		st.close();
	}
	
	@Test
	public void testNestedSelectsOnAgg() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10, 0);

		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select * from (select distinct bool from "+type+") ");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		int count = 0;
		while(rs.next())count++;
		assertEquals(2, count);
		rs.close();

		rs = st.executeQuery("select * from (select distinct bool, count(1) from "+type+") ");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(2, count);
		rs.close();

		rs = st.executeQuery("select bool from (select distinct bool, count(*) from "+type+") ");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("select bool, s from (select distinct bool, sum(intNum) as s from "+type+")");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("select s from (select distinct bool, sum(intNum) as s from "+type+") ");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("select * from (select distinct bool, AVG(intNum) from "+type+" where bool = true) ");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(1, count);
		rs.close();
		
		rs = st.executeQuery("select * from (select distinct bool, AVG(intNum) a from "+type+" having a >= 5)");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(1, count);
		rs.close();
		
		rs = st.executeQuery("select * from (select distinct bool, AVG(intNum)/4 as a from "+type+") limit 1");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next())count++;
		assertEquals(1, count);
		rs.close();
		
		st.close();
	}
}
