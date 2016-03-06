package nl.anchormen.esjdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Test;

@ClusterScope(scope=Scope.TEST, numDataNodes=1)
public class TableReferenceIT extends Sql4EsBase {

	private String index = "testindex";
	private String type = "testdocs";
	
	public TableReferenceIT() throws Exception {
		super();
	}

	@Test
	public void simpleQueries() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		
		ResultSet rs = st.executeQuery("select * FROM "+type+" as t");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(15, rsm.getColumnCount());
		int count = 0;
		while(rs.next()) count++;
		assertEquals(10, count);
		rs.close();
		
		rs = st.executeQuery("select t.* FROM "+type+" as t");
		rsm = rs.getMetaData();
		assertEquals(15, rsm.getColumnCount());
		count = 0;
		while(rs.next()) count++;
		assertEquals(10, count);
		
		rs = st.executeQuery("select "+type+".* FROM "+type+" as t");
		rsm = rs.getMetaData();
		assertEquals(15, rsm.getColumnCount());
		count = 0;
		while(rs.next()) count++;
		assertEquals(10, count);

		rs = st.executeQuery("select t.intNum, t.text FROM "+type+" t");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next()) count++;
		assertEquals(10, count);
		
		rs = st.executeQuery("select t.intNum, t.text FROM "+type+" t WHERE t.intNum > 5");
		count = 0;
		while(rs.next()) count++;
		assertEquals(4, count);
		
		rs = st.executeQuery("select t.intNum, t.text FROM "+type+" t WHERE t.intNum > 5 AND "+type+".intNum < 8");
		count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);

		st.close();
	}
	
	@Test
	public void aggregationQueries() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		
		ResultSet rs = st.executeQuery("select distinct bool FROM "+type+" as t");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		int count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("select distinct t.bool FROM "+type+" as t");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("select t.bool, sum(t.intNum) FROM "+type+" as t group by bool");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("select t.bool, sum(t.intNum) FROM "+type+" as t group by t.bool");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("select bool, sum(intNum) FROM "+type+" t group by t.bool");
		rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);
		rs.close();

		st.close();
	}
}
