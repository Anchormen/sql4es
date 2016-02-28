package nl.anchormen.esjdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
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
		
		int count = 0;
		while(rs.next()) count++;
		assertEquals(10, count);
		rs.close();
		
		rs = st.executeQuery("select t.* FROM "+type+" as t");
		count = 0;
		while(rs.next()) count++;
		assertEquals(10, count);
		st.close();
	}
}
