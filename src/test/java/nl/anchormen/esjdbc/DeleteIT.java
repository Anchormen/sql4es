package nl.anchormen.esjdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Test;

@ClusterScope(scope=Scope.TEST, numDataNodes=1)
public class DeleteIT extends Sql4EsBase {

	private String index = "testindex";
	private String type = "testdocs";
		
	public DeleteIT() throws Exception {
		super();
	}
		
	@Test
	public void deleteFlat() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		assertRowCount(st, type, 10);
		
		assert(!st.execute("DELETE FROM "+type+" where intNum = 9"));
		flush(); refresh();
		assertRowCount(st, type, 9);
		
		int rows = st.executeUpdate("DELETE FROM "+type+" where bool = false");
		assertEquals(4, rows);
		flush(); refresh();
		assertRowCount(st, type, 5);
		
		rows = st.executeUpdate("DELETE FROM "+type);
		assertEquals(5, rows);
		flush(); refresh();
		assertRowCount(st, type, 0);
	}
	
	@Test
	public void deleteNested() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10, 2);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		assertRowCount(st, type, 10);
		
		assert(!st.execute("DELETE FROM "+type+" where nestedDoc.intNum = 8"));
		flush(); refresh();
		assertRowCount(st, type, 9);
		
		int rows = st.executeUpdate("DELETE FROM "+type+" where docs.bool = true");
		assertEquals(4, rows);
		flush(); refresh();
		assertRowCount(st, type, 5);
		
		rows = st.executeUpdate("DELETE FROM "+type);
		assertEquals(5, rows);
		flush(); refresh();
		assertRowCount(st, type, 0);
		st.close();
	}
	
	private void assertRowCount(Statement st, String type, int expected) throws SQLException{
		ResultSet rs = st.executeQuery("SELECT count(*) FROM "+type);
		rs.next();
		assertEquals(expected, rs.getInt(1));
		st.close();
	}
}
