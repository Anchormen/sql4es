package nl.anchormen.esjdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Test;

@ClusterScope(scope=Scope.TEST, numDataNodes=1)
public class BatchIT extends Sql4EsBase {

	private String index = "testindex";
		
	public BatchIT() throws Exception {
		super();
	}
	
	@Test
	public void testAddingRemoving() throws SQLException{
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		st.addBatch("CREATE TABLE monkey (_id String)");
		st.addBatch("INSERT INTO monkey (myInt) VALUES (1)");
		st.addBatch("INSERT INTO monkey (myInt) VALUES (2), (3), (4)");
		st.clearBatch();
		int[] res = st.executeBatch();
		assertEquals(0, res.length);
		
		st.executeUpdate("CREATE TABLE monkey (_id String)");
		st.addBatch("INSERT INTO monkey (myInt) VALUES (1)");
		st.addBatch("INSERT INTO monkey (myInt) VALUES (2), (3), (4)");
		res = st.executeBatch();
		assertEquals(2, res.length);
		flush();
		refresh();
		ResultSet rs = st.executeQuery("SELECT myInt FROM monkey");
		int count = 0;
		while(rs.next()){
			count++;
			assert(rs.getInt(1) >= 1 && rs.getInt(1) <= 4 );
		}
		assertEquals(4, count);

		st.addBatch("INSERT INTO monkey (myInt) VALUES (5), (6)");
		st.addBatch("INSERT INTO monkey (myInt) VALUES (7), (8)");
		st.addBatch("DELETE FROM monkey WHERE myInt <= 2"); // not possible to test myInt >= 5 because a flush is required
		res = st.executeBatch();
		assertEquals(3, res.length);
		flush();
		refresh();
		rs = st.executeQuery("SELECT myInt FROM monkey");
		count = 0;
		while(rs.next()){
			count++;
			assert(rs.getInt(1) > 2 );
		}
		assertEquals(6, count);

		st.addBatch("CREATE TABLE monkey2 (_id String)");
		st.addBatch("INSERT INTO monkey2 (myInt) VALUES (1)");
		st.addBatch("INSERT INTO monkey2 (myInt) VALUES (2), (3), (4)");
		st.addBatch("DROP TABLE "+index);
		res = st.executeBatch();
		assertEquals(4, res.length);
		flush();
		refresh();
		try{
			rs = st.executeQuery("SELECT myInt FROM monkey2");
			assert(false);
		}catch(Exception e){}
		st.close();
	}
	

}
