package nl.am.esjdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class NotSupportedIT extends Sql4EsBase {

	private String index = "testindex";
	
	public NotSupportedIT() throws Exception {
		super();
	}
	
	@Test
	public void testUnsupportedSyntax() throws SQLException{
		createIndex(index);
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		
		try{
			st.execute("SELECT something FROM type, (SELECT max(x) FROM type2) as type2 WHERE var = tpe2");
			assert(false);
		}catch(Exception e){}

		try{
			st.execute("SELECT something FROM type1, type2 where col1 = col2");
			assert(false);
		}catch(Exception e){ }
		
		try{
			st.executeUpdate("SELECT something FROM type1");
			assert(false);
		}catch(Exception e){ }
	}
	
}
