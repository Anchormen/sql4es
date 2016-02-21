package nl.anchormen.esjdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

public class UpdateIT extends Sql4EsBase {

	private String index = "testindex";
	private String type = "testtype";
	
	public UpdateIT() throws Exception{
		super();
	}

	@Test
	public void update() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10, 2);
		refresh();
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		int res = st.executeUpdate("UPDATE "+type+" SET intNum = 100 WHERE intNum > 7");
		assertEquals(2, res);
		refresh();
		
		ResultSet rs = st.executeQuery("SELECT count(1) FROM "+type+" WHERE intNum = 100");
		if(rs.next()) assertEquals(2, rs.getLong(1));
		else assert(false);
		rs.close();
		
		res = st.executeUpdate("UPDATE "+type+" SET nestedDoc.text = 'bool is true' WHERE nestedDoc.bool = true");
		assertEquals(5, res);
		refresh();
		
		rs = st.executeQuery("SELECT count(1) FROM "+type+" WHERE nestedDoc.text = 'bool is true'");
		if(rs.next()) assertEquals(5, rs.getLong(1));
		else assert(false);
		rs.close();
		
		st.close();
	}

}
