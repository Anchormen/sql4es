package nl.am.trials;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import nl.anchormen.sql4es.model.Utils;

public class ConnectionTest {

	public static void main(String[] args){
		ResultSet rs;
		try {
			Class.forName("nl.am.sql4es.jdbc.ESDriver");
			Connection conn = DriverManager.getConnection("jdbc:sql4es://localhost:9300/rb?"+Utils.PROP_DEFAULT_ROW_LENGTH+"=100");
			Statement st = conn.createStatement();
			//st.execute("insert into testtype (myString) VALUES ('David Bowie, whose incomparable sound and chameleon-like ability to reinvent himself made him a pop music fixture for more than four decades, has died. He was 69. Bowie died Sunday after an 18-month battle with cancer, his publicist Steve Martin told CNN.')");
			rs = st.executeQuery("select var as V from bla where V in (1)");
			System.out.println(rs);
			//st.execute("DROP table testindex");
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
