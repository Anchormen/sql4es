package nl.anchormen.esjdbc;

import java.sql.Array;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Test;

import nl.anchormen.sql4es.model.Utils;

@ClusterScope(scope=Scope.TEST, numDataNodes=1)
public class SimpleSelectsIT extends Sql4EsBase {
	
	private String index = "testindex";
	private String type = "testdocs";
	
	public SimpleSelectsIT() throws Exception {
		super();
	}
	
	@Test
	public void resultTypingFlat() throws Exception{
		createIndexTypeWithDocs(index, type, true, 1);
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select _id, _type, _index, intNum, longNum, floatNum, doubleNum,"
				+ "shortNum, byteNum, bool, text, date, nestedDoc, numbers, docs FROM "+type+" limit 1");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(15, rsm.getColumnCount());
		assertEquals(Types.VARCHAR, rsm.getColumnType(1));
		assertEquals(Types.VARCHAR, rsm.getColumnType(2));
		assertEquals(Types.VARCHAR, rsm.getColumnType(3));
		assertEquals(Types.INTEGER, rsm.getColumnType(4));
		assertEquals(Types.BIGINT, rsm.getColumnType(5));
		assertEquals(Types.FLOAT, rsm.getColumnType(6));
		assertEquals(Types.DOUBLE, rsm.getColumnType(7));
		assertEquals(Types.SMALLINT, rsm.getColumnType(8));
		assertEquals(Types.TINYINT, rsm.getColumnType(9));
		assertEquals(Types.BOOLEAN, rsm.getColumnType(10));
		assertEquals(Types.VARCHAR, rsm.getColumnType(11));
		assertEquals(Types.DATE, rsm.getColumnType(12));
		assertEquals(Types.JAVA_OBJECT, rsm.getColumnType(13));
		assertEquals(Types.ARRAY, rsm.getColumnType(14));
		assertEquals(Types.JAVA_OBJECT, rsm.getColumnType(15));
	}
	
	/**
	 * The nested TestDocument has a dynamic mapping and is partially empty which is why it 
	 * has a different (more basic) mapping than above 
	 * @throws Exception
	 */
	@Test
	public void resultTypingNested() throws Exception{
		createIndexTypeWithDocs(index, type, true, 1, 1);
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select nestedDoc FROM "+type+" limit 1");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(12, rsm.getColumnCount());
		assertEquals(Types.BIGINT, rsm.getColumnType(1));
		assertEquals(Types.BIGINT, rsm.getColumnType(2));
		assertEquals(Types.BIGINT, rsm.getColumnType(3));
		assertEquals(Types.BOOLEAN, rsm.getColumnType(4));
		assertEquals(Types.OTHER, rsm.getColumnType(5));
		assertEquals(Types.DOUBLE, rsm.getColumnType(6));
		assertEquals(Types.BIGINT, rsm.getColumnType(7));
		assertEquals(Types.DOUBLE, rsm.getColumnType(8));
		assertEquals(Types.ARRAY, rsm.getColumnType(9));
		assertEquals(Types.VARCHAR, rsm.getColumnType(10));
		assertEquals(Types.OTHER, rsm.getColumnType(11));
		assertEquals(Types.BIGINT, rsm.getColumnType(12));
	}
	
	@Test
	public void selectAll() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select * from "+type);
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(15, rsm.getColumnCount());
		assertEquals(Types.VARCHAR, rsm.getColumnType(1));
		
		int count = 0;
		while(rs.next()){
			count++;
			assert(rs.getObject("numbers") instanceof Array);
			assertEquals(5, ((Object[])rs.getArray("numbers").getArray() ).length);
			ResultSet rs2 = rs.getArray("numbers").getResultSet();
			int count2 = 0;
			while(rs2.next()) count2++;
			assertEquals(5, count2);
			
			rs2 = rs.getArray("numbers").getResultSet(1,2);
			count2 = 0;
			while(rs2.next()) count2++;
			assertEquals(2, count2);
			
			rs2 = rs.getArray("numbers").getResultSet(-1,4);
			count2 = 0;
			while(rs2.next()) count2++;
			assertEquals(4, count2);
			
			rs2 = rs.getArray("numbers").getResultSet(3,40);
			count2 = 0;
			while(rs2.next()) count2++;
			assertEquals(2, count2);
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select * from "+type+" limit 5");
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(5, count);
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void selectRanges() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		
		// single range
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select _id, intNum, text as txt from "+type+" where intNum >= 5");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		assertEquals(Types.INTEGER, rsm.getColumnType(2));
		
		int count = 0;
		while(rs.next()){
			assert(rs.getInt("intNum") >= 5);
			assert(rs.getString("txt").contains(""+rs.getInt(2)));
			count++;
		}
		assertEquals(5, count);
		
		// multi range
		rs = st.executeQuery("select _id, floatNum, text from "+type+" where shortNum > 3 AND byteNum <= 8");
		rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		assertEquals(Types.FLOAT, rsm.getColumnType(2));
		
		count = 0;
		while(rs.next()){
			assert(rs.getFloat("floatNum") > 3 && rs.getFloat(2) <= 8);
			count++;
		}
		assertEquals(5, count);

		// multi range BETWEEN
		rs = st.executeQuery("select _id, floatNum, text from "+type+" where shortNum between 4 AND 8");
		rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		assertEquals(Types.FLOAT, rsm.getColumnType(2));
		
		count = 0;
		while(rs.next()){
			assert(rs.getFloat("floatNum") > 3 && rs.getFloat(2) <= 8);
			count++;
		}
		assertEquals(5, count);
		
		rs = st.executeQuery("select floatNum from "+type+" where intNum NOT BETWEEN 3 AND 8");
		rsm = rs.getMetaData();
		count = 0;
		while(rs.next()){
			assert(rs.getFloat("floatNum") < 3 || rs.getFloat(1) > 8);
			count++;
		}
		assertEquals(4, count);
		
		// date range
		rs = st.executeQuery("select _id, date, text from "+type+" where date > '2016-03-10' AND date < '2016-07-31'");
		rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		assertEquals(Types.DATE, rsm.getColumnType(2));
		
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(4, count);

		// date range BETWEEN
		rs = st.executeQuery("select _id, date, text from "+type+" where date between '2016-03-10' AND '2016-07-31'");
		rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		assertEquals(Types.DATE, rsm.getColumnType(2));
		
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(4, count);
		
		PreparedStatement pst = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test")
				.prepareStatement("select date from "+type+" where date > ?");
		pst.setDate(1, new Date(2016-1900, 5, 5));
		rs = pst.executeQuery();
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.DATE, rsm.getColumnType(1));
		
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(4, count);
		
		// IN
		rs = st.executeQuery("select intNum from "+type+" where intNum IN (-1, 0, 1)");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.INTEGER, rsm.getColumnType(1));
		
		count = 0;
		while(rs.next()){
			assert(rs.getInt(1) >= -1 && rs.getInt(1) <= 1);
			assertEquals(rs.getInt(1), rs.getInt("intNum"));
			count++;
		}
		assertEquals(2, count);
		st.close();
		pst.close();
	}
	
	@Test
	public void selectTextExact() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		
		// exact string match
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select _id, intNum, text from "+type+" where text = 'Some text for document 3'");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		assertEquals(Types.INTEGER, rsm.getColumnType(2));
		
		int count = 0;
		while(rs.next()){
			assertEquals(3, rs.getInt("intNum"));
			assertEquals("Some text for document 3", rs.getString(3));
			count++;
		}
		assertEquals(1, count);
		
		// partial string match
		rs = st.executeQuery("select _id, intNum, text from "+type+" where text LIKE '%6'");
		rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		assertEquals(Types.INTEGER, rsm.getColumnType(2));
		
		count = 0;
		while(rs.next()){
			assertEquals(6, rs.getInt("intNum"));
			count++;
		}
		assertEquals(1, count);
		
		// partial string match
		rs = st.executeQuery("select text from "+type+" where text LIKE '%text%'");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		
		count = 0;
		while(rs.next()){
			assert(rs.getString("text").contains("text"));
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select text from "+type+" where text = '%text%'");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		
		count = 0;
		while(rs.next()){
			assert(rs.getString("text").contains("text"));
			count++;
		}
		assertEquals(10, count);
		
		// mismatch (no results)
		rs = st.executeQuery("select text from "+type+" where text = 'something stupid'");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(0, count);
		st.close();
	}
	
	@Test
	public void selectTextAnalyzed() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		
		// exact string match
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select text from "+type+" where text.analyzed = 'text'");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.VARCHAR, rsm.getColumnType(1));
		
		int count = 0;
		while(rs.next()){
			assert(rs.getString(1).startsWith("Some text for document"));
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select text from "+type+" where text.analyzed = 'nothing'");
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(0, count);
		
		rs = st.executeQuery("select text from "+type+" where text.analyzed = 'for document'");
		count = 0;
		while(rs.next()){
			assert(rs.getString(1).startsWith("Some text for document"));
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select text from "+type+" where text.analyzed = 'document%'");
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select text from "+type+" where text.analyzed IN ('some')");
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(10, count);
		st.close();
	}
	
	@Test
	public void selectBool() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10, 1);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select bool from "+type+" where bool = True");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		int count = 0;
		while(rs.next()){
			count++;
			assertEquals(true, rs.getBoolean("bool"));
		}
		assertEquals(5, count);
		
		rs = st.executeQuery("select bool as B from "+type+" where bool = 0");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			assertEquals(false, rs.getBoolean("B"));
			count++;
		}
		assertEquals(5, count);
		
		rs = st.executeQuery("select bool from "+type+" where bool = 'F'");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(false, rs.getObject(1));
		}
		assertEquals(5, count);
		
		rs = st.executeQuery("select nestedDoc.bool from "+type+" where bool > 0");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(false, rs.getBoolean(1)); // nested doc bool value is inverted of its parents
		}
		assertEquals(5, count);
		
		rs = st.executeQuery("select nestedDoc.bool from "+type+" where nestedDoc.bool > 0");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(true, rs.getBoolean(1)); 
		}
		assertEquals(5, count);
		
		rs = st.executeQuery("select bool from "+type+" where bool = False AND bool = 'True' ");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(0, count);
		
		rs = st.executeQuery("select sum(bool) from "+type+" where bool = True");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(5, rs.getDouble("sum(bool)"), 0.0001);
			assertEquals(rs.getObject("sum(bool)"), rs.getDouble(1));
		}
		assertEquals(1, count);
		st.close();
	}
	
	@Test
	public void selectAndOr() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select _id as ID, intNum from "+type+" where intNum = 3 OR intNum = 9 OR intNum = 6");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		assertEquals(Types.INTEGER, rsm.getColumnType(2));
		
		int count = 0;
		while(rs.next()){
			assert(rs.getInt("intNum") > 0 && rs.getInt(2)%3 == 0);
			assert(rs.getString("ID").length() > 0);
			count++;
		}
		assertEquals(3, count);
		
		rs = st.executeQuery("select _id, floatNum, doubleNum from "+type+" where (floatNum > 0 AND floatNum <= 4) OR doubleNum = 8");
		rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		assertEquals(Types.FLOAT, rsm.getColumnType(2));
		assertEquals(Types.DOUBLE, rsm.getColumnType(3));
		
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(5, count);		
		
		rs = st.executeQuery("select _id, floatNum, doubleNum from "+type+" where floatNum > 0 AND (floatNum <= 4 OR doubleNum = 8)");
		rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		assertEquals(Types.FLOAT, rsm.getColumnType(2));
		assertEquals(Types.DOUBLE, rsm.getColumnType(3));
		
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(5, count);
		
		rs = st.executeQuery("select _id, floatNum, doubleNum from "+type+" where floatNum <> 2 OR intNum <> 3");
		rsm = rs.getMetaData();
		
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select _id, floatNum, doubleNum from "+type+" where floatNum <> 2 AND intNum <> 3");
		rsm = rs.getMetaData();
		
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(8, count);
		st.close();
	}
	
	@Test
	public void LimitsAndMax() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select _id from "+type+" limit 3");
		
		int count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(3, count);
		
		rs = st.executeQuery("select _id from "+type+" limit 100");
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(10, count);
		
		st.setMaxRows(5);
		rs = st.executeQuery("select _id from "+type+" limit 9");
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(5, count);
		st.close();
	}
	
	@Test
	public void getID() throws Exception{
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		int res = st.executeUpdate("INSERT INTO mytype (_id, myInt) VALUES ('id1', 1), ('id2', 2)");
		assertEquals(2, res);
		flush();
		refresh();
		Utils.sleep(1000);
		
		st.execute("USE "+index);
		
		ResultSet rs = st.executeQuery("SELECT * FROM mytype WHERE _id = 'no-valid-id'");
		int count = 0;
		while(rs.next()) count++;
		assertEquals(0, count);
		
		rs = st.executeQuery("SELECT * FROM mytype WHERE _id = 'id1'");
		count = 0;
		while(rs.next()) count++;
		assertEquals(1, count);
		
		rs = st.executeQuery("SELECT * FROM mytype WHERE _id = 'id1' AND myInt > 2");
		count = 0;
		while(rs.next()) count++;
		assertEquals(0, count);
		
		rs = st.executeQuery("SELECT * FROM mytype WHERE _id = 'id1' OR myInt >= 2");
		count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);
		
		rs = st.executeQuery("SELECT * FROM mytype WHERE _id = 'id1' AND myInt < 2");
		count = 0;
		while(rs.next()) count++;
		assertEquals(1, count);
		
		rs = st.executeQuery("SELECT * FROM mytype WHERE _id IN ('id1', 'id2', 'whateverid')");
		count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);
		st.close();
	}
	
	@Test
	public void search() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("SELECT _score, text FROM "+type+" WHERE _search = 'text.analyzed:document'");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(2, rsm.getColumnCount());
		assertEquals(Types.DOUBLE, rsm.getColumnType(1));
		assertEquals(Types.VARCHAR, rsm.getColumnType(2));
		int count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("SELECT _score, text FROM "+type+" WHERE _search = 'text.analyzed:document' AND intNum > 5");
		count = 0;
		while(rs.next()) count++;
		assertEquals(4, count);
		
		rs = st.executeQuery("SELECT _score, highlight(text.analyzed), intNum%2, text FROM "+type+" WHERE _search = 'text.analyzed:document' AND intNum > 5");
		 rsm = rs.getMetaData();
		assertEquals(4, rsm.getColumnCount());
		assertEquals(Types.DOUBLE, rsm.getColumnType(1));
		assertEquals(Types.ARRAY, rsm.getColumnType(2));
		assertEquals(Types.FLOAT, rsm.getColumnType(3));
		assertEquals(Types.VARCHAR, rsm.getColumnType(4));

		st.close();
	}
		
}
