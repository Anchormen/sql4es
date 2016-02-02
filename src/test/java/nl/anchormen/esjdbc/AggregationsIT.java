package nl.anchormen.esjdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import org.junit.Test;

public class AggregationsIT extends Sql4EsBase {

	private String index = "testindex";
	private String type = "testdocs";
	
	public AggregationsIT() throws Exception {
		super();
	}
	
	@Test
	public void testSimpleAggregations() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select count(*) from "+type);
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.BIGINT, rsm.getColumnType(1));
		int count = 0;
		while(rs.next()){
			count++;
			assertEquals(10, rs.getLong(1));
		}
		assertEquals(1, count);
		
		rs = st.executeQuery("select COUNT(1) from "+type);
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.BIGINT, rsm.getColumnType(1));
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(10, rs.getLong(1));
		}
		assertEquals(1, count);
		
		rs = st.executeQuery("select max(doubleNum) as maximus from "+type);
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.DOUBLE, rsm.getColumnType(1));
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(9, rs.getDouble("maximus"), 0.0001);
		}
		assertEquals(1, count);
		
		rs = st.executeQuery("select Min(longNum) from "+type);
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.BIGINT, rsm.getColumnType(1));
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(0, rs.getLong(1));
		}
		assertEquals(1, count);
		
		rs = st.executeQuery("select suM(shortNum) from "+type);
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.SMALLINT, rsm.getColumnType(1));
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(45, rs.getShort("suM(shortNum)"));
		}
		assertEquals(1, count);
		
		rs = st.executeQuery("select AVG(floatNum) from "+type);
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		assertEquals(Types.DOUBLE, rsm.getColumnType(1));
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(4.5, rs.getFloat("AVG(floatNum)"), 0.001f);
		}
		assertEquals(1, count);
		st.close();
	}
	
	@Test
	public void testGroupBy() throws Exception{
		createIndexTypeWithDocs(index, type, true, 100, 1);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select bool, count(*), max(intNum), min(floatNum), avg(doubleNum) from "+type+" GROUP BY bool");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(5, rsm.getColumnCount());
		assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
		assertEquals(Types.BIGINT, rsm.getColumnType(2));
		assertEquals(Types.INTEGER, rsm.getColumnType(3));
		assertEquals(Types.FLOAT, rsm.getColumnType(4));
		assertEquals(Types.DOUBLE, rsm.getColumnType(5));
		int count = 0;
		while(rs.next()){
			count++;
			assertEquals(50, rs.getLong(2));
			assert(rs.getInt(3) >= 98 );
			assert(rs.getFloat(4) <= 1 );
			assertEquals(50, rs.getDouble(5), 1);
		}
		assertEquals(2, count);
		
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum), min(floatNum), avg(doubleNum) from "+type+" GROUP BY bool, nb");
		rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
		assertEquals(Types.BOOLEAN, rsm.getColumnType(2));
		assertEquals(Types.BIGINT, rsm.getColumnType(3));
		assertEquals(Types.INTEGER, rsm.getColumnType(4));
		assertEquals(Types.FLOAT, rsm.getColumnType(5));
		assertEquals(Types.DOUBLE, rsm.getColumnType(6));

		count = 0;
		while(rs.next()){
			count++;
			assertEquals(50, rs.getLong(3));
			assert(rs.getInt(4) >= 98 );
			assert(rs.getFloat(5) <= 1 );
			assertEquals(50, rs.getDouble(6), 1);
		}
		assertEquals(2, count);

		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum), min(floatNum), avg(doubleNum) "
				+ "from "+type+" WHERE shortNum >= 50 GROUP BY bool, nb");
		rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
		assertEquals(Types.BOOLEAN, rsm.getColumnType(2));
		assertEquals(Types.BIGINT, rsm.getColumnType(3));
		assertEquals(Types.INTEGER, rsm.getColumnType(4));
		assertEquals(Types.FLOAT, rsm.getColumnType(5));
		assertEquals(Types.DOUBLE, rsm.getColumnType(6));

		count = 0;
		while(rs.next()){
			count++;
			assertEquals(25, rs.getLong(3));
			assert(rs.getInt(4) >= 98 );
			assert(rs.getFloat(5) <= 51 );
			assertEquals(75, rs.getDouble(6), 1);
		}
		assertEquals(2, count);
		st.close();
	}
	
	@Test
	public void testGroupByWithWhereAndHaving() throws Exception{
		createIndexTypeWithDocs(index, type, true, 100, 1);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum), min(floatNum), avg(doubleNum) "
				+ "from "+type+" WHERE shortNum >= 50 GROUP BY bool, nb");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
		assertEquals(Types.BOOLEAN, rsm.getColumnType(2));
		assertEquals(Types.BIGINT, rsm.getColumnType(3));
		assertEquals(Types.INTEGER, rsm.getColumnType(4));
		assertEquals(Types.FLOAT, rsm.getColumnType(5));
		assertEquals(Types.DOUBLE, rsm.getColumnType(6));

		int count = 0;
		while(rs.next()){
			count++;
			assertEquals(25, rs.getLong(3));
			assert(rs.getInt(4) >= 98 );
			assert(rs.getFloat(5) <= 51 );
			assertEquals(75, rs.getDouble(6), 1);
		}
		assertEquals(2, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum), min(floatNum), avg(doubleNum) "
				+ "from "+type+" WHERE shortNum >= 50 AND doubleNum < 60 GROUP BY bool, nb");
		rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
		assertEquals(Types.BOOLEAN, rsm.getColumnType(2));
		assertEquals(Types.BIGINT, rsm.getColumnType(3));
		assertEquals(Types.INTEGER, rsm.getColumnType(4));
		assertEquals(Types.FLOAT, rsm.getColumnType(5));
		assertEquals(Types.DOUBLE, rsm.getColumnType(6));

		count = 0;
		while(rs.next()){
			count++;
			assertEquals(5, rs.getLong(3));
			assert(rs.getInt(4) >= 58 );
			assert(rs.getFloat(5) <= 51 );
			assertEquals(55, rs.getDouble(6), 1);
		}
		assertEquals(2, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum) m, min(floatNum), avg(doubleNum) "
				+ "from "+type+" GROUP BY bool, nb HAVING m >= 99");
		rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
		assertEquals(Types.BOOLEAN, rsm.getColumnType(2));
		assertEquals(Types.BIGINT, rsm.getColumnType(3));
		assertEquals(Types.INTEGER, rsm.getColumnType(4));
		assertEquals(Types.FLOAT, rsm.getColumnType(5));
		assertEquals(Types.DOUBLE, rsm.getColumnType(6));

		count = 0;
		while(rs.next()){
			count++;
			assertEquals(50, rs.getLong(3));
			assert(rs.getInt(4) >= 99 );
			assert(rs.getFloat(5) <= 51 );
			assertEquals(50, rs.getDouble(6), 0.001);
		}
		assertEquals(1, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum) m, min(floatNum), avg(doubleNum) "
				+ "from "+type+" GROUP BY bool, nb HAVING max(intNum) >= 99");
		rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
		assertEquals(Types.BOOLEAN, rsm.getColumnType(2));
		assertEquals(Types.BIGINT, rsm.getColumnType(3));
		assertEquals(Types.INTEGER, rsm.getColumnType(4));
		assertEquals(Types.FLOAT, rsm.getColumnType(5));
		assertEquals(Types.DOUBLE, rsm.getColumnType(6));
		
		count = 0;
		while(rs.next()){
			count++;
			assertEquals(50, rs.getLong(3));
			assert(rs.getInt(4) >= 99 );
			assert(rs.getFloat(5) <= 51 );
			assertEquals(50, rs.getDouble(6), 0.001);
		}
		assertEquals(1, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*) as c, max(intNum), min(floatNum), avg(doubleNum) "
				+ "from "+type+" GROUP BY bool, nb HAVING c > 99999");
		rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
		assertEquals(Types.BOOLEAN, rsm.getColumnType(2));
		assertEquals(Types.BIGINT, rsm.getColumnType(3));
		assertEquals(Types.INTEGER, rsm.getColumnType(4));
		assertEquals(Types.FLOAT, rsm.getColumnType(5));
		assertEquals(Types.DOUBLE, rsm.getColumnType(6));

		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(0, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum), min(floatNum), avg(doubleNum) "
				+ "from "+type+" GROUP BY bool, nb HAVING count(*) > 99999");
		rsm = rs.getMetaData();
		assertEquals(6, rsm.getColumnCount());
		assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
		assertEquals(Types.BOOLEAN, rsm.getColumnType(2));
		assertEquals(Types.BIGINT, rsm.getColumnType(3));
		assertEquals(Types.INTEGER, rsm.getColumnType(4));
		assertEquals(Types.FLOAT, rsm.getColumnType(5));
		assertEquals(Types.DOUBLE, rsm.getColumnType(6));
		
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(0, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, max(intNum) as maximus, "
				+ "avg(doubleNum) as average from "+type+" GROUP BY bool, nb HAVING maximus > average");
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(2, count);
		st.close();
	}
	
	@Test
	public void testGroupByWithOrder() throws Exception{
		createIndexTypeWithDocs(index, type, true, 100, 1);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum) m, min(floatNum), avg(doubleNum) "
				+ "from "+type+" GROUP BY bool, nb ORDER BY m");

		int count = 0;
		int max = -1;
		while(rs.next()){
			count++;
			assert(rs.getInt(4) >= max );
			max = rs.getInt(4);
		}
		assertEquals(2, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum) m, min(floatNum), avg(doubleNum) "
				+ "from "+type+" GROUP BY bool, nb ORDER BY max(intNum)");
		
		count = 0;
		max = -1;
		while(rs.next()){
			count++;
			assert(rs.getInt(4) >= max );
			max = rs.getInt(4);
		}
		assertEquals(2, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum) m, min(floatNum), avg(doubleNum) "
				+ "from "+type+" GROUP BY bool, nb ORDER BY m ASC");

		count = 0;
		max = -1;
		while(rs.next()){
			count++;
			assert(rs.getInt(4) >= max );
			max = rs.getInt(4);
		}
		assertEquals(2, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum) m, min(floatNum), avg(doubleNum) "
				+ "from "+type+" GROUP BY bool, nb ORDER BY max(intNum) ASC");

		count = 0;
		max = -1;
		while(rs.next()){
			count++;
			assert(rs.getInt(4) >= max );
			max = rs.getInt(4);
		}
		assertEquals(2, count);
		
		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum) m, min(floatNum), avg(doubleNum) "
				+ "from "+type+" GROUP BY bool, nb ORDER BY m DESC");

		count = 0;
		max = 1000;
		while(rs.next()){
			count++;
			assert(rs.getInt(4) <= max );
			max = rs.getInt(4);
		}
		assertEquals(2, count);

		st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		rs = st.executeQuery("select bool, nestedDoc.bool as nb, count(*), max(intNum) m, min(floatNum), avg(doubleNum) "
				+ "from "+type+" WHERE longNum > 78 GROUP BY bool, nb ORDER BY m DESC");
		
		count = 0;
		max = 1000;
		while(rs.next()){
			count++;
			assert(rs.getInt(4) <= max );
			max = rs.getInt(4);
		}
		assertEquals(2, count);
		st.close();
	}
	
	@Test
	public void testDistinct() throws Exception{
		createIndexTypeWithDocs(index, type, true, 100, 1);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select distinct bool, count(*) from "+type);

		int count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(2, count);
		
		rs = st.executeQuery("select distinct bool, nestedDoc.bool, count(*) from "+type+" WHERE longNum > 32.5");
		count = 0;
		while(rs.next()){
			count++;
		}
		assertEquals(2, count);
		
		rs = st.executeQuery("select intNum%5 from "+type);
		count = 0;
		while(rs.next()){
			assert(rs.getInt(1) < 5);
			count++;
		}
		assertEquals(100, count);
		st.close();
	}
}
