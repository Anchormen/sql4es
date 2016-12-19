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
public class ArithmeticsIT extends Sql4EsBase {

	private String index = "testindex";
	private String type = "testdocs";
		
	public ArithmeticsIT() throws Exception {
		super();
	}
		
	@Test
	public void singleComputations() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10,2);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select intNum*10 from "+type);
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(Types.FLOAT, rsm.getColumnType(1));
		
		int count = 0;
		while(rs.next()){
			assert(rs.getInt(1)%10 == 0);
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select shortNum+10 from "+type);
		rsm = rs.getMetaData();
		assertEquals(Types.FLOAT, rsm.getColumnType(1));
		
		count = 0;
		while(rs.next()){
			assert(rs.getShort(1) >= 10 && rs.getShort(1) < 20);
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select longNum-9 as long from "+type);
		rsm = rs.getMetaData();
		assertEquals(Types.FLOAT, rsm.getColumnType(1));
		
		count = 0;
		while(rs.next()){
			assert(rs.getLong(1) >= -9 && rs.getLong("long") <= 0);
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select floatNum/10 from "+type);
		rsm = rs.getMetaData();
		assertEquals(Types.FLOAT, rsm.getColumnType(1));
		
		count = 0;
		while(rs.next()){
			assert(rs.getFloat(1) < 1);
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select nestedDoc.doubleNum%3 as f from "+type);
		rsm = rs.getMetaData();
		assertEquals(Types.FLOAT, rsm.getColumnType(1));
		
		count = 0;
		while(rs.next()){
			assert(rs.getFloat(1) >= -1 && rs.getFloat("f") <= 2);
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select 100%9+4*6/3-10, (100%9+4)*(6/3)-10 as calc from "+type);
		rsm = rs.getMetaData();
		assertEquals(Types.FLOAT, rsm.getColumnType(1));
		
		count = 0;
		while(rs.next()){
			assertEquals(-1, rs.getFloat(1), 0.0001);
			assertEquals(0, rs.getFloat("calc"), 0.0001);
			count++;
		}
		assertEquals(1, count);
	}
	
	
	@Test
	public void combinedComputations() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10, 2);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select intNum/shortNum as calc from "+type+" where intNum > 0");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(Types.FLOAT, rsm.getColumnType(1));
		
		int count = 0;
		while(rs.next()){
			assertEquals(1, rs.getFloat(1), 0.0001f);
			assertEquals(1, rs.getFloat("calc"), 0.0001f);
			count++;
		}
		assertEquals(9, count);
		
		rs = st.executeQuery("select (longNum+1)/doubleNum as calc from "+type);
		rsm = rs.getMetaData();
		assertEquals(Types.FLOAT, rsm.getColumnType(1));
		
		count = 0;
		while(rs.next()){
			assert(rs.getFloat(1) > 1);
			count++;
		}
		assertEquals(10, count);
		
		rs = st.executeQuery("select (bool*-1)*floatNum as calc from "+type);
		rsm = rs.getMetaData();
		assertEquals(Types.FLOAT, rsm.getColumnType(1));
		
		count = 0;
		while(rs.next()){
			assert(rs.getFloat(1) <= 0);
			count++;
		}
		assertEquals(10, count);

		rs = st.executeQuery("select bool, sum(intNum)/count(1) as avg1, avg(intNum) as avg2 from "+type+" group by bool ");
		rsm = rs.getMetaData();
		assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
		assertEquals(Types.FLOAT, rsm.getColumnType(2));
		assertEquals(Types.DOUBLE, rsm.getColumnType(3));
		
		count = 0;
		while(rs.next()){
			assertEquals(rs.getFloat(2), rs.getFloat("avg2"), 0.0001);
			count++;
		}
		assertEquals(2, count);
	}
	
	@Test
	public void offsetComputations() throws Exception{
		createIndexTypeWithDocs(index, type, true, 10,2);
		
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("select intNum/shortNum[-1], intNum from "+type+" where intNum > 0 order by intNum asc");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(Types.FLOAT, rsm.getColumnType(1));
		int count = 0;
		while(rs.next()){
			assert(new Float(rs.getFloat(1)).isNaN() || rs.getFloat(1) > 1);
			count++;
		}
		assertEquals(9, count);
		rs.close();
		
		rs = st.executeQuery("select distinct bool, count(1) as c, c/c[1] from "+type+" ");
		count = 0;
		while(rs.next()){
			assert(new Float(rs.getFloat(3)).isNaN() || rs.getFloat(3) == 1);
			count++;
		}
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("select distinct bool, count(1)/count(1)[1] as calc from "+type+" ");
		count = 0;
		while(rs.next()){
			assert(new Float(rs.getFloat("calc")).isNaN() || rs.getFloat("calc") == 1);
			count++;
		}
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("select distinct bool, count(1)/sum(longNum)[1] as calc from "+type+" ");
		count = 0;
		while(rs.next()){
			assert(new Float(rs.getFloat("calc")).isNaN() || rs.getFloat("calc") < 0.5);
			count++;
		}
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("select distinct nestedDoc.bool, count(nestedDoc.bool)/count(nestedDoc.bool)[-1] from "+type+" ");
		count = 0;
		while(rs.next()){
			assert(new Float(rs.getFloat(2)).isNaN() || rs.getFloat(2) == 1);
			count++;
		}
		assertEquals(2, count);
		rs.close();
		
		st.close();
	}
	
}
