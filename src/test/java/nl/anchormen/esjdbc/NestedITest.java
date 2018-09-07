package nl.anchormen.esjdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

@ClusterScope(scope=Scope.TEST, numDataNodes=1)
public class NestedITest extends ESIntegTestCase{
		
	private String index = "testindex";
	private String type = "testdocs";
		
	public NestedITest() throws Exception {
		super();
		Class.forName("nl.anchormen.sql4es.jdbc.ESDriver");
	}
	
	@Test
	public void testNestedType() throws Exception{
		indexDocs(10, 1);
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM "+type+" WHERE intNum > 5");
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(9, rsm.getColumnCount());
		int count = 0;
		while(rs.next()) count++;
		assertEquals(8, count);
		rs.close();
		
		rs = st.executeQuery("SELECT * FROM "+type +" WHERE _id = 'doc_8'");
		count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("SELECT * FROM "+type+" WHERE nestedDoc.intNum > 5");
		count = 0;
		while(rs.next()) count++;
		assertEquals(12, count);
		rs.close();
		
		rs = st.executeQuery("SELECT nestedDoc FROM "+type+" WHERE nestedDoc.intNum > 5 AND text = 'NestedDoc number 6'");
		rsm = rs.getMetaData();
		assertEquals(3, rsm.getColumnCount());
		count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("SELECT nestedDoc.text FROM "+type+" WHERE nestedDoc.intNum > 5 AND nestedDoc.text = 'NestedDoc number 5'");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next()) count++;
		assertEquals(0, count);
		rs.close();
		
		rs = st.executeQuery("SELECT intNum FROM "+type+" WHERE nestedDoc.intNum > 5 AND nestedDoc.text = 'NestedDoc number 7'");
		rsm = rs.getMetaData();
		assertEquals(1, rsm.getColumnCount());
		count = 0;
		while(rs.next()) count++;
		assertEquals(2, count);
		rs.close();
		
		rs = st.executeQuery("SELECT * FROM "+type+" WHERE nestedDoc.intNum < 3 OR nestedDoc.text = 'NestedDoc number 6'");
		count = 0;
		while(rs.next()) count++;
		assertEquals(8, count);
		rs.close();
		
		rs = st.executeQuery("SELECT * FROM "+type+" WHERE nestedDoc.intNum < 3 OR (nestedDoc.text = 'NestedDoc number 6' AND intNum = 5)");
		count = 0;
		while(rs.next()) count++;
		assertEquals(6, count);
		rs.close();
		
		st.close();
		
	}
	
	@Test
	public void testInnerNestedType() throws Exception{
		indexDocs(5, 2);
		Statement st = DriverManager.getConnection("jdbc:sql4es://localhost:9300/"+index+"?test").createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM "+type);
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(10, rsm.getColumnCount());
		int count = 0;
		while(rs.next()) count++;
		assertEquals(20, count);
		rs.close();
		
		rs = st.executeQuery("SELECT * FROM "+type +" WHERE _id = 'doc_2'");
		count = 0;
		while(rs.next()) count++;
		assertEquals(4, count);
		rs.close();
		
		rs = st.executeQuery("SELECT intNum, nestedDoc FROM "+type +" WHERE intNum >= 3");
		count = 0;
		while(rs.next()) count++;
		assertEquals(8, count);
		rs.close();
		
		rs = st.executeQuery("SELECT nestedDoc.nestedDoc FROM "+type +" WHERE intNum = 0");
		assertEquals(3, rs.getMetaData().getColumnCount());
		count = 0;
		while(rs.next()) count++;
		assertEquals(4, count);
		rs.close();
		
		rs = st.executeQuery("SELECT nestedDoc.nestedDoc FROM "+type +" WHERE intNum = 0 AND nestedDoc.intNum = 1");
		// result is one top level documents with 4(!) nested results which are exploded
		count = 0;
		while(rs.next()) count++;
		assertEquals(4, count);
		rs.close();
		
		st.close();
	}
	
	private void indexDocs(int count, int depth) throws IOException{
		String mapping = AccessController.doPrivileged(new PrivilegedAction<String>(){
			@Override
			public String run() {
				try {
					return new String(Files.readAllBytes(Paths.get("src/test/resources/NestedDocMapping.json")));
				} catch (IOException e) {
					return null;
				}
			}
		});
		if(mapping == null) throw new IOException("Unable to read NestedDocMapping.json");
		client().admin().indices().prepareCreate(index).addMapping(type, mapping, XContentType.JSON).get();
		ObjectMapper mapper = new ObjectMapper();
		for(int i=0; i<count; i++){
			index(index, type, "doc_"+i, mapper.writeValueAsString(new NestedDoc(i, depth)));
		}
		flush();
	}
	
	@SuppressWarnings("unused")
	private class NestedDoc{
		
		private int intNum;
		private String text;
		private List<NestedDoc> nestedDoc;
		
		public NestedDoc(int i, int depth){
			this.intNum = i;
			this.text = "NestedDoc number "+i;
			this.nestedDoc = new ArrayList<NestedDoc>();
			if(depth > 0){
				nestedDoc.add(new NestedDoc(i+1, depth-1));
				nestedDoc.add(new NestedDoc(i+2, depth-1));
			}
		}

		public int getIntNum() {
			return intNum;
		}

		public String getText() {
			return text;
		}

		public List<NestedDoc> getNestedDoc() {
			return nestedDoc;
		}
	}

}
