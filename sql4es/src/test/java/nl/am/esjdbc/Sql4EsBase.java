package nl.am.esjdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.elasticsearch.test.ESIntegTestCase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Sql4EsBase extends ESIntegTestCase {

	/**
	 * Loads the ESDriver
	 * @throws Exception
	 */
	public Sql4EsBase() throws Exception {
		super();
		Class.forName("nl.am.sql4es.jdbc.ESDriver");
	}
	
	/**
	 * Creates the index with optionally the mapping and a number of docs
	 * @param index
	 * @param type
	 * @param withMapping
	 * @param nrDocs
	 * @throws IOException
	 */
	protected void createIndexTypeWithDocs(String index, String type, boolean withMapping, int nrDocs) throws IOException{
		if(withMapping){
			String mapping = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDocumentMapping.json")));
			client().admin().indices().prepareCreate(index).addMapping(type, mapping).execute().actionGet();
		}else{
			createIndex(index);
		}
		if(nrDocs > 0) addDocs(index, type, nrDocs);
		refresh();
	}
	
	protected void createIndexTypeWithDocs(String index, String type, boolean withMapping, int nrDocs, int history) throws IOException{
		if(withMapping){
			String mapping = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDocumentMapping.json")));
			client().admin().indices().prepareCreate(index).addMapping(type, mapping).execute().actionGet();
		}else{
			createIndex(index);
		}
		if(nrDocs > 0) addDocs(index, type, nrDocs, history);
		refresh();
	}
	
	/**
	 * Adds the specified number of docs to the type within index
	 * @param index
	 * @param type
	 * @param number
	 * @throws JsonProcessingException
	 */
	protected void addDocs(String index, String type, int number) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		for(int i=0; i<number; i++){
			index(index, type, "doc_"+i, mapper.writeValueAsString(TestDocument.createSimpleDoc(i)));
		}
		flush();
	}
	
	protected void addDocs(String index, String type, int number, int history) throws JsonProcessingException {
		if(history == 0) {
			addDocs(index, type, number);
			return;
		}
		ObjectMapper mapper = new ObjectMapper();
		for(int i=0; i<number; i++){
			TestDocument doc = TestDocument.createNestedDoc(i, history);
			index(index, type, "doc_"+i, mapper.writeValueAsString(doc));
		}
		flush();
	}
	
}
