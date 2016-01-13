package nl.am.esjdbc;

import java.util.ArrayList;
import java.util.Date;

public class TestDocument {

	private int intNum;
	private long longNum;
	private float floatNum;
	private double doubleNum;
	private short shortNum;
	private byte byteNum;
	private String text;
	private Date date;
	private boolean bool;
	private TestDocument nestedDoc;
	private ArrayList<Number> numbers = new ArrayList<Number>();
	private ArrayList<TestDocument> docs = new ArrayList<TestDocument>();
	
	@SuppressWarnings("deprecation")
	public TestDocument(int id){
		this.intNum = id;
		this.longNum = id;
		this.floatNum = id;
		this.doubleNum = id;
		this.shortNum = (short)id;
		this.byteNum = (byte)id;
		this.text = "Some text for document "+id;
		this.date = new Date(2016-1900,id%12, 1);
		this.bool = id%2 == 0;
		this.nestedDoc = null;
		for(int i=id+1; i<=id+5; i++) numbers.add(i);
	}
	
	public static TestDocument createSimpleDoc(int id){
		return new TestDocument(id);
	}
	
	public static TestDocument createNestedDoc(int id, int history) {
		TestDocument doc = createSimpleDoc(id);
		if (history > 0) doc.setNestedDoc(createSimpleDoc(id - 1));
		for(int i=1; i<history; i++){
			doc.getDocs().add(createSimpleDoc(id- i));
		}
		return doc;
 	}

	public int getIntNum() {
		return intNum;
	}

	public void setIntNum(int intNum) {
		this.intNum = intNum;
	}

	public long getLongNum() {
		return longNum;
	}

	public void setLongNum(long longNum) {
		this.longNum = longNum;
	}

	public float getFloatNum() {
		return floatNum;
	}

	public void setFloatNum(float floatNum) {
		this.floatNum = floatNum;
	}

	public double getDoubleNum() {
		return doubleNum;
	}

	public void setDoubleNum(double doubleNum) {
		this.doubleNum = doubleNum;
	}

	public short getShortNum() {
		return shortNum;
	}

	public void setShortNum(short shortNum) {
		this.shortNum = shortNum;
	}

	public byte getByteNum() {
		return byteNum;
	}

	public void setByteNum(byte byteNum) {
		this.byteNum = byteNum;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public boolean isBool() {
		return bool;
	}

	public void setBool(boolean bool) {
		this.bool = bool;
	}

	public TestDocument getNestedDoc() {
		return nestedDoc;
	}

	public void setNestedDoc(TestDocument nestedDoc) {
		this.nestedDoc = nestedDoc;
	}

	public ArrayList<Number> getNumbers() {
		return numbers;
	}

	public void setNumbers(ArrayList<Number> numbers) {
		this.numbers = numbers;
	}

	public ArrayList<TestDocument> getDocs() {
		return docs;
	}

	public void setDocs(ArrayList<TestDocument> docs) {
		this.docs = docs;
	}

}
