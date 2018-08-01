package json2csv;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Json2CSVTest {
	
	static Json2CSV j2c = new Json2CSV();
	
	@Test
	public void testA_ReadJson() throws IOException {
		assertEquals(0, j2c.readJson("D:\\ZaloniWork\\parsertoconvertjsontocsvformat\\zdp_entity_complex_2.json"));
	}
	
	@Test
	public void testB_CreateEntityList() throws IOException {
		assertEquals(0, j2c.createEntityList());
	}
	
	@Test
	public void testC_CreateEntityListMeta() throws IOException {
		assertEquals(0, j2c.createEntityListMeta());
	}
	
	@Test
	public void testD_CreatePartitionMeta() throws IOException {
		assertEquals(0, j2c.createPartitionMeta());
	}
}
