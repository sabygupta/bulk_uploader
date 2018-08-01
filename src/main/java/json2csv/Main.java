package json2csv;

import java.io.IOException;

public class Main {

	public static void main(String[] args) throws IOException {
		String filePath = "D:\\parsertoconvertjsontocsvformat\\zdp_entity_complex_2.json";
		Json2CSV j2c = new Json2CSV();
		j2c.readJson(filePath);
		j2c.createEntityList();
		j2c.createEntityListMeta();
		j2c.createPartitionMeta();
		return;
	}

}
