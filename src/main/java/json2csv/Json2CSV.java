package json2csv;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class Json2CSV {
	private Map<String, JsonNode> jsonMap;
	private String filePath, entityMetaFilename, partitionMetaFilename;
	
	public int readJson(String jsonPath) throws IOException {
		this.filePath = jsonPath.substring(0, jsonPath.lastIndexOf('\\')+1);
		this.jsonMap = new LinkedHashMap<String, JsonNode>();
		
		byte[] jsonData = Files.readAllBytes(Paths.get(jsonPath));
		ObjectMapper objMapper = new ObjectMapper();
		JsonNode jsonObj = objMapper.readValue(jsonData, JsonNode.class);
		for(Iterator<String> it = jsonObj.fieldNames() ; it.hasNext();) {
			String key = it.next();
			this.jsonMap.put(key, jsonObj.get(key));
		}
		return 0;
	}
	
	public int createEntityList() throws IOException {
		ArrayList<String> header = new ArrayList<String>(Arrays.asList("META_FILE_NAME", "TECHNICAL_NAME", "BUSINESS_NAME", 
				"LABEL", "DESCRIPTION", "SOURCE_PLATFORM", "SOURCE_SCHEMA", "FILE_FORMAT", "DELIMITER", "OWNER", "UPDATE_ENTITY_VERSION",
				"TABLE_NAME", "TARGET_SCHEMA", "EXTERNAL_TABLE", "EXTERNAL_DATA_PATH", "TABLE_PROPERTIES", "PROJECT_NAME", 
				"SUBJECT_AREA", "PARTITION_FILE_NAME", "SERDE_CLASS", "INPUT_FORMAT_CLASS", "OUTPUT_FORMAT_CLASS"));
		ArrayList<String> keys = new ArrayList<String>(Arrays.asList("metaFilename", "technicalName", "businessName", 
				"label", "description", "sourcePlatform", "sourceSchema", "dataFileFormat", "delimiter", "owner", "updateEntityVersion", 
				"tableName", "targetSchema", "external", "externalDataPath", "tableProperties", "ownerProjectName", 
				"subjectArea", "partitionFilename", "serdeLib", "inputFormatLib", "outputFormatLib"));
		ArrayList<String> values = new ArrayList<String>();
		
		JsonNode resultNode = this.jsonMap.get("result");
		for(String key : keys) {
			if(key.equals("metaFilename")) {
				this.entityMetaFilename = this.filePath+resultNode.get("technicalName").toString().replaceAll("\"", "")+"_entity_fields_list.meta";
				values.add(this.entityMetaFilename);
			} else if(key.equals("updateEntityVersion")) {
				values.add("\"FALSE\"");
			}else if(key.equals("partitionFilename")) {
				this.partitionMetaFilename = this.filePath+resultNode.get("technicalName").toString().replaceAll("\"", "")+"_entity_partition.meta";
				values.add(this.partitionMetaFilename);
			}else if(key.equals("tableProperties")) {
				JsonNode tblProps = resultNode.get(key);
				Map<String, String> tblPropsMap = new LinkedHashMap<String, String>();
				for(Iterator<String> it = tblProps.fieldNames() ; it.hasNext();) {
					String tkey = it.next();
					if(tkey != "") {
						tblPropsMap.put(tkey, tblProps.get(tkey).toString().replaceAll("\"", ""));
					}
				}
				if(!tblPropsMap.isEmpty()) {
					values.add("\""+tblPropsMap.toString().replace("{", "").replace("}", "")+"\"");
				}else {
					values.add("");
				}
			} else {
				values.add(resultNode.get(key).toString().replaceAll("\"", ""));
			}
		}
		
		String entityListFile = this.filePath+resultNode.get("technicalName").toString().replaceAll("\"", "")+"_entity_list.csv";
		
		FileWriter fileWriter = new FileWriter(entityListFile);
		BufferedWriter csvWriter = new BufferedWriter(fileWriter);
		csvWriter.write(String.join(",", header));
		csvWriter.newLine();
		csvWriter.write(String.join(",", values));
		csvWriter.close();
		fileWriter.close();
		
		return 0;
	}
	
	public int createEntityListMeta() throws JsonParseException, JsonMappingException, IOException {
		ArrayList<String> header = new ArrayList<String>(Arrays.asList("TECHNICAL_NAME", "BUSINESS_NAME", "DATA_TYPE", "COMPLEX_DATA_TYPE", 
				"DATA_LENGTH", "DATA_SCALE", "FORMAT", "PRIMARY_KEY", "COLUMN_ID", "SENSITIVITY", "RULE_NAME", "ADDITIONAL_FIELD_PROPERTIES", 
				"DESCRIPTION"));
		ArrayList<String> keys = new ArrayList<String>(Arrays.asList("fieldTechnicalName", "fieldBusinessName", "fieldDataType", "complexType", 
				"fieldDataLength", "dataScale", "dataFormat", "primary", "fieldId", "sensitivityValue", "ruleName", "additionalFieldProperties", 
				"description"));
		ArrayList<ArrayList<String>> values = new ArrayList<ArrayList<String>>();
		
		JsonNode fieldsNode = this.jsonMap.get("result").get("fields");
		assert(fieldsNode instanceof ArrayNode);
		
		if(fieldsNode.size() > 0) {
			for (int i=0; i<fieldsNode.size(); i++) {
				JsonNode fieldNode = fieldsNode.get(i);
				ArrayList<String> value = new ArrayList<String>();
				for(int j=0; j<keys.size(); j++ ) {
					String key = keys.get(j);
					if(key.equals("complexType")) {
						if(fieldNode.get(key).toString().equals("true")) {
							String schemaStr = fieldNode.get("fieldDataTypeSchema").toString().replace("\\", "");
							schemaStr = schemaStr.substring(1, schemaStr.length() - 1);
							JsonNode schemaNode = new ObjectMapper().readTree(schemaStr);
							value.add(schemaNode.get("values").get("type").toString());
						} else {
							value.add("");
						}
					} else if(key.equals("primary")) {
						if(fieldNode.get(key).toString().equals("true")) {
							value.add("YES");
						}else {
							value.add("");
						}
					} else if(key.equals("ruleName")) {
						JsonNode ruleMappingsNode = fieldNode.get("ruleMappings");
						assert(ruleMappingsNode instanceof ArrayNode);
						if(ruleMappingsNode.size() > 0) {
							ArrayList<String> ruleName = new ArrayList<String>();
							for(JsonNode node : ruleMappingsNode) {
								ruleName.add(node.get("ruleName").toString().replaceAll("\"", ""));
							}
							value.add(String.join("|", ruleName));
						}else {
							value.add("");
						}
					} else if(key.equals("additionalFieldProperties")) {
						value.add("");
					} else {
						value.add(fieldNode.get(key).toString().replaceAll("\"", ""));
					}
				}
				values.add(value);
			}
		}
		
		FileWriter fileWriter = new FileWriter(this.entityMetaFilename);
		BufferedWriter csvWriter = new BufferedWriter(fileWriter);
		csvWriter.write(String.join(",", header));
		csvWriter.newLine();
		for(ArrayList<String> list : values) {
			csvWriter.write(String.join(",", list));
			csvWriter.newLine();
		}
		csvWriter.close();
		fileWriter.close();
		return 0;
	}
	
	public int createPartitionMeta() throws IOException {
		ArrayList<String> header = new ArrayList<String>(Arrays.asList("COLUMN_NAME", "COLUMN_DATATYPE", "POSITION"));
		ArrayList<String> keys = new ArrayList<String>(Arrays.asList("partitionFieldName", "fieldDataType", "partitionFieldPosition"));
		ArrayList<String> values = new ArrayList<String>();
		
		JsonNode partitionNode = this.jsonMap.get("result").get("partitionFields").get(0);
		assert(partitionNode instanceof ArrayNode);
		
		if(partitionNode != null) {
			for(String key : keys) {
				values.add(partitionNode.get(key).toString().replaceAll("\"", ""));
			}
		}
		
		FileWriter fileWriter = new FileWriter(this.partitionMetaFilename);
		BufferedWriter csvWriter = new BufferedWriter(fileWriter);
		csvWriter.write(String.join(",", header));
		csvWriter.newLine();
		csvWriter.write(String.join(",", values));
		csvWriter.close();
		fileWriter.close();
		
		return 0;
	}
}
