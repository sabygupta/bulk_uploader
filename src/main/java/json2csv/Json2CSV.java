package json2csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Json2CSV {
	private Map<String, JsonNode> jsonMap;
	private Path jsonPath, csvPath, entityMetaPath, partitionMetaPath;
	
	public int readJson(String jsonPath) throws IOException {
		this.jsonPath = Paths.get(jsonPath);
		this.jsonMap = new LinkedHashMap<String, JsonNode>();
		
		byte[] jsonData = Files.readAllBytes(this.jsonPath);
		ObjectMapper objMapper = new ObjectMapper();
		JsonNode jsonObj = objMapper.readValue(jsonData, JsonNode.class);
		for(Iterator<String> it = jsonObj.fieldNames(); it.hasNext();) {
			String key = it.next();
			this.jsonMap.put(key, jsonObj.get(key));
		}
		
		this.setAllPaths(this.jsonPath);
		
		return 0;
	}
	
	private void setAllPaths(Path path) {
		Path currentDir = path.getParent();
		Path metaDir = Paths.get(currentDir.toString(), "meta");
		if(!Files.exists(metaDir)) {
			new File(metaDir.toString()).mkdirs();
		}
		String technicalName = this.jsonMap.get("result").get("technicalName").toString().replaceAll("\"", "");
		this.csvPath = Paths.get(currentDir.toString(), technicalName+"_entity_list.csv");
		this.entityMetaPath = Paths.get(metaDir.toString(), technicalName+"_entity_fields_list.meta");
		this.partitionMetaPath = Paths.get(metaDir.toString(), technicalName+"_entity_partition.meta");
		return;
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
				values.add(this.entityMetaPath.toString());
			} else if(key.equals("updateEntityVersion")) {
				values.add("\"FALSE\"");
			}else if(key.equals("partitionFilename")) {
				values.add(this.partitionMetaPath.toString());
			}else if(key.equals("tableProperties")) {
				JsonNode tblProps = resultNode.get(key);
				List<String> tblPropsList = new ArrayList<String>();
				for(Iterator<String> it = tblProps.fieldNames() ; it.hasNext();) {
					String tkey = it.next();
					if(tkey != "") {
						tblPropsList.add(tkey+".delim:"+tblProps.get(tkey).toString().replaceAll("\"", ""));
					}
				}
				if(!tblPropsList.isEmpty()) {
					values.add("\""+String.join(",", tblPropsList)+"\"");
				} else {
					values.add("");
				}
			} else {
				values.add(resultNode.get(key).toString().replaceAll("\"", ""));
			}
		}
		
		FileWriter fileWriter = new FileWriter(this.csvPath.toString());
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
				"fieldDataLength", "dataScale", "dataFormat", "primary", "fieldId", "sensitivityValue", "ruleName", "customAttributes", 
				"description"));
		ArrayList<ArrayList<String>> values = new ArrayList<ArrayList<String>>();
		
		JsonNode fieldsNode = this.jsonMap.get("result").get("fields");
		
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
						if(ruleMappingsNode.size() > 0) {
							ArrayList<String> ruleName = new ArrayList<String>();
							for(JsonNode node : ruleMappingsNode) {
								ruleName.add(node.get("ruleName").toString().replaceAll("\"", ""));
							}
							value.add(String.join("|", ruleName));
						}else {
							value.add("");
						}
					} else if(key.equals("customAttributes")) {
						List<String> attList = new ArrayList<String>();
						JsonNode cusAttNode = fieldNode.get(key);
						for(JsonNode child : cusAttNode) {
							this.getAdditionalAttributes(child, null, attList);
						}
						if(!attList.isEmpty()) {
							value.add(String.join("|", attList));
						} else {
							value.add("");
						}
					} else {
						value.add(fieldNode.get(key).toString().replaceAll("\"", ""));
					}
				}
				values.add(value);
			}
		}
		
		FileWriter fileWriter = new FileWriter(this.entityMetaPath.toString());
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
	
	private void getAdditionalAttributes(JsonNode node, String key, List<String> list){
		if(node instanceof ArrayNode) {
			for(JsonNode child : node) {
				this.getAdditionalAttributes(child, null, list);
			}
		} else if(node instanceof ObjectNode) {
			for(Iterator<String> it = node.fieldNames(); it.hasNext();) {
				String akey = it.next();
				this.getAdditionalAttributes(node.get(akey), akey, list);
			}
		} else {
			list.add(key+"="+node.toString().replaceAll("\"", ""));
		}
	}
	
	public int createPartitionMeta() throws IOException {
		ArrayList<String> header = new ArrayList<String>(Arrays.asList("COLUMN_NAME", "COLUMN_DATATYPE", "POSITION"));
		ArrayList<String> keys = new ArrayList<String>(Arrays.asList("partitionFieldName", "fieldDataType", "partitionFieldPosition"));
		ArrayList<String> values = new ArrayList<String>();
		
		JsonNode partitionNode = this.jsonMap.get("result").get("partitionFields").get(0);
		
		if(partitionNode != null) {
			for(String key : keys) {
				values.add(partitionNode.get(key).toString().replaceAll("\"", ""));
			}
		}
		
		FileWriter fileWriter = new FileWriter(this.partitionMetaPath.toString());
		BufferedWriter csvWriter = new BufferedWriter(fileWriter);
		csvWriter.write(String.join(",", header));
		csvWriter.newLine();
		csvWriter.write(String.join(",", values));
		csvWriter.close();
		fileWriter.close();
		
		return 0;
	}
}
