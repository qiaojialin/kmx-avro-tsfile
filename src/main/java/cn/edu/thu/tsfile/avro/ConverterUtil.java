package cn.edu.thu.tsfile.avro;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Stefanie Zhao
 */

public class ConverterUtil {

    public ConverterUtil(){

    }
    
    public TSDataType convertAvroTypeToTsfile(Schema.Type type) {
		if (type.equals(Schema.Type.INT)) {
			return TSDataType.INT32;
		} else if (type.equals(Schema.Type.LONG)) {
			return TSDataType.INT64;
		} else if (type.equals(Schema.Type.BOOLEAN)) {
			return TSDataType.BOOLEAN;
		} else if (type.equals(Schema.Type.FLOAT)) {
			return TSDataType.FLOAT;
		} else if (type.equals(Schema.Type.DOUBLE)) {
			return TSDataType.DOUBLE;
		} else if (type.equals(Schema.Type.STRING)) {
			return TSDataType.BYTE_ARRAY;
		} else if (type.equals(Schema.Type.BYTES)) {
			return TSDataType.BYTE_ARRAY;
		} else if (type.equals(Schema.Type.ENUM)) {
			return TSDataType.ENUMS;
		} else {
			throw new UnsupportedOperationException("Cannot convert Avro type : " + type);
		}
	}

    public Map loadYaml(String filePath) throws FileNotFoundException {
        
		File yamlFile = new File(filePath);
		return Yaml.loadType(yamlFile, HashMap.class);
    }

	public Map loadYaml(String filePath, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path file = new Path(filePath);
		FSDataInputStream getIt = fs.open(file);
		return Yaml.loadType(getIt, HashMap.class);
	}

    public static void main(String[] args) throws Exception {

        String testFilePath = System.getProperty("user.dir") + "/src/main/resources";
        String testFileName2 = "tsfile-avro.yaml";
        ConverterUtil util = new ConverterUtil();

        Map result = util.loadYaml(testFilePath + testFileName2);
        for(Object key : result.keySet()) {
        	Map re = (Map)result.get(key);
        	System.out.println("-----" + key + "-----");
        	for(Object subkey: re.keySet()){
        		System.out.println(subkey + ":" + re.get(subkey));
        	}
        }
	}
}
