import cn.edu.thu.tsfile.avro.AvroConverter;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.hadoop.HDFSOutputStream;
import cn.edu.thu.tsfile.timeseries.FileFormat.TsFile;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.File;

/**
 * Created by qiaojialin on 2017/4/28.
 */
public class Example {
    public static void main(String[] args) throws Exception {

        Schema avroSchema = new Schema.Parser().parse(new File("src/main/resources/kmx.avsc"));
        System.out.println(avroSchema);

        Configuration conf = new Configuration();
        String yamlPath = "src/main/resources/avro-tsfile.yaml";
        AvroConverter converter = new AvroConverter(yamlPath, conf);

        JSONObject tsfileSchema = converter.convertSchema(avroSchema);
        System.out.println(tsfileSchema);

        TSRandomAccessFileWriter hdfsOutput = new HDFSOutputStream("src/main/resources/kmx.tsfile", conf, true);
        TsFile tsFile = new TsFile(hdfsOutput, tsfileSchema);

        Path sequencePath = new Path("src/main/resources/windfarm_BMdxz_001.seq");
        SequenceFile.Reader sequenceReader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(sequencePath));

        int i = 0;
        try {
            Text key = new Text();
            BytesWritable value = new BytesWritable();
            GenericRecord record;
            while (sequenceReader.next(key, value)) {
                i++;
                ByteArrayInputStream in = new ByteArrayInputStream(value.getBytes());
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
                DatumReader<GenericRecord> avroReader = new SpecificDatumReader<>(avroSchema);
                record = avroReader.read(null, decoder);
                TSRecord tsRecord = converter.convertRecord(record);
                tsFile.writeLine(tsRecord);
            }
        } finally {
            IOUtils.closeStream(sequenceReader);
            tsFile.close();
        }
        System.out.println(i);
    }
}
