package org.eduonix.arvoserde;


import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;


import java.io.*;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by ubu on 5/1/14.
 */
public class SerdeArvo {

    // https://github.com/apache/avro/blob/trunk/lang/java/mapred/src/test/java/org/apache/avro/mapred/TestReflectJob.java

    private  String uniquePathId = ""+System.currentTimeMillis();

    private  Path input;

    private Path serdeOut = new Path("./SerdeDataOut/"+uniquePathId);

    private Path serdeIn;

    public SerdeArvo(Path input) {

        this.input = input;
    }








    public void testDataPipe() throws Exception {

        File file = new File(input.toString()+"/out1.txt");
        List<String> lines = Files.readLines(file, Charsets.UTF_8);
        System.out.println(lines.size());

        writeLinesFile(new File(input.toString()),lines );

        serdeIn = new Path(input.toString(),"lines.avro" );

        File dataFile = new File(serdeIn.toString());
        validateFile(dataFile);

    }



    private void writeLinesFile(File dir, List<String> lines) throws IOException {
        DatumWriter<Line> writer = new ReflectDatumWriter<Line>();
        DataFileWriter<Line> out = new DataFileWriter<Line>(writer);
        File linesFile = new File(dir+"/lines.avro");
        dir.mkdirs();
        out.create(ReflectData.get().getSchema(Line.class), linesFile);
        for (String line : lines)
            out.append(new Line(line));
        out.close();
    }

    private void validateFile(File file) throws Exception {
        DatumReader<Line> reader = new ReflectDatumReader<Line>();
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        DataFileStream<Line> counts = new DataFileStream<Line>(in,reader);

        for (Line wc : counts) {
            System.out.println(wc.getText());

        }
        in.close();

    }



    static class  Line {
        private String text = "";
        public  Line() {}
        public  Line(String text) { this.text = text; }
        public String toString() { return text; }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }
    }

    static class LineCount {
        private long count;
        public LineCount() {}
        public LineCount(long count) { this.count = count; }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }


}



