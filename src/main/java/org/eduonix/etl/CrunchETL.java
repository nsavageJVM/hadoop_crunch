package org.eduonix.etl;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.crunch.*;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.util.*;

/**
 * Created by ubu on 4/27/14.
 */
public class CrunchETL {

    private String input;
    private String extractOut;

    private static final Splitter SPLITTER = Splitter.onPattern("\\s+").omitEmptyStrings();


    public CrunchETL(String input, String extractOut) {
        this.input = input;
        this.extractOut = extractOut;
    }

    public void extractData() {

        Pipeline pipeline = MemPipeline.getInstance();

        PCollection<String> lines = pipeline.read(From.textFile(input));

        System.out.println("extract phase, data-source meta data : " + lines.getName() + ": " + lines.getSize());

        PObject <Collection<String>> dirtyData = lines.asCollection();
        Iterator<String> tokenStore = dirtyData.getValue().iterator();
        Set<List<String>> cleanLines = Sets.newHashSet();


        while (tokenStore.hasNext()) {

            String dirtyLine = tokenStore.next();

            List<String> dirtyTokens = SPLITTER.splitToList(dirtyLine);

            if(dirtyTokens.size() != 2)   continue;

            if(dirtyTokens.size() == 2 ) {

                if(dirtyTokens.get(0).matches("[\\D]*") || dirtyTokens.get(0).matches("[\\D]*")) continue;

            }


            cleanLines.add(dirtyTokens);

        }

        PCollection< List<String>> cleanLinesP = MemPipeline.collectionOf(cleanLines);

        pipeline.writeTextFile(cleanLinesP, extractOut+"/extract");
        pipeline.done();


    }



    public void transformData() throws IOException {

        Path transformInputPath = new Path(extractOut+"/extract", "out1.txt");

        Path transformOutputPath = new Path(extractOut+"/extract/transform/");

        Pipeline pipeline = MemPipeline.getInstance();

        PCollection<String> lines =  pipeline.read(From.textFile(transformInputPath.toString()));

        System.out.println("transform phase, data-source meta data : " + lines.getName() + ": " + lines.getSize());

        PCollection<LineItem> lineItems = lines.parallelDo(
                new MapFn<String, LineItem>() {
                    @Override
                    public LineItem map(String input) {

                        System.out.println("proc1 " + input);
                        String[] fields = input.split(",");
                        LineItem li = clean(fields);
                        return li;
                    }
                }, Avros.reflects(LineItem.class));

        Iterator<LineItem> iter = lineItems.materialize().iterator();

        FileSystem fs = FileSystem.getLocal(new Configuration());

        BufferedWriter bufferedWriter =new BufferedWriter(new OutputStreamWriter(fs.create(transformOutputPath, true)));

        while (iter.hasNext()) {
            bufferedWriter.write(iter.next().toString());

        }

        bufferedWriter.close();

        pipeline.done();


    }


    public void loadData() throws IOException {


        Path transformInputPath = new Path(extractOut+"/extract", "transform");

        Pipeline pipeline = MemPipeline.getInstance();

        PCollection<String> lines =  pipeline.read(From.textFile(transformInputPath.toString()));

        System.out.println("load phase, data-source meta data : " + lines.getName() + ": " + lines.getSize());

        PCollection<JsonObject> lineItems = lines.parallelDo(

                new MapFn<String, JsonObject>() {
                    @Override
                    public JsonObject map(String input) {

                        JsonObject li = new JsonParser().parse(input).getAsJsonObject();
                        return li;
                    }
                }, Avros.reflects(JsonObject.class));

        Iterator<JsonObject> iter = lineItems.materialize().iterator();
        while (iter.hasNext()) {
            System.out.println(iter.next().toString());

        }



    }





    private LineItem  clean(String[] tokens) {

        String cleanX = tokens[0];
        cleanX = cleanX.substring(1).replaceAll(",","");
        String cleanY = tokens[0].substring(0,(tokens[0].length())).replaceAll(",","");
        cleanY= cleanY.substring(1,(cleanY.length())).replaceAll(",","");

        System.out.println(cleanX+"   :   "+cleanY);

        BigDecimal x = BigDecimal.valueOf(Math.abs(Double.valueOf(cleanX)));
        BigDecimal y = BigDecimal.valueOf(Double.valueOf(cleanY));
        BigDecimal z =  y.multiply(x);
        x = x.movePointRight(6);
        y = y.movePointRight(6);
        z = z.movePointRight(6);
        String xStr = x.toString();
        String yStr =  y.toString();
        String zStr =  z.toString();
        LineItem co_ord = new LineItem(xStr, yStr, zStr);

        return co_ord;
    }







}
