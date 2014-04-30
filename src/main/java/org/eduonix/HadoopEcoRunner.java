package org.eduonix;

import org.apache.hadoop.fs.Path;
import org.eduonix.etl.CrunchETL;

import java.io.IOException;

/**
 * Created by ubu on 4/27/14.
 */
public class HadoopEcoRunner {

    private static String uniquePathId = ""+System.currentTimeMillis();
    private static Path testDataInput = new Path("./testData","seismic");
    private static Path extractDataTestOutput = new Path("./EtlDataOut/"+uniquePathId);


    public static void main(String[] args) throws IOException {

        CrunchETL crunchETL = new CrunchETL(testDataInput.toString(), extractDataTestOutput.toString());
        crunchETL.extractData();
        crunchETL.transformData();
        crunchETL.loadData();

    }



}
