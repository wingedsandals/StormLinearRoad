package utils;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.opencsv.CSVWriter;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by cpa on 2/19/15.
 */
public class JSONUtils {
    /**
     * Parses JSON published by Kafka.
     **/
    public static class ParseJSON extends BaseFunction {
        /**
         * Constant: HTTP Code 200.
         **/
        private static final int HTTP_CODE_200 = 200;
        /**
         * Takes a tuple adds the RDNS and emits a new tuple.
         *
         * @param tuple an TridentTuple that contains fields in JSON format
         * @param collector the TridentCollector
         **/
        @Override
        public final void execute(
                final TridentTuple tuple,
                final TridentCollector collector
        ) {
            byte[] bytes = tuple.getBinary(0);
            try {
                String decoded = new String(bytes);
                JSONObject json = new JSONObject(decoded);
                collector.emit(new Values(
                        json.getString("name")
                        , json.getInt("type")
                ));
            } catch (JSONException e) {
                System.err.println("Caught JSONException: " + e.getMessage());
            }
        }
    }
    /**
     * The WriteCSV class takes a TridentTuple and prints the fields to a flat
     * file.
     **/
    public static class WriteCSV extends BaseFunction {
        /** Constant: The file path where the file will be saved. **/
        public static final String FILE_PATH
                = "";
        /** Constant: The file name to save to. **/
        public static final String FILE_NAME = "example_out.csv";
        /** Constant: The number of fields in the csv doc. **/
        public static final int NUM_FIELDS = 2;
        /** Constant: The source where the instance came from. **/
        public static final int FIELD_NAME = 0;
        /** Constant: The type of instance (custid or ip). **/
        public static final int FIELD_TYPE = 1;
        @Override
        public final void execute(
                final TridentTuple tuple,
                final TridentCollector collector
        ) {
            try {
                CSVWriter writer = new CSVWriter(
                        new FileWriter(FILE_NAME, true), ',');
                String[] fields = new String[NUM_FIELDS];
                fields[FIELD_NAME]
                        = tuple.getStringByField("name");
                fields[FIELD_TYPE]
                        = tuple.getIntegerByField("type").toString();
                writer.writeNext(fields);
                writer.close();
                collector.emit(
                        new Values(tuple.getStringByField("name")));
            } catch (IOException e) {
                System.err.println("Caught IOException: " + e.getMessage());
            }
        }
    }

    /**
     * Check to make sure the environmental variable is set and return the
     * value.
     *
     * @param env variable to get the value of.
     * @return Environmental variable value.
     * @throws IOException if row length is incorrect
     **/
    private static String getEnvVar(final String env) throws IOException {
        String value = System.getenv(env);
        if (value == null) {
            throw new IOException(env + " environment variable is not set.");
        }
        return value;
    }



}
