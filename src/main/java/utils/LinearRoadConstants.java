package utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;

import com.sun.xml.internal.bind.v2.schemagen.xmlschema.List;

/**
 * Created by jdu on 2/25/15.
 */
public class LinearRoadConstants {

    public static String inputFile = "votes-o-40.txt";
    public static String timestampsInputFile = "";
    public static String streamId = "LinearRoadTridentTopology";
    public static int numContestants = 6;
    public static int memcachedPort = 11211;
    public static InetSocketAddress server = new InetSocketAddress("localhost", memcachedPort);
    public static ArrayList servers = new ArrayList(Arrays.asList(server));

	public static final String FILE_DIR = "/media/john/Files2/linearroad/xways/1/";
	public static final String HISTORIC_TOLLS_FILE = "historical-tolls.out";
	public static final String SEGMENT_STATISTICS_FILE = "seg_history.out0";
	
	public static final String VEHICLE_TABLE_NAME = "tolls_per_vehicle";
	public static final String SEG_STATS_TABLE_NAME = "segment_history";
	public static final String EMIT_TIME_STR = "XXXXX";
	
	public static final int  BATCH_SIZE = 1000;
	public static final int  MAX_ARRAY_SIZE = 10000000;
	public static final int  XWAY_COL = 2;
	public static final int  SEG_STATS_ABSDAY_COL = 0;
	public static final int  SEG_STATS_DOW_COL_TABLE = 9;
    
	// potential return codes
    public static final long INSERT_POSITION_SUCCESSFUL = 0;
    public static final long DETECT_ACCIDENT_SUCCESSFUL = 1;
    public static final long CALCULATE_TOLLS_SUCCESSFUL = 2;
    public static final long PROCEDURE_SUCCESSFUL = 4;
    public static final long INSERT_POSITION_ERR_BAD_SEG = 5;
    
    public static final int NUMBER_OF_XWAYS = 1;
    public static final int NUM_PARTITIONS = 1;
    
    public static final int REPORT_FREQUENCY = 30;
    public static final int STOPPED_POS_POINTS = 4;
    public static final int MINUTES_PER_DAY = 1440;
    public static final int NUM_MINUTES_HISTORY = 2;
    public static final int ENTRANCE_LANE = 0;
    public static final int EXIT_LANE = 4;
    
    public static final int IN_PORT_NUM = 8800;
    public static final int OUT_PORT_NUM = 8900;
    public static final String OUT_HOST = "localhost";
    
    public static int findPartId(int xway)
    {
    	return xway % NUM_PARTITIONS;
    }
    
    public static int getDayOfWeek(int day)
    {
    	return day % 7 + 1;
    }
}
