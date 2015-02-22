package spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import utils.LinearRoadConstants;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AccidentSpout implements IBatchSpout {

    private TopologyContext context;
    private Map config;

    private ArrayList<String> str_timestamps;
    private Map<Integer, String> timestamps;
    private int m_position = 0;
    private int m_flag = 0;
    


    @Override
    public void open(Map map, TopologyContext topologyContext) {
        
        this.config = map;
        this.context = topologyContext;
        
        String  line = null;
        str_timestamps = new ArrayList<String>();
        timestamps = new HashMap<Integer, String>();

        try{
            // open input stream test.txt for reading purpose.
            InputStream fis=new FileInputStream(LinearRoadConstants.timestampsInputFile);
            BufferedReader br = new BufferedReader( new InputStreamReader(fis) );
            while ((line = br.readLine()) != null) {
                str_timestamps.add(line);
                Values vs = getTimestampValues(line);
                timestamps.put((Integer) vs.get(0), line);
            }

            fis.close();
            br.close();
        } catch(Exception e) {
            e.printStackTrace();
        }

        m_position = 0;

        System.out.println("\n\n\n\n\n\n\n\n\n\nFIELDZ: " + this.getOutputFields());
    }

    @Override
    public void emitBatch(long l, TridentCollector tridentCollector) {
        if(hasMoreTimestamps()) {
            Values c = getTimestampValues(nextTimestampString());
            tridentCollector.emit(c);
        }
    }

    @Override
    public void ack(long l) {

    }

    @Override
    public void close() {

    }

    @Override
    public Map getComponentConfiguration() {
        return config;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("xway", "tod", "ts");
    }


    public boolean hasMoreTimestamps()
    {
        int size = this.str_timestamps.size();

        if(m_position >= size)
            return false;
        else
            return true;
    }
    
    private static Values getTimestampValues(String timestamp) {
        String[] values = timestamp.split(",");

        int xway = Integer.valueOf(values[0]);
        int tod = Integer.valueOf(values[1]);
        int ts = Integer.valueOf(values[2]);
        
        return new Values(xway, tod, ts);
    }

    public String nextTimestampString()
    {
        String position = "";
        System.out.println("get timestamp at: " + m_position);
        position = str_timestamps.get(m_position);
        m_position++;
        return position;
    }
    
    public Values getTimestampByXway(Integer xway) {
    	return getTimestampValues(timestamps.get(xway));
    }

}
