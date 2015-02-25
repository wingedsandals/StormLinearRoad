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
import java.util.Map;

/**
 * Created by jdu on 1/29/15.
 */
public class LinearRoadSpout implements IBatchSpout {

    private TopologyContext context;
    private Map config;

    private ArrayList<String> str_positions;
    private int m_position = 0;
    private int m_flag = 0;
    


    @Override
    public void open(Map map, TopologyContext topologyContext) {
        
        this.config = map;
        this.context = topologyContext;
        
        String  line = null;
        str_positions = new ArrayList<String>();

        try{
            // open input stream test.txt for reading purpose.
            InputStream fis=new FileInputStream(LinearRoadConstants.inputFile);
            BufferedReader br = new BufferedReader( new InputStreamReader(fis) );
            while ((line = br.readLine()) != null) {
                str_positions.add(line);
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
        if(hasMorePositions()) {
            Values c = getPositionValues(nextPositionString());
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
        return new Fields("flag", "time", "vid", "qid",
        		"spd", "xway", "lane", "dir", "seg", "pos",
//        		"part_id", 
        		"segbegin", "segend", "day", "tod");
    }


    public boolean hasMorePositions()
    {
        int size = this.str_positions.size();

        if(m_position >= size)
            return false;
        else
            return true;
    }
    
    private static Values getPositionValues(String position) {
        String[] values = position.split(",");

        long flag = Integer.valueOf(values[0]);
        long time = Long.valueOf(values[1]);
        long vid = Long.valueOf(values[2]);
        long qid = Long.valueOf(values[3]);
        int spd = Integer.valueOf(values[4]);
        int xway = Integer.valueOf(values[5]);
        int lane = Integer.valueOf(values[6]);
        int dir = Integer.valueOf(values[7]);
        int seg = Integer.valueOf(values[8]);
        int pos = Integer.valueOf(values[9]);
//        int part_id = Integer.valueOf(values[10]);
        int segbegin = Integer.valueOf(values[11]);
        int segend = Integer.valueOf(values[12]);
        int day = Integer.valueOf(values[13]);
        int tod = Integer.valueOf(values[14]);
        
        return new Values(flag, time, vid, qid, 
        		spd, xway, lane, dir, seg, pos,
//        		part_id, 
        		segbegin, segend, day, tod);
    }
    
    public Values getCurrentPositionValues() {
    	return getPositionValues(str_positions.get(m_position));
    }

    public String nextPositionString()
    {
        String position = "";
        System.out.println("get position at: " + m_position);
        position = str_positions.get(m_position);
        m_position++;
        return position;
    }
    

}
