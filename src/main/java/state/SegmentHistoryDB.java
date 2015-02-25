package state;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.planner.Node;
import storm.trident.state.State;

public class SegmentHistoryDB extends TridentState implements State {
	
	protected SegmentHistoryDB(TridentTopology topology, Node node) {
		super(topology, node);
		// TODO Auto-generated constructor stub
	}

	Map<String, SegmentHistory> segmentHistories = new HashMap<String, SegmentHistory>();

	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub
		
	}
	
	public void setSegmentHistoryBulk(List<SegmentHistory> segmentHistories) {
		for (int i = 0; i < segmentHistories.size(); i++) {
			this.segmentHistories.put(
					new SegmentHistoryKey(segmentHistories.get(i).xway, 
							segmentHistories.get(i).dir, 
							segmentHistories.get(i).seg, 
							segmentHistories.get(i).absday,
							segmentHistories.get(i).dow,
							segmentHistories.get(i).tod).toString(), 
					new SegmentHistory(segmentHistories.get(i)));
		}
		
	}
	
	public List<SegmentHistory> getSegmentHistoryBulk(List<SegmentHistoryKey> segmentHistoryKeys) {
		List<SegmentHistory> ret = new ArrayList<SegmentHistory>();
		for (SegmentHistoryKey segmentHistoryKey: segmentHistoryKeys) {
			ret.add(segmentHistories.get(segmentHistoryKey.toString()));
		}
		
		return ret;
	}
	
	// update toll on the values
	public void updateToll(List<SegmentHistoryKey> segmentHistoryKeys) {
		setSegmentHistoryBulk(getSegmentHistoryBulk(segmentHistoryKeys));
	}
}
