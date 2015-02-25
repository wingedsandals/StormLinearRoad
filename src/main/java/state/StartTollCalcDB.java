package state;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Values;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.planner.Node;
import storm.trident.state.State;

public class StartTollCalcDB extends TridentState implements State {
	
	protected StartTollCalcDB(TridentTopology topology, Node node) {
		super(topology, node);
		// TODO Auto-generated constructor stub
	}

	Map<String, StartTollCalc> startTollCalcs = new HashMap<String, StartTollCalc>();

	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub
		
	}
	
	public void setStartTollCalcBulk(List<StartTollCalc> startTollCalcs) {
		for (int i = 0; i < startTollCalcs.size(); i++) {
			this.startTollCalcs.put(
					new StartTollCalcKey(startTollCalcs.get(i).tod, startTollCalcs.get(i).xway).toString(), 
					new StartTollCalc(startTollCalcs.get(i).xway,
							startTollCalcs.get(i).tod,
							startTollCalcs.get(i).ts));
		}
		
	}
	
	public List<StartTollCalc> getStartTollCalcBulk(List<StartTollCalcKey> startTollCalcKeys) {
		List<StartTollCalc> ret = new ArrayList<StartTollCalc>();
		for (StartTollCalcKey startTollCalcKey: startTollCalcKeys) {
			ret.add(startTollCalcs.get(startTollCalcKey.toString()));
		}
		
		return ret;
	}
	
	public Integer getStartTollCalcCount() {
		return startTollCalcs.size();
	}
	
	// TODO: get the min tod, xway
	public Values getMinTodXway() {
		Values ret = new Values();
		
		return ret;
	}
	
	
}
