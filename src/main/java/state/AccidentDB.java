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

public class AccidentDB extends TridentState implements State {
	
	protected AccidentDB(TridentTopology topology, Node node) {
		super(topology, node);
		// TODO Auto-generated constructor stub
	}

	Map<AccidentKey, Accident> accidents = new HashMap<AccidentKey, Accident>();

	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub
		
	}
	
	public void setAccidentBulk(List<Accident> accidents) {
		for (int i = 0; i < accidents.size(); i++) {
			this.accidents.put(
					new AccidentKey(accidents.get(i).xway, accidents.get(i).lane, accidents.get(i).dir, accidents.get(i).seg), 
					new Accident(accidents.get(i)));
		}
		
	}
	
	public List<Accident> getAccidentBulk(List<AccidentKey> accidentKeys) {
		List<Accident> ret = new ArrayList<Accident>();
		for (AccidentKey accidentKey: accidentKeys) {
			ret.add(accidents.get(accidentKey));
		}
		
		return ret;
	}
	
	public Integer getAccidentCount() {
		return accidents.size();
	}
	
}
