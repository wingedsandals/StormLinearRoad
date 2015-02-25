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

public class InputDB extends TridentState implements State {
	
	protected InputDB(TridentTopology topology, Node node) {
		super(topology, node);
		// TODO Auto-generated constructor stub
	}

	ArrayList<Input> inputs = new ArrayList<Input>();

	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub
		
	}
	
	public void setInputBulk(List<Input> inputs) {
		for (int i = 0; i < inputs.size(); i++) {
			this.inputs.add(new Input(inputs.get(i)));
		}
		
	}
	
	public List<Input> getInputBulk() {
		return inputs;
	}
	
	public Integer getInputCount() {
		return inputs.size();
	}
	
}
