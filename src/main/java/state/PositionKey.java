package state;

public class PositionKey {
	public Integer xway;
	public Long ts;
	public Long tod;
	public Long vid;
	
	public PositionKey(Integer xway, Long ts, Long tod, Long vid) {
		this.xway = xway;
		this.ts = ts;
		this.tod = tod;
		this.vid = vid;
	}
}
