package state;

public class SegmentHistoryKey {
	public Integer xway;
	public Integer dir;
	public Integer seg;
	public Integer absday;
	public Integer dow;
	public long tod;
	
	public SegmentHistoryKey(Integer xway, Integer dir, Integer seg, 
			Integer absday, Integer dow, long tod) {
		this.xway = xway;
		this.dir = dir;
		this.seg = seg;
		this.absday = absday;
		this.dow = dow;
		this.tod = tod;
	}
	

	public SegmentHistoryKey(Integer xway, Integer dir, Integer seg, 
			Integer absday, long tod) {
		this.xway = xway;
		this.dir = dir;
		this.seg = seg;
		this.absday = absday;
		this.tod = tod;
	}
	
	public String toString() {
		return String.valueOf(this.xway) + " " +
				String.valueOf(this.dir) + " " +
				String.valueOf(this.seg) + " " +
				String.valueOf(this.absday) + " " +
				String.valueOf(this.tod);
	}
	
}
