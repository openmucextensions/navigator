package misc;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestTimer {

	public static void main(String[] args) throws ParseException {
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date firstTime = format.parse("2015-01-01 00:00:00");
		
		Date nextRun = new Date(getNextRun(firstTime.getTime(), 1000 * 60 * 15));
		
		System.out.println("Next run: " + nextRun.toString());
	}
	
	public static long getNextRun(long firstTime, long interval) {
		// calculate the next time stamp to run 
		long now = System.currentTimeMillis();	
		return now - ((now - firstTime) % interval) + interval;
	}
}