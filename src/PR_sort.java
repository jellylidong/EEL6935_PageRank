import java.io.IOException;
//import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

//input is the result of PR_calc: key ||pr||count||u1 u2 u3 ...
public class PR_sort {
	public static class Map extends MapReduceBase
	implements Mapper<LongWritable, Text, FloatWritable, Text>{
		public void map(LongWritable key, Text value, OutputCollector<FloatWritable, Text> output, Reporter reporter)
		throws IOException{
			// key ||pr||count||u1 u2 u3 ...
			String line = value.toString();
			int split = line.indexOf("||");
			Text newKey = new Text(line.substring(0, split).trim());
			
			// pr||count||u1 u2 u3 ...
			line = line.substring(split+2);
			split = line.indexOf("||");
			float rank = -1.0f * Float.parseFloat(line.substring(0, split));
			
			output.collect(new FloatWritable(rank), newKey);
//			output.collect(new Text(Float.toString(rank)), newKey);
		}
	}
	
	public static class Reduce extends MapReduceBase
	implements Reducer<FloatWritable, Text, Text, FloatWritable>{
		public void reduce(FloatWritable key, Iterator<Text> links, OutputCollector<Text, FloatWritable> output, Reporter reporter)
		throws IOException{
			//Text newVal = new Text(Float.toString(-1.0f * key.get()));
			float tmp = -0.1f * Float.parseFloat(key.toString());
//			Text newVal = new Text(Float.toString(tmp));
			while(links.hasNext())
				output.collect(links.next(), new FloatWritable(tmp));
//				output.collect(links.next(), newVal);
		}
	}
}
