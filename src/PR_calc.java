import java.io.*;
import java.util.*;

//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

//input is the result of PR_initial: key ||pr||count||u1 u2 u3 ...
public class PR_calc {
    public static enum myCounter{
    	sum,
    	num
    }
	public static class Map extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException{
			
			String line = value.toString(); // key ||pr||count||u1 u2 u3 ...
			int split = line.indexOf("||");
			Text newKey = new Text(line.substring(0, split).trim());
			output.collect(newKey, new Text("0.0")); //in case only out, no in
			
			
			line = line.substring(split+2); // pr||count||u1 u2 u3 ...
			split = line.indexOf("||");
			float rank = Float.parseFloat(line.substring(0, split));
			
			
			line = line.substring(split+2); // count||u1 u2 u3 .......................
			output.collect(newKey, new Text("||"+line)); // ||count||u1 u2 u3 ... or ||count||
			split = line.indexOf("||");
			float count = Float.parseFloat(line.substring(0, split));
			
			if(line.length() > 2){
			
				line = line.substring(split+2); // u1 u2 u3 ....
				StringTokenizer st = new StringTokenizer(line);
				while(st.hasMoreTokens()){
					Text url = new Text(st.nextToken());
					Text val = new Text(String.valueOf(rank/count));
					output.collect(url, val);
				}
			}
			//reporter.getCounter(myCounter.preSum).increment(1);;
		}
	}
	
	public static class Reduce extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterator<Text> links, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException{
			float sum = 0.0f;
			String others = ""; // ||count||u1 u2 u3 ... or ||count||
			String tmp = "";
			while(links.hasNext()){
				tmp = links.next().toString();
				if(tmp.contains("||"))
					others = tmp; // // ||count||u1 u2 u3 ... or ||count||
				else
					sum += Float.parseFloat(tmp);
			}
			sum = 0.85f * sum + 0.15f;
			if(others.equals(""))
				others = "||0.0||"; // in case only in, no out
			output.collect(key, new Text("||" + Float.toString(sum) + others));
			
			reporter.getCounter(myCounter.sum).increment((long) (sum*1000));
			reporter.getCounter(myCounter.num).increment((1));
				
		}
	}
	
	public static void main(String[] args){
		
	}
}
