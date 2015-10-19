import java.io.IOException;
import java.util.Iterator;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

//input is the results of PR_calc
public class PR_statistic {
	public static class Map extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException{
			// key ||pr||count||u1 u2 u3 ...
			String line = value.toString();
			int split = line.indexOf("||");
			Text newKey = new Text(line.substring(0, split).trim());
			
			
			// pr||count||u1 u2 u3 ...
			line = line.substring(split+2);
			split = line.indexOf("||");
//			float rank = Float.parseFloat(line.substring(0, split));
			
			
			// count||u1 u2 u3 ...
			line = line.substring(split+2);
			//output.collect(newKey, new Text("||"+line)); // ||count||u1 u2 u3 ... or ||count||
			split = line.indexOf("||");
			float count = Float.parseFloat(line.substring(0, split));
			
			Text oneKey = new Text("key");
			
			output.collect(oneKey, new Text("node_" + newKey.toString()));
			output.collect(oneKey, new Text("edge_" + Float.toString(count)));
		}
	}
	
	public static class Reduce extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterator<Text> links, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException{
			float nodes = 0.0f;
			float edges = 0.0f;
			float maxEdge = Float.MIN_VALUE;
			float minEdge = Float.MAX_VALUE;
			
			while(links.hasNext()){
				String tmp = links.next().toString();
				if(tmp.contains("node_"))
					nodes += 1.0f;
				else{
					int split = tmp.indexOf("_");
					float tmpVal = Float.parseFloat(tmp.substring(split + 1));
					//float tmpVal = Float.parseFloat(tmp.substring(5));
				
					edges += tmpVal;
					maxEdge = Math.max(maxEdge, tmpVal);
					minEdge = Math.min(minEdge, tmpVal);
				}
			}
			output.collect(new Text("Total nodes"), new Text(Float.toString(nodes)));
			output.collect(new Text("Total edges"), new Text(Float.toString(edges)));
			output.collect(new Text("Maximum edges"), new Text(Float.toString(maxEdge)));
			output.collect(new Text("Minimum edges"), new Text(Float.toString(minEdge)));
			output.collect(new Text("Average edges"), new Text(Float.toString(edges/nodes)));
		}
	}
	
	public static void main(String[] args){
		
	}
}
