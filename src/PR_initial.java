import java.io.*;
import java.util.*;

//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
public class PR_initial {
	public static class Map extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException{
			StringTokenizer st = new StringTokenizer(value.toString());
			Text newKey = new Text(st.nextToken());
			String urls = "";
			int count = 0;
			while(st.hasMoreTokens()){
				urls += st.nextToken() + " ";
				count++;
			}
			Text newVal = new Text("||" + "1" + "||" + Integer.toString(count) + "||" + urls);
			output.collect(newKey, newVal);
		}
	}
	
	public static class Reduce extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterator<Text> links, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException{
			while(links.hasNext())
				output.collect(key, links.next());
		}
	}
	
	public static void main(String[] args){
		
	}
}
