
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import java.io.IOException;
import org.apache.hadoop.fs.*;

public class PR_runAll {
	public static void main(String[] args) throws IOException{
		
		String pathPre = "/home/vcoder/EDA/hadoop/";
		//String pathPre = args[0];
		//int iter = Integer.parseInt(args[0]);
		//PageRank Initial
		JobConf initialConf = new JobConf(PR_initial.class);
		initialConf.setJobName("initial");
		
		FileSystem initialFs = FileSystem.get(initialConf);
//		
		Path initialIn = new Path(pathPre + "input_initial");
		Path initialOut= new Path(pathPre + "output_initial");
		if(initialFs.exists(initialOut))
			initialFs.delete(initialOut, true);
		initialConf.setOutputKeyClass(Text.class);
		initialConf.setOutputValueClass(Text.class);
		initialConf.setMapperClass(PR_initial.Map.class);
		initialConf.setReducerClass(PR_initial.Reduce.class);
		initialConf.setInputFormat(TextInputFormat.class);
		initialConf.setOutputFormat(TextOutputFormat.class);
//		
//	
		FileInputFormat.setInputPaths(initialConf, initialIn);
		FileOutputFormat.setOutputPath(initialConf,initialOut);
	
		JobClient.runJob(initialConf);
		
		//PageRank Calculation
		JobConf calcConf = new JobConf(PR_calc.class);
		calcConf.setJobName("Calculation");
		
		Path calcIn = initialOut;
		Path calcOut= new Path(pathPre + "output_calc1");
		long curSum = 0;
		long preSum = 0;
		int i = 0;
//		for(int i = 1; i <= iter; i++){
		while(Math.abs(curSum - preSum) > 1 && Math.abs(curSum - preSum) != 0){
			i++;
			calcIn = (i == 1)? calcIn : calcOut;
			calcOut = new Path(pathPre + "output_calc" + Integer.toString(i));
			FileSystem calcFs = FileSystem.get(calcConf);
			if(calcFs.exists(calcOut))
				calcFs.delete(calcOut, true);
			
			calcConf.setOutputKeyClass(Text.class);
			calcConf.setOutputValueClass(Text.class);
			calcConf.setMapperClass(PR_calc.Map.class);
			calcConf.setReducerClass(PR_calc.Reduce.class);
			calcConf.setInputFormat(TextInputFormat.class);
			calcConf.setOutputFormat(TextOutputFormat.class);
			
		
			FileInputFormat.setInputPaths(calcConf, calcIn);
			FileOutputFormat.setOutputPath(calcConf,calcOut);
		
			RunningJob calcJob = JobClient.runJob(calcConf);
			long num = calcJob.getCounters().findCounter("myCounter", "num").getValue();
			preSum = curSum;
			curSum = calcJob.getCounters().findCounter("myCounter", "sum").getValue()/num;
			calcJob.getCounters().findCounter("myCounter", "sum").setValue(0);
			
			calcConf = new JobConf(PR_calc.class);
			calcConf.setJobName("Calculation");
		}
		
		//Sort result
		JobConf sortConf = new JobConf(PR_sort.class);
		sortConf.setJobName("Sort");
		
		Path sortIn = calcOut;
		Path sortOut= new Path(pathPre + "output_sort");
		FileSystem sortFs = FileSystem.get(sortConf);
		if(sortFs.exists(sortOut))
			sortFs.delete(sortOut, true);
		
		sortConf.setOutputKeyClass(FloatWritable.class);
		sortConf.setOutputValueClass(Text.class);
		sortConf.setMapperClass(PR_sort.Map.class);
		sortConf.setReducerClass(PR_sort.Reduce.class);
		sortConf.setInputFormat(TextInputFormat.class);
		sortConf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(sortConf, sortIn);
		FileOutputFormat.setOutputPath(sortConf,sortOut);
	
		JobClient.runJob(sortConf);
		
		//Statistic
		JobConf statConf = new JobConf(PR_statistic.class);
		statConf.setJobName("Statistic");
		
		Path statIn = calcOut;
		Path statOut= new Path(pathPre + "output_statistic");
		FileSystem statFs = FileSystem.get(statConf);
		if(statFs.exists(statOut))
			statFs.delete(statOut, true);
		statConf.setOutputKeyClass(Text.class);
		statConf.setOutputValueClass(Text.class);
		statConf.setMapperClass(PR_statistic.Map.class);
		statConf.setReducerClass(PR_statistic.Reduce.class);
		statConf.setInputFormat(TextInputFormat.class);
		statConf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(statConf, statIn);
		FileOutputFormat.setOutputPath(statConf,statOut);
	
		JobClient.runJob(statConf);
	}
}
