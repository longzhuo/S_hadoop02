package org.temp;    

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

	public static void main(String[] args) 
		throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			System.err.println(-1);
		}
		
		// 作业
		Job job = new Job();
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("最大的温度");
		
		// 文件I/O
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 映射，归约
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		// 输出	键，值
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}

}
 