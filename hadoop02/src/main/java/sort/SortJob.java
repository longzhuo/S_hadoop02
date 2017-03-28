package sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SortJob {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Sortint");
		job.setJarByClass(SortJob.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		//设置输入格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//设置map的输出类型
		job.setMapOutputKeyClass(IntPaire.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置排序
		job.setSortComparatorClass(TextIntComparator.class);
		
		//设置group
		job.setGroupingComparatorClass(TextComparator.class);//以key进行grouping
		
		job.setPartitionerClass(PartitionByText.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/huhui/input/words.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/output"));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
