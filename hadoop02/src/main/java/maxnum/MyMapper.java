package maxnum;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable>{
	
	@Override
	protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(new Text(), new IntWritable(Integer.parseInt(value.toString())));
	}
	
}
