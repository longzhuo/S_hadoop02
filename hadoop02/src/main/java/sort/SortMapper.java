package sort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Object, Text, IntPaire, IntWritable>{

	public IntPaire intPaire = new IntPaire();
	public IntWritable intWritable = new IntWritable(0);
	
	@Override
	protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int intValue = Integer.parseInt(value.toString());
		intPaire.setFirstKey(key.toString());
		intPaire.setSecondKey(intValue);
		System.out.println("firstKey="+key.toString()+"  secondKey="+intValue);
		intWritable.set(intValue);
		context.write(intPaire, intWritable);//key:str1  value:5
	}
}
