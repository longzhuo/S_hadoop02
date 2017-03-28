package sort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<IntPaire, IntWritable, Text, Text> {

	@Override
	protected void reduce(IntPaire key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		StringBuffer combineValue = new StringBuffer();
		Iterator<IntWritable> itr = values.iterator();
		while(itr.hasNext()){
			int value = itr.next().get();
			combineValue.append(value + ",");
		}
		int length = combineValue.length();
		String str = "";
		if(combineValue.length() > 0){
			str = combineValue.substring(0, length-1);
		}
		context.write(new Text(key.getFirstKey()), new Text(str));
	}
	
}
