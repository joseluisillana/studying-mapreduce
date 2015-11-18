package extracredit;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The sort mapper had to invert the key (the word pair) and the value (the 
 * frequency), so now we need to swap them back.
 */
public class WordCoSortReducer extends Reducer<LongWritable,Text,Text,LongWritable> {

  @Override
  public void reduce(LongWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    for (Text value : values) {
    	context.write(value,key);
    }
  }
}
