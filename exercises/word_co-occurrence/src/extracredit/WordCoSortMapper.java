package extracredit;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoSortMapper extends Mapper<Text, Text, LongWritable, Text> {

  @Override
  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
    
	  	long longvalue = Long.parseLong(value.toString());
        context.write(new LongWritable(longvalue), key);
  }
}
