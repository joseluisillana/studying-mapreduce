package extracredit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class WordCoSort extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: WordCoSort <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(WordCoSort.class);
    job.setJobName("Sort Word Co-Occurrence");
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    /*
     * Input is in the form word,word<tab>int, so we can use the
     * KeyValueTextInputFormat class which splits the input line
     * using the tab as a delimiter by default.  So the input
     * to the mapper will be key=word,word/value=int.
     */
    job.setInputFormatClass(KeyValueTextInputFormat.class);

    /*
     * Set Mapper and Reducer classes
     */
    job.setMapperClass(WordCoSortMapper.class);
    job.setReducerClass(WordCoSortReducer.class);

    /*
     * Set Mapper and Reducer types
     */
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    
    /*
     * The default sort order would be ascending, but we want descending so
     * we need a custom comparator to sort by
     */
    job.setSortComparatorClass(DescendingLongComparator.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new WordCoSort(), args);
    System.exit(exitCode);
  }
}
