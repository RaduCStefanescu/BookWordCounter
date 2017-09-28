import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class BookJobRunner extends Configured implements Tool {

    /* implement the run method of the hadoop Tool interface */

    public int run(String[] args) throws Exception {
        /* set the parameters for the Job we want to run */
        Job job = new Job(getConf());
        /* we set the input format to the TextInputFormat that comes
        out of the box with Hadoop. The format reads the input files and
        provides to the mapper an input format (Object, Text) where Text is
        one line retrieved from the file
         */
        job.setInputFormatClass(TextInputFormat.class);
        /* stuff you also need to set, take my word for it */
        job.setJarByClass(getClass());
        job.setJobName(getClass().getSimpleName());

        /* set the input and output paths */
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /* set the implementions for our mapper and reducer */
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
