import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

public class BookJobRunner extends Configured implements Tool {

    private static final String JOB_NAME = "Simple word count job";
    private static final int SUCCESS = 0;
    private static final int FAILURE = 1;
    /* implement the run method of the hadoop Tool interface */

    public int run(String[] args) throws Exception {

        JobBuilder jobBuilder = new JobBuilder();
        Optional<Job> hadoopJob = jobBuilder.withConfiguration(getConf())
                .withInputFormatClass(TextInputFormat.class)
                .withJarByClass(getClass())
                .withJobName(JOB_NAME)
                .withMapperClass(WordCountMapper.class)
                .withReducerClass(IntSumReducer.class)
                .withOutputKeyClass(Text.class)
                .withOutputValueClass(IntWritable.class)
                .build();

        if(!hadoopJob.isPresent()) {
            System.err.println("Error running Hadoop job: " + JOB_NAME);
            return FAILURE;
        }


        JobRunner hadoopJobRunner = new JobRunner.JobRunnerBuilder()
                .withJob(hadoopJob.get())
                .withArguments(args)
                .build();
        hadoopJobRunner.setInputPath();
        hadoopJobRunner.setOutputPath();

        return hadoopJobRunner.run() ? SUCCESS : FAILURE;
    }
}
