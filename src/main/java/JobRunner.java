import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class JobRunner {

    private static final int INPUT_PATH_INDEX = 0;
    private static final int OUTPUT_PATH_INDEX = 1;

    private List<String> arguments;
    private Job hadoopJob;

    private JobRunner(){ }

    public void setInputPath() throws IOException{
        FileInputFormat.addInputPath(hadoopJob,
                new Path(arguments.get(INPUT_PATH_INDEX)));
    }

    public void setOutputPath() throws IOException{
        FileOutputFormat.setOutputPath(hadoopJob,
                new Path(arguments.get(OUTPUT_PATH_INDEX)));
    }

    public boolean run()
            throws IOException, InterruptedException, ClassNotFoundException{
        return hadoopJob.waitForCompletion(true);
    }

    public static class JobRunnerBuilder{
        private List<String> arguments;
        private Job job;

        public JobRunnerBuilder withArguments(String[] args){
            this.arguments = Arrays.asList(args);
            return this;
        }

        public JobRunnerBuilder withJob(Job job){
            this.job = job;
            return this;
        }

        public JobRunner build(){
            JobRunner runner = new JobRunner();
            runner.arguments = this.arguments;
            runner.hadoopJob = this.job;
            return runner;
        }
    }
}
