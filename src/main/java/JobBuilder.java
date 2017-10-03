import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class JobBuilder {

    private Job job;
    private Configuration configuration;
    private Class inputFormatClass;
    private Class jarClass;
    private String jobName;
    private Class mapperClass;
    private Class reducerClass;
    private Class outputKeyClass;
    private Class outputValueClass;

    public JobBuilder withConfiguration(Configuration conf){
        this.configuration = conf;
        return this;
    }

    public JobBuilder withInputFormatClass(Class inputFormatClass) {
        this.inputFormatClass = inputFormatClass;
        return this;
    }

    public JobBuilder withJarByClass(Class jarClass){
        this.jarClass = jarClass;
        return this;
    }

    public JobBuilder withJobName(String jobName){
        this.jobName = jobName;
        return this;
    }

    public JobBuilder withMapperClass(Class mapperClass){
        this.mapperClass = mapperClass;
        return this;
    }

    public JobBuilder withReducerClass(Class reducerClass) {
        this.reducerClass = reducerClass;
        return this;
    }

    public JobBuilder withOutputKeyClass(Class outputKeyClass){
        this.outputKeyClass = outputKeyClass;
        return this;
    }

    public JobBuilder withOutputValueClass(Class outputValueClass){
        this.outputValueClass = outputValueClass;
        return this;
    }

    public Optional<Job> build(){
        try {
            job = new Job();
        }catch(IOException exception){
            System.err.println("Error creating new hadoop job: " + exception.getStackTrace());
            return Optional.absent();
        }

        job.setInputFormatClass(this.inputFormatClass);
        job.setJarByClass(this.jarClass);
        job.setJobName(this.jobName);

        /* set the implementions for our mapper and reducer */
        job.setMapperClass(this.mapperClass);
        job.setReducerClass(this.reducerClass);

        job.setOutputKeyClass(this.outputKeyClass);
        job.setOutputValueClass(this.outputValueClass);

        return Optional.of(job);
    }

}
