import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    protected void reduce(Text word, Iterable<IntWritable> values, Context ctx){
        /* super complex logic right here folks! */
        int sum = 0;
        for(IntWritable value : values){
            sum += value.get();
        }

        try {
            ctx.write(word, new IntWritable(sum));
        }catch(IOException ioException){
            System.out.println("Issues with writing output values to context for word: "
                    + word.toString()
                    + ioException.getLocalizedMessage());
        }catch(InterruptedException intException) {
            System.out.println("Issues with writing output values to context for word: "
                    + word.toString()
                    + intException.getLocalizedMessage());
        }
    }
}
