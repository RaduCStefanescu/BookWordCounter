import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context ctx){

        /* the input format will be something like (something, line) */
        /* we need to parse each line for obtaining the words available on that line*/
        Text word = new Text();
        StringTokenizer tokenizer = new StringTokenizer(value.toString()
                .replaceAll("[^A-Za-z]+", " ").toLowerCase());

        while(tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            try {
                ctx.write(word, ONE);
            } catch (IOException ioexception){
                System.out.println("Issues with writing intermediate values to context for word: "
                        + word.toString()
                        + ioexception.getLocalizedMessage());
            } catch (InterruptedException intexception){
                System.out.println("Issues with writing intermediate values to context for word: "
                        + word.toString()
                        + intexception.getLocalizedMessage());
            }
        }
    }
}
