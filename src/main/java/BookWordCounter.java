import org.apache.hadoop.util.ToolRunner;

public class BookWordCounter {
    public static void main(String[] args)
            throws Exception {

        /* use the hadoop core provided ToolRunner class
        * to run our job*/
        int rc = ToolRunner.run(new BookJobRunner(), args);
        System.exit(rc);
    }
}
