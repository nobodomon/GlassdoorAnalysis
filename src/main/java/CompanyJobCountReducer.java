import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompanyJobCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable totalJobs = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        totalJobs.set(sum);
        // output the key-value pair
        context.write(key, totalJobs);
    }
}