import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompanyJobCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text company = new Text();
    private final IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");
        // extract the company field
        company.set(words[0]);
        // output key-value pair
        context.write(company, one);
    }
}