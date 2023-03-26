import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text company = new Text();
    private DoubleWritable rating = new DoubleWritable();

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        // Extract company name and rating fields

        String companyStr = fields[0].trim();
        String ratingStr = fields[1].trim();
        double ratingDouble = Double.parseDouble(ratingStr);
        company.set(companyStr);
        rating.set(ratingDouble);
        context.write(company, rating);
    }
}