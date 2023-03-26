import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class RatingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private HashMap<String, Double[]> companyRatings = new HashMap<>();

    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer.Context context) throws IOException, InterruptedException {
        double totalRating = 0;
        double count = 0;

        // Calculate the total rating and count for each company
        for (DoubleWritable value : values) {
            totalRating += value.get();
            count++;
        }

        // Save the total rating and count for each company in a HashMap
        Double[] ratingCount = new Double[]{totalRating, count};
        companyRatings.put(key.toString(), ratingCount);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // Calculate and output the average rating for each company
        for (String company : companyRatings.keySet()) {
            Double avgRating = (double) companyRatings.get(company)[0] / companyRatings.get(company)[1];
            context.write(new Text(company), new DoubleWritable(avgRating));
        }
    }
}