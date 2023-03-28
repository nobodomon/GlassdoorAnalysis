import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReviewReducer extends Reducer<Text, MapWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Reducer<Text, MapWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        double totalPros = 0;
        double totalCons = 0;
        double totalSummary = 0;
        double count = 0;

        // Calculate the total rating and count for each company
        for (MapWritable value : values) {
            IntWritable pros = (IntWritable) value.get(new Text("pros"));
            IntWritable cons = (IntWritable) value.get(new Text("cons"));
            IntWritable summary = (IntWritable) value.get(new Text("summary"));

            totalPros += pros.get();
            totalCons += cons.get();
            totalSummary += summary.get();
            count++;
        }

        // Save the total rating and count for each company in a HashMap
        double avgProScore = totalPros / count;
        double avgConScore = totalCons / count;
        double avgSummaryScore = totalSummary / count;

        StringBuilder sb = new StringBuilder();

        sb.append("Average pros score: ").append(avgProScore).append(", ");
        sb.append("Average cons score: ").append(avgConScore).append(", ");
        sb.append("Average summary score: ").append(avgSummaryScore);

        context.write(key, new Text(sb.toString()));
    }
}
