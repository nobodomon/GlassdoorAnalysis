import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class RatingReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {
    private DoubleWritable finalRatingWritable = new DoubleWritable();
    private DoubleWritable willRecommendWritable = new DoubleWritable();
    private DoubleWritable willRecommendNonEmptyWritable = new DoubleWritable();
    private DoubleWritable wontRecommendWritable = new DoubleWritable();
    private DoubleWritable wontRecommendNonEmptyWritable = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Reducer<Text, MapWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double totalRating = 0;
        double willRecommend = 0;
        double wontRecommend = 0;
        int count = 0;

        int nonEmptyRecomendationCount = 0;

        // Calculate the total rating and count for each company
        for (MapWritable value : values) {
            DoubleWritable currRating = (DoubleWritable) value.get(new Text("FinalRating"));
            DoubleWritable recommendation = (DoubleWritable) value.get(new Text("Recommendation"));

            totalRating += currRating.get();

            double recommendationValue = recommendation.get();

            if(recommendationValue != 0){
                nonEmptyRecomendationCount++;
            }
            count++;

            if(recommendationValue == 1){
                willRecommend++;
            }

            if(recommendationValue == -1){
                wontRecommend++;
            }
        }

        double willRecommendPercentage = (willRecommend / count) * 100;
        double willRecommendPercentageWithoutEmpty = (willRecommend / nonEmptyRecomendationCount) * 100;

        double wontRecommendPercentage = (wontRecommend / count) * 100;
        double wontRecommendPercentageWithoutEmpty = (wontRecommend / nonEmptyRecomendationCount) * 100;

        double maxRating = count * 30;
        double averageRatings = (totalRating / maxRating) * 100;

        willRecommendWritable.set(willRecommendPercentage);
        willRecommendNonEmptyWritable.set(willRecommendPercentageWithoutEmpty);
        wontRecommendWritable.set(wontRecommendPercentage);
        wontRecommendNonEmptyWritable.set(wontRecommendPercentageWithoutEmpty);
        finalRatingWritable.set(averageRatings);

        context.write(new Text("WillRecommend"), willRecommendWritable);
        context.write(new Text("WillRecommendNonEmpty"), willRecommendNonEmptyWritable);
        context.write(new Text("WontRecommend"), wontRecommendWritable);
        context.write(new Text("WontRecommendNonEmpty"), wontRecommendNonEmptyWritable);
        context.write(new Text("AverageRating"), finalRatingWritable);

    }
}