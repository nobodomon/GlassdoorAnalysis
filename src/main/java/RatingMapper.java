import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class RatingMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

    private DoubleWritable finalRatingWritable = new DoubleWritable();
    private DoubleWritable recommendationWritable = new DoubleWritable();
    private MapWritable mapWritable = new MapWritable();


    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Extract company name and review field
        nlpPipeline.init();

        String[] fields = line.split(",");

        double overallRating = Double.parseDouble(fields[4].trim());

        double workLifeRating = Double.parseDouble(fields[7].trim());
        double cultureRating = Double.parseDouble(fields[8].trim());
        double diversityRating = Double.parseDouble(fields[9].trim());
        double seniorLeadershipRating = Double.parseDouble(fields[10].trim());
        double careerOpportunitiesRating = Double.parseDouble(fields[12].trim());
        double compensationAndBenefitsRating = Double.parseDouble(fields[13].trim());

        double finalRating = 0;

        if(workLifeRating + cultureRating + diversityRating + seniorLeadershipRating + careerOpportunitiesRating + compensationAndBenefitsRating == 0){
            finalRating = overallRating;
        }else{
            double workLifeWeightedRating = workLifeRating * 1.3;
            double cultureWeightedRating = cultureRating * 0.5;
            double diversityWeightedRating = diversityRating * 1.5;
            double seniorLeadershipWeightedRating = seniorLeadershipRating * 0.2;
            double careerOpportunitiesWeightedRating = careerOpportunitiesRating * 1.4;
            double compensationAndBenefitsWeightedRating = compensationAndBenefitsRating * 1.1;

            finalRating = (
                    workLifeWeightedRating + cultureWeightedRating + diversityWeightedRating + seniorLeadershipWeightedRating + careerOpportunitiesWeightedRating + compensationAndBenefitsWeightedRating
                    );
        }
        finalRatingWritable.set(finalRating);
        mapWritable.put(new Text("FinalRating"), finalRatingWritable);

        String recommendation = fields[11].trim();
        double finalRecommendation = 0;
        if(recommendation.isEmpty()){
            finalRecommendation = 0;
        }
        if(recommendation.equals("POSITIVE")) {
            finalRecommendation = 1;
        }
        if(recommendation.equals("NEGATIVE")){
            finalRecommendation = -1;
        }

        recommendationWritable.set(finalRecommendation);
        mapWritable.put(new Text("Recommendation"), recommendationWritable);

        context.write(new Text("Ratings"), mapWritable);
    }
}
