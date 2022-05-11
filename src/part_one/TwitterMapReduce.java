package part_one;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.*;

import part_one.TwitterMapReduce.TokenizerMapper.TweetReducer;

public class TwitterMapReduce {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        // Field @list will store correlated hashtags for each hashtag on the tweet.
        private static Text list = new Text();
        // Field @hashtag stores the key in each output. It contains a hashtag
        // found in a twitter text and is initialized by converting String into
        // Text.
        private Text hashtag = new Text();

        /* The map() function breaks down the line of text into words using
           Java's StringTokenizer class, and then outputs a pair (word, list)
           for each hashtag in the text field of the line. */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tuple = line.split("\\n");
            ArrayList<String> hashtagsList = new ArrayList<String>();

            try {
                for(int i=0;i<tuple.length; i++){
                    JSONObject obj = new JSONObject(tuple[i]);

                    String text = obj.getString("text");
                    for (String word : text.split(" ")) {
                        if (word.length() > 0 && word.charAt(0) == '#') {
                            hashtagsList.add(word);
                            //System.out.println(counter);
                        }
                    }
                }

                for (String tag : hashtagsList) {
                    ArrayList<String> tagList = new ArrayList<String>(hashtagsList);
                    while (tagList.contains(tag)) {
                        tagList.remove(tag);
                    }

                    String finalString = "";
                    for (String notMe : tagList) {
                        finalString = finalString + notMe + "\n";
                    }

                    //System.out.println(tagList);
                    hashtag.set(tag);
                    list.set(finalString);
                    context.write(hashtag, list);
                }

            } catch(Exception e) {
                e.printStackTrace();
            }

        }

        public static class TweetReducer extends Reducer<Text, Text, Text, IntWritable> {

        // Field @result stores how many times a word has appeared.
        private IntWritable result = new IntWritable();

        /* The reduce() function receives a key and an iterable over all
           values for that key. We add up the values of all counters and
           output a pair (word, count). */
        public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum += 1;
            }
            result.set(sum);
            ctx.write(key, result);
        }
        
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        /* We generate a job; specify the Mapper, Combiner, and Reducer
           classes; specify the output key and value types. The
           directories where the inputs are read from and outputs
           written to are received from the command line. */
        Job job = Job.getInstance(conf, "twitter-map-reduce");
        job.setJarByClass(TwitterMapReduce.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(TweetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}