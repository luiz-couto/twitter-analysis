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

public class TwitterMapReduce {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        // Field @one is the value in all outputs.
        private final static IntWritable one = new IntWritable(1);
        // Field @hashtag stores the key in each output. It contains a hashtag
        // found in a twitter text and is initialized by converting String into
        // Text.
        private Text hashtag = new Text();
        private Integer counter = 0;

        /* The map() function breaks down the line of text into words using
           Java's StringTokenizer class, and then outputs a pair (word, one)
           for each work in the line. */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tuple = line.split("\\n");
            ArrayList<String> hashtagsList = new ArrayList<String>();

            try{
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

                    hashtag.set(tag);
                    System.out.println(tagList);

                    //context.write(hashtag, one);
                }

            }catch(Exception e){
                e.printStackTrace();
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
        //job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}