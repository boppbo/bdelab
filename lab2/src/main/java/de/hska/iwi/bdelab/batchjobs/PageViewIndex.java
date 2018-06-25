
package de.hska.iwi.bdelab.batchjobs;

import com.backtype.hadoop.pail.PailFormat;
import com.backtype.hadoop.pail.PailFormatFactory;
import com.backtype.hadoop.pail.PailSpec;
import de.hska.iwi.bdelab.batchstore.FileUtils;
import de.hska.iwi.bdelab.schema2.Data;
import manning.tap2.DataPailStructure;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

/*      18/06/25 04:09:14 INFO mapreduce.Job: Running job: job_1524027623060_0524
        18/06/25 04:09:18 INFO mapreduce.Job: Job job_1524027623060_0524 running in uber mode : false
        18/06/25 04:09:18 INFO mapreduce.Job:  map 0% reduce 0%
        18/06/25 04:09:25 INFO mapreduce.Job:  map 33% reduce 0%
        18/06/25 04:09:28 INFO mapreduce.Job:  map 67% reduce 0%
        18/06/25 04:09:29 INFO mapreduce.Job:  map 100% reduce 100%
        18/06/25 04:09:30 INFO mapreduce.Job: Job job_1524027623060_0524 completed successfully

        Job Counters
                Killed map tasks=1
                Launched map tasks=3
                Launched reduce tasks=3

                Total time spent by all map tasks (ms)=18925 => 6,3
                Total time spent by all reduce tasks (ms)=4836

                Map output bytes=41227228
                Bytes Written=890976 */
public class PageViewIndex {

    public static class Map extends MapReduceBase implements Mapper<Text, BytesWritable, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static Text pageView = new Text();

        private transient TDeserializer des;

        private TDeserializer getDeserializer() {
            if (des == null) des = new TDeserializer();
            return des;
        }
        
        // helper method for deserializing de.hska.iwi.bdelab.schema2.Data objects (aka facts)
        public Data deserialize(byte[] record) {
            Data ret = new Data();
            try {
                getDeserializer().deserialize((TBase) ret, record);
            } catch (TException e) {
                throw new RuntimeException(e);
            }
            return ret;
        }

        // THE MAP FUNCTION
        public void map(Text key, BytesWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {

            // This is how to deserialize a fact (de.hska.iwi.bdelab.schema2.Data object) from incoming pail data
            Data data = deserialize(value.getBytes());

            if (!data.get_dataunit().is_set_pageview())
                return;

            LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(
                    data.get_pedigree().get_true_as_of_secs(), 0, ZoneOffset.UTC);
            pageView.set(String.format(
                    "%s %d %s",
                    localDateTime.format(DateTimeFormatter.ISO_DATE),
                    localDateTime.getHour(),
                    data.get_dataunit().get_pageview().get_page().get_url()));

            // a static key results in a single partition on the reducer-side
            output.collect(pageView, one);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        
        // THE REDUCE FUNCTION
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(PageViewIndex.class);
        conf.setJobName("count pageViews");
        conf.setNumReduceTasks(conf.getNumReduceTasks() + 2);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        ////////////////////////////////////////////////////////////////////////////
        // input as pails
        PailSpec spec = PailFormatFactory.getDefaultCopy().setStructure(new DataPailStructure());
        PailFormat format = PailFormatFactory.create(spec);
        String masterPath = FileUtils.prepareMasterFactsPath(false,false);
        //
        conf.setInputFormat(format.getInputFormatClass());
        FileInputFormat.setInputPaths(conf, new Path(masterPath));
        ////////////////////////////////////////////////////////////////////////////

        ////////////////////////////////////////////////////////////////////////////
        // output as text
        conf.setOutputFormat(TextOutputFormat.class);
        FileSystem fs = FileUtils.getFs(false);
        FileOutputFormat.setOutputPath(conf, new Path(
                FileUtils.getTmpPath(fs, "pageview-count", true, false)));
        ////////////////////////////////////////////////////////////////////////////

        JobClient.runJob(conf);
    }
}
