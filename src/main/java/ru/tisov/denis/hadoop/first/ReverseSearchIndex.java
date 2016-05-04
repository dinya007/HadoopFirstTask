package ru.tisov.denis.hadoop.first;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.commons.lang.StringUtils.isEmpty;

public class ReverseSearchIndex {

    private static final String SPACE = "\\s+";
    private static final String NON_CHARACTER = "[^A-Za-z0-9 ]";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.newInstance(conf);

        fileSystem.deleteOnExit(new Path(args[1]));
        fileSystem.delete(new Path(args[2]), true);

        Job firstJob = Job.getInstance(conf, "First Job");
        firstJob.setJarByClass(ReverseSearchIndex.class);
        firstJob.setMapperClass(FirstMapper.class);
        firstJob.setReducerClass(FirstReducer.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(firstJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(firstJob, new Path(args[1]));

        boolean firstJobSuccessfullyFinished = firstJob.waitForCompletion(true);

        if (!firstJobSuccessfullyFinished) System.exit(1);

        Job secondJob = Job.getInstance(conf, "Second Job");
        secondJob.setJarByClass(ReverseSearchIndex.class);
        secondJob.setMapperClass(SecondMapper.class);
        secondJob.setReducerClass(SecondReducer.class);

//        secondJob.setInputFormatClass(KeyValueTextInputFormat.class);

        secondJob.setOutputKeyClass(CompoundKey.class);
        secondJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(secondJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(secondJob, new Path(args[2]));

        secondJob.waitForCompletion(true);

    }

    public static class FirstMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text id = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\n");

            for (int i = 0; i < lines.length; i++) {
                String[] words = lines[i].split(SPACE);
                id.set(words[0]);

                // Words for checking: Czechs Carmelite
                for (int j = 1; j < words.length; j++) {
                    String candidateWord = words[j].replaceAll(NON_CHARACTER, "").trim();
                    if (!isEmpty(candidateWord)) {
                        word.set(candidateWord);
                        context.write(word, id);
                    }
                }

            }
        }
    }

    public static class SecondMapper extends Mapper<Object, Text, CompoundKey, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] occurrences = value.toString().split(SPACE);

            String[] result = new String[occurrences.length - 1];
            System.arraycopy(occurrences, 1, result, 0, result.length);

            String values = StringUtils.join(" ", result);

            context.write(new CompoundKey(occurrences.length - 1, new Text(occurrences[0])), new Text(values));
        }

    }

    public static class FirstReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            Set<String> sortedSetFromIterable = getSortedSetFromIterable(values);

            for (String value : sortedSetFromIterable) {
                stringBuilder.append(" ").append(value);
            }

            context.write(key, new Text(stringBuilder.toString().trim()));
        }
    }

    public static class SecondReducer extends Reducer<CompoundKey, Text, Text, Text> {
        public void reduce(CompoundKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key.key, new Text(String.valueOf(values.iterator().next().toString().split(SPACE).length)));
        }
    }

    public static class CompoundKey implements WritableComparable<CompoundKey> {

        private Integer count = 0;
        private Text key;

        public CompoundKey() {

        }

        public CompoundKey(Integer count, Text key) {
            this.count = count;
            this.key = key;
        }

        @Override
        public int compareTo(CompoundKey o) {
            return ComparisonChain.start().compare(count, o.count)
                    .result();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(count.toString());
            out.writeUTF(key.toString());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count = Integer.valueOf(in.readUTF());
            key = new Text(in.readUTF());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompoundKey that = (CompoundKey) o;
            return Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            System.out.println("hashcode");
            return Objects.hash(key);
        }

        @Override
        public String toString() {
            return "CompoundKey{" +
                    "count=" + count +
                    ", key=" + key +
                    '}';
        }
    }

    public static Set<String> getSortedSetFromIterable(Iterable<Text> iterable) {
        Set<String> set = new TreeSet<String>();
        for (Text value : iterable) {
            set.add(value.toString());
        }
        return set;
    }
}
