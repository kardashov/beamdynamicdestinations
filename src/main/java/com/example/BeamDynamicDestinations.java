package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.*;

import javax.annotation.Nullable;

/**
 * <p>Run the example with
 * <pre>
 * mvn compile exec:java -Dexec.mainClass=org.example.BeamDynamicDestinations -Dexec.args="--runner=DirectRunner" -Pdirect-runner
 * </pre>
 */
public class BeamDynamicDestinations {
    public static void main(String[] args)  {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
        Pipeline p = Pipeline.create(options);

//        PCollection<String> stringPCollection =
        p.apply(Create.of("fd1_word1", "fd1_word2", "fd2_word3", "fd3_word3","fd3_word4"))

                .apply(FileIO.<String>write().to("dest_folder").via(TextIO.sink()).withCompression(Compression.UNCOMPRESSED));

        p.run().waitUntilFinish();
    }
}
