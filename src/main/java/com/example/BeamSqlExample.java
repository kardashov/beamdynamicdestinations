package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.*;

import javax.annotation.Nullable;

/**
 * This is a quick example, which uses Beam SQL DSL to create a data pipeline.
 *
 * <p>Run the example with
 * <pre>
 * mvn -pl sdks/java/extensions/sql \
 *   compile exec:java -Dexec.mainClass=org.apache.beam.sdk.extensions.sql.example.BeamSqlExample \
 *   -Dexec.args="--runner=DirectRunner" -Pdirect-runner
 *
 *   mvn compile exec:java -Dexec.mainClass=com.example.BeamSqlExample -Dexec.args="--runner=DirectRunner" -Pdirect-runner
 * </pre>
 */
class BeamSqlExample {
  public static void main(String[] args) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    //define the input row format
    RowType type = RowSqlType
        .builder()
        .withIntegerField("c1")
        .withVarcharField("c2")
        .withDoubleField("c3")
        .build();

    Row row1 = Row.withRowType(type).addValues(1, "row", 1.0).build();
    Row row2 = Row.withRowType(type).addValues(2, "row", 2.0).build();
    Row row3 = Row.withRowType(type).addValues(3, "row", 3.0).build();

    //create a source PCollection with Create.of();
    PCollection<Row> inputTable = PBegin.in(p).apply(Create.of(row1, row2, row3)
        .withCoder(type.getRowCoder()));

    //Case 1. run a simple SQL query over input PCollection with BeamSql.simpleQuery;
    PCollection<Row> outputStream = inputTable.apply(
        BeamSql.query("select c1, c2, c3 from PCOLLECTION where c1 > 1"));

    // print the output record of case 1;
    outputStream.apply(
        "log_result",
        MapElements.via(
            new SimpleFunction<Row, Void>() {
              public @Nullable
              Void apply(Row input) {
                // expect output:
                //  PCOLLECTION: [3, row, 3.0]
                //  PCOLLECTION: [2, row, 2.0]
                System.out.println("PCOLLECTION: " + input.getValues());
                return null;
              }
            }));

    // Case 2. run the query with BeamSql.query over result PCollection of case 1.
    PCollection<Row> outputStream2 =
        PCollectionTuple.of(new TupleTag<>("CASE1_RESULT"), outputStream)
            .apply(BeamSql.query("select c2, sum(c3) from CASE1_RESULT group by c2"));

    // print the output record of case 2;
    outputStream2.apply(
        "log_result",
        MapElements.via(
            new SimpleFunction<Row, Void>() {
              @Override
              public @Nullable
              Void apply(Row input) {
                // expect output:
                //  CASE1_RESULT: [row, 5.0]
                System.out.println("CASE1_RESULT: " + input.getValues());
                return null;
              }
            }));

    p.run().waitUntilFinish();
  }
}
