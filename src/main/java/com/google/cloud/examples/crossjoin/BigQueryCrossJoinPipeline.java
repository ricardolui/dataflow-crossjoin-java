/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.examples.crossjoin;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;


/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line:
 * <p>
 * mvn compile exec:java -Dexec.mainClass=com.google.cloud.examples.crossjoin.BigQueryCrossJoinPipelineWithHashedMapCombiner
 * -Dexec.args="--runner=DataflowRunner --project=<YOUR_PROJECT_ID> --tempLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --numWorkers=100 --maxNumWorkers=800 --workerMachineType=e2-highmem-4 --region=us-central1 --diskSizeGb=30
 * --contextTableRef=<PROJECT>:<DATASET><TABLE> --domainTableRef=<PROJECT>:<DATASET><TABLE> --destinationTableRef=<PROJECT>:<DATASET><TABLE>"
 */
public class BigQueryCrossJoinPipeline {


    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCrossJoinPipeline.class);

    /**
     * BigQueryCrossJoinPipelineOptions
     */
    public interface BigQueryCrossJoinPipelineOptions extends PipelineOptions {
        @Description("BigQuery table to read from in the form <project>:<dataset>.<table>")
        @Validation.Required
        String getContextTableRef();

        void setContextTableRef(String contextTableRef);

        @Description("BigQuery table to read from in the form <project>:<dataset>.<table>")
        @Validation.Required
        String getDomainTableRef();

        void setDomainTableRef(String domainTableRef);

        @Description("BigQuery table to export from in the form <project>:<dataset>.<table>")
        @Validation.Required
        String getDestinationTableRef();

        void setDestinationTableRef(String destinationTableRef);


    }


    public static void main(String[] args) {

        BigQueryCrossJoinPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryCrossJoinPipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        // Create a side input that updates each second.
        PCollectionView<Map<Integer, Iterable<TableRow>>> contextView = p.apply("Read Context Table",
                BigQueryIO.readTableRows()
                        .from(options.getContextTableRef())
                        .withMethod(BigQueryIO.TypedRead.Method.EXPORT))
                .apply("Map Key", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptor.of(TableRow.class))).
                        via((SerializableFunction<TableRow, KV<Integer, TableRow>>) input -> KV.of(Integer.parseInt((String) input.get("collector__id")), input)))
                .apply("GroupByKey", GroupByKey.create())
                .apply(View.asMap());


        PCollection<TableRow> domain =
                p.apply("Read from BigQuery table",
                        BigQueryIO.readTableRows()
                                .from(options.getDomainTableRef())
                                .withMethod(BigQueryIO.TypedRead.Method.EXPORT))
                        .apply(ParDo.of(new DoFn<TableRow, TableRow>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                TableRow domain = c.element();
                                Map<Integer, Iterable<TableRow>> hashContext = c.sideInput(contextView);

                                Integer collectorId = Integer.parseInt((String) domain.get("collector__id"));

                                Iterable<TableRow> contextByCollectorId = hashContext.get(collectorId);
                                if (contextByCollectorId != null) {

                                    //Test done to persist only stats and not all the rows
//                                    int totalCount = Iterators.size(contextByCollectorId.iterator());
//                                    TableRow stat = new TableRow();
//                                    stat.set("cnt", totalCount);
//                                    stat.set("collector__id", collectorId);
//                                    c.output(stat);

                                    for (TableRow context : contextByCollectorId) {

                                        SimpleDateFormat dateFormatLocal = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                                         SimpleDateFormat dateFormatLocal = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");

                                        try {
                                            Date emissionTs = dateFormatLocal.parse(domain.get("emission_ts").toString());
                                            Date timestampLow = dateFormatLocal.parse(context.get("__timestamp_low").toString());
                                            Date timestampHigh = dateFormatLocal.parse(context.get("__timestamp_high").toString());

                                            if (emissionTs.compareTo(timestampLow) >= 0) {
                                                if (emissionTs.compareTo(timestampHigh) <= 0) {
                                                    // context.set("domain_id", domain.get("id"));
                                                    // context.set("test", System.currentTimeMillis()/1000);
                                                    domain.set("__timestamp_low", context.get("__timestamp_low"));
                                                    domain.set("__timestamp_high", context.get("__timestamp_high"));
                                                    c.output(domain);
                                                }
                                            }
                                        } catch (ParseException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                } else {
                                    LOG.debug("null contextByCollectorId");
                                }
                            }
                        }).withSideInput("context", contextView));

        domain.apply("Write to Merged", BigQueryIO.writeTableRows().to(options.getDestinationTableRef()).withMethod(BigQueryIO.Write.Method.FILE_LOADS).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run();
    }
}