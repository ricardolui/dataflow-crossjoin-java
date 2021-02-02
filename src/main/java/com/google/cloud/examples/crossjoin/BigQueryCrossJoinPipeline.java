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
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.List;


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
 * Platform, you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=DataflowRunner
 */
public class BigQueryCrossJoinPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCrossJoinPipeline.class);

    public static void main(String[] args) {
        Pipeline p = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create());


        // Create a side input that updates each second.
        PCollectionView<List<TableRow>> contextView =
                p.apply("Read Context Table",
                        BigQueryIO.readTableRows()
                                .from("datalake.context")
                                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ))
                        .apply(View.asList());


        PCollection<TableRow> domain =
                p.apply("Read from BigQuery table",
                        BigQueryIO.readTableRows()
                                .from("datalake.domain")
                                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ))
                        .apply(ParDo.of(new DoFn<TableRow, TableRow>() {

                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                TableRow domain = c.element();
                                List<TableRow> arrayContext = c.sideInput(contextView);
                                for (TableRow context : arrayContext) {
                                    c.output(context);

//                                    if (domain.get("collector__id").equals(context.get("collector__id"))) {
//
//                                        SimpleDateFormat dateFormatLocal = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
//                                        LOG.debug(domain.get("emission_ts").getClass().toString());
//                                        try {
//                                            Date emissionTs = dateFormatLocal.parse(domain.get("emission_ts").toString());
//                                            Date timestampLow = dateFormatLocal.parse(context.get("__timestamp_low").toString());
//                                            Date timestampHigh = dateFormatLocal.parse(context.get("__timestamp_high").toString());
//
//                                            if (emissionTs.compareTo(timestampLow) >= 0) {
//                                                if (emissionTs.compareTo(timestampHigh) <= 0) {
//                                                    context.set("domain_id", domain.get("id"));
//                                                    context.set("test", System.currentTimeMillis()/1000);
//                                                    c.output(context);
//                                                }
//                                            }
//                                        } catch (ParseException e) {
//                                            e.printStackTrace();
//                                        }
//
//                                    }
                                }
                            }
                        }).withSideInput("context", contextView));
        domain.apply("Write to Merged", BigQueryIO.writeTableRows().to("datalake.crossjoin_dataflow").withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run();
    }
}
