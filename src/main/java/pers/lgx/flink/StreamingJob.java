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

package pers.lgx.flink;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.lgx.flink.bean.OrderEntity;
import pers.lgx.flink.bean.ProductEntity;
import pers.lgx.flink.bean.RetEntity;
import pers.lgx.flink.schema.RowSchema;

import java.util.Optional;
import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingJob.class);

    private static Properties properties = new Properties();
    static {
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
    }
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Row> product = env.addSource(
                new FlinkKafkaConsumer<Row>("product-topic", new RowSchema(new ProductEntity()), properties));
        DataStream<Row> order = env.addSource(
                new FlinkKafkaConsumer<Row>("order-topic", new RowSchema(new OrderEntity()), properties));



        Table productTab = tEnv.fromDataStream(product, "product_id, product_name, created_at, ptime.proctime");
        Table orderTab = tEnv.fromDataStream(order, "order_id, product_id, created_at, ptime.proctime");

        tEnv.registerTable("Product", productTab);
        tEnv.registerTable("Orders", orderTab);

        useTemporalTableFunction(tEnv);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    public static void useTemporalTableFunction(final StreamTableEnvironment tEnv) {
        TemporalTableFunction tempFunc = tEnv.scan("Product")
                .createTemporalTableFunction("ptime", "product_id");
        tEnv.registerFunction("ProductInfo", tempFunc);

        Table ret = tEnv.sqlQuery("SELECT o.order_id, o.product_id, p.product_name FROM Orders AS o " +
                ", LATERAL TABLE (ProductInfo(o.ptime)) AS p WHERE o.product_id = p.product_id");


        KafkaTableSink sink = new KafkaTableSink(TableSchema.builder()
                .fields(new String[]{"order_id", "product_id", "product_name"},
                        new DataType[]{DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()}).build(),
                "sink-topic", properties, Optional.empty(), new RowSchema(new RetEntity())
        );

        tEnv.registerTableSink("kafkaSink", sink);
        ret.insertInto("kafkaSink");
    }
}
