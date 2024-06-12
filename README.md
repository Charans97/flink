1.Configure a datagen source connector in kafka connect.

2.Create the product table with JDBC source connector to fetch the data of product table which is present in postgres container. 
```bash
           CREATE TABLE product (
           `id` INT,
           `created` TIMESTAMP(3), 
           `modified` TIMESTAMP(3),
           `name` VARCHAR,
           `category` VARCHAR, 
           `price` DOUBLE PRECISION, 
           WATERMARK FOR `created` AS `created`
           ) WITH (
           'connector' = 'jdbc',
           'url' = 'jdbc:postgresql://postgres:5432/plf_training',
           'table-name' = 'product',
           'username' = 'platformatory',
           'password' = 'plf_password'
           );
```

3.Create the purchase table with KAFKA source connector to fetch the data from the purchase topic. 
```bash
           CREATE TABLE purchase (
           `id` BIGINT,
           `product_id` BIGINT,
           `quantity` BIGINT,
           `customer_id` STRING,
           `discount` DOUBLE,
           `created_at` TIMESTAMP(3) METADATA FROM ‘timestamp’,
           WATERMARK FOR `created_at` AS `created_at`
           ) WITH (
            'connector' = 'kafka',
           'topic' = 'purchase',
           'scan.startup.mode' = 'earliest-offset',
           'properties.bootstrap.servers' = 'kafka-1:9092',
           'properties.security.protocol' = 'SASL_PLAINTEXT',
           'properties.sasl.mechanism' = 'PLAIN',
           'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";',
           'format' = 'avro-confluent',
           'avro-confluent.schema-registry.url' = 'http://schema-registry:8085',
           'avro-confluent.schema-registry.subject' = 'purchase-value');
```

4.Create the result table 
```bash
           CREATE TABLE sales ( 
           `window_start` TIMESTAMP(3), 
           `window_end` TIMESTAMP(3),
           `product_id` BIGINT, 
           `category` STRING, 
           `price` DOUBLE,
           `total` DOUBLE,
           PRIMARY KEY(product_id) NOT ENFORCED
           ) WITH ( 
           'connector' = 'upsert-kafka', 
           'topic' = 'sales', 
           'properties.bootstrap.servers' = 'kafka-1:9092', 
           'properties.group.id' = 'consumer-sales-group',
           'properties.security.protocol' = 'SASL_PLAINTEXT',
           'properties.sasl.mechanism' = 'PLAIN',
           'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";',
           'key.format' = 'avro-confluen           NOTE: 
            1. Use the HOP window while inserting the data into sales table, as upser-kafka or changlog table doesn’t support window aggregation, but they do support TOP – N. 
            2. You might see data duplicacy in sales table. So better understand what going on in sales table and how the data is getting stored, then change the result mode into changelog. t',
           'properties.allow.auto.create.topics' = 'false',
           'key.avro-confluent.schema-registry.url' = 'http://schema-registry:8085',
           'key.avro-confluent.schema-registry.subject' = 'top-sales-value',
           'value.format' = 'avro-confluent',
           'value.avro-confluent.schema-registry.url' = 'http://schema-registry:8085',
           'value.avro-confluent.schema-registry.subject' = 'top-sales-value',
           'properties.auto.offset.reset' = 'earliest');
```

5.Insert the data into sales table with window_start, window_end.
            NOTE: 
            1. Use the HOP window while inserting the data into sales table, as upser-kafka or changlog table doesn’t support window aggregation, but they do support TOP – N. 
            2. You might see data duplicacy in sales table. So better understand what going on in sales table and how the data is getting stored, then change the result mode into changelog. 
```bash
set 'sql-client.execution.result-mode' = 'changelog';
```

Insert the data into the table: 
```bash
           INSERT INTO sales(window_start, window_end, product_id, category, price, total) 
           SELECT window_start, window_end, pr.product_id, p.category, p.price, 
           SUM((p.price * pr.quantity - (1 - COALESCE(pr.discount,0)/100))) AS total
           FROM TABLE(HOP(TABLE purchase, DESCRIPTOR(ts), INTERVAL '5' MINUTE, INTERVAL '1' HOUR)) AS pw 
           JOIN purchase pr ON pr.product_id = pw.id 
           JOIN product p ON p.id = pw.id
           GROUP BY window_start, window_end, pr.product_id, p.category, p.price;
```

Check the data in table: 
```bash
select * from sales;
```

6.Perform the query to find the top performing product. 
```bash
           SELECT * FROM ( 
           SELECT * , ROW_NUMBER() OVER (ORDER BY window_start ASC) AS rownum 
           FROM( 
           select * from sales group by window_start, window_end, product_id, category, price, total ORDER BY total DESC) 
           ) where rownum = 2;
```


