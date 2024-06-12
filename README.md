Setup.
 -Write a docker compose file for FlinkSQL, postgres, kafka, zookeeper, connect , and schema registry. 
 -Setup a datagen source connector with the schema defined in purchase-schema.avsc.
       
-purchase-schema.avsc 
        
```bash
 {
 "namespace": "confluent",
 "name": "purchase",
 "type": "record",
 "compatibility": "NONE",
 "fields": [
 {"name": "id", "type": {
 "type": "long",
 "arg.properties": {
 "iteration": {
 "start": 0
 }
 }
 }},
 {"name": "product_id", "type": {
 "type": "long",
 "arg.properties": {
 "range":{
 "min":1,
 "max":100
 
           
           }
           }
                }},
                {"name":"quantity",
                  "type":{
                     "type":"long",
                     "arg.properties":{
                        "range":{
                           "min":1,
                           "max":10
                        }
                     }
                 }},
                 {"name": "customer_id", "type": {
                  "type": "string",
                     "arg.properties": {
                       "regex": "[0-9]*",
                       "length": {
                         "min":1,
                         "max":100
                       }
                   }
              }},
              {"name": "discount", "type": {
                "type": "double",
                 "arg.properties": {
                    "range":{
                       "min":0,
                       "max":50
                    }
                 }
              }}
        ]
}
```
