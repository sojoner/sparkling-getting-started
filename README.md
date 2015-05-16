# Getting started project for [Sparkling](https://gorillalabs.github.io/sparkling/)

This is a companion repo to the sparkling [guide] (https://gorillalabs.github.io/sparkling/articles/tfidf_guide.html). Please see this guide for further information.

### Submit the uber .jar

* build the .jar

      $ lein uberjar

* Copy the source onto the cluster
      
      $ scp target/sparkling-getting-started-1.0.0-SNAPSHOT-standalone.jar <USER>@<HOST>:/data

* Execute the code within the master container

      $ docker exec -it cobra/apachespark_master_1 /usr/spark/bin/spark-submit --master spark://master:7077 /data/sparkling-getting-started-1.0.0-SNAPSHOT-standalone.jar