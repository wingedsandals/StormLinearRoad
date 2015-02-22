# StormVoter

How to build:
    run mvn clean install -DskipTests
    
How to run:
    storm jar target/StormVoter-1.0-SNAPSHOT-jar-with-dependencies.jar LinearRoadTridentTopology \[arg1\] \[arg2\] \[arge3\]
    
    arg1 (Optional) : position file     (Default: votes-o-40.txt)
    arg2 (Optional) : timestamp file    (Default: votes-o-40.txt)
    arg3 (Optional) : memcached port    (Default: 11211)
