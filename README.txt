
This is an example using Spark Machine Learning decision trees , written in Scala, 
to demonstrate How to get started with Spark ML on a MapR sandbox 

Install and fire up the Sandbox using the instructions here: http://maprdocs.mapr.com/home/SandboxHadoop/c_sandbox_overview.html. 

Use an SSH client such as Putty (Windows) or Terminal (Mac) to login. See below for an example:
use userid: user01 and password: mapr.

For VMWare use:  $ ssh user01@ipaddress 

For Virtualbox use:  $ ssh user01@127.0.0.1 -p 2222 

You can build this project with Maven using IDEs like Eclipse or NetBeans, and then copy the JAR file to your MapR Sandbox, or you can install Maven on your sandbox and build from the Linux command line, 
for more information on maven, eclipse or netbeans use google search. 

After building the project on your laptop, you can use scp to copy your JAR file from the project target folder to the MapR Sandbox:

use userid: user01 and password: mapr.
For VMWare use:  $ scp  nameoffile.jar  user01@ipaddress:/user/user01/. 

For Virtualbox use:  $ scp -P 2222 nameoffile.jar  user01@127.0.0.1:/user/user01/.  

Copy the data files from the project data folder to the sandbox using scp to this directory /user/user01/data/ on the sandbox:
flights20170102.json	
flights20170304.json

from the project data folder: 
For VMWare use:  $ scp  *.csv user01@ipaddress:/user/user01/.
For Virtualbox use:  $ scp -P 2222 *.csv  user01@127.0.0.1:/user/user01/data/. 

You can run the code in the spark shell by copy pasting the code from the scala file in the spark shell after launching:
 
$spark-shell --master local[2]

Or you can run the applications with these steps:

______________________________________________________

Step 0: 

First compile the project: Select project  -> Run As -> Maven Install

Copy the jar and data to the sandbox 

from the target directory:
scp  *.jar user01@ipaddress:/user/user01/.
from the data directory:
scp  *.jsonuser01@ipaddress:/user/user01/.
____________________________________________________________________

Step 1:  Run the Spark program which will create and save the machine learning model: 

spark-submit --class ml.Flight --master local[2] spark-ml-flightdelay-1.0.jar

______________________________________________________



- Create the topics to read from and write to in MapR streams:

maprcli stream create -path /user/user01/stream -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /user/user01/stream -topic flights -partitions 3
maprcli stream topic create -path /user/user01/stream -topic flightp -partitions 3

to get info on the flights topic :
maprcli stream topic info -path /user/user01/stream -topic flights

to delete topics:
maprcli stream topic delete -path /user/user01/stream -topic flights  
maprcli stream topic delete -path /user/user01/stream -topic flightp 

____________________________________________________________________


Step 2: Publish flight data to the flights topic

Run the MapR Event Streams Java producer to produce messages with the topic and data file arguments:

java -cp spark-ml-flightdelay-1.0.jar:`mapr classpath` streams.MsgProducer /user/user01/stream:flights /user/user01/data/flights20170102.json

Step 3: Consume from flights topic, enrich with Model predictions, publish to flightp topic 

Run the Spark Consumer Producer (in separate consoles if you want to run at the same time) run the spark consumer with the topic to read from and write to:

spark-submit --class stream.SparkKafkaConsumerProducer --master local[2] spark-ml-flightdelay-1.0-jar-with-dependencies.jar  /user/user01/stream:flights /user/user01/stream:flightp

____________________________________________________________________

Step 4: Consume from the flightp topic

Run the Spark Consumer with the topic to read from

spark-submit --class stream.SparkKafkaConsumer --master local[2] spark-ml-flightdelay-1.0-jar-with-dependencies.jar /user/user01/stream:flightp

or Run the MapR Event Streams Java consumer with the topic to read from:

java -cp spark-ml-flightdelay-1.0.jar:`mapr classpath` streams.MsgConsumer /user/user01/stream:flightp 
