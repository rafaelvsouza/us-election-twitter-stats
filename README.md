# us-election-twitter-stats

## Introduction
PySpark script to show some statistics about the 2020 Presidential US Election tweets. 

Statistics covered:
* Number of tweets by continent
* Number of tweets by country
* Number of tweets by state
* Number of tweets by city
* Most retweeted tweet
* Tweet with most likes
* Most active day and hour

The scripts was writter in Python with Pyspark libraries and uses a dataset from Kaggle. The dataset is split in two files, one for each candidate.

## Datasets

* Joe Biden Tweets   : https://www.kaggle.com/manchunhui/us-election-2020-tweets?select=hashtag_joebiden.csv
* Donald Trump Tweets: https://www.kaggle.com/manchunhui/us-election-2020-tweets?select=hashtag_donaldtrump.csv

## Used tools
* Hadoop 3.2.1
* Spark 3.0.1
* Python 3.6.9

## Environmental Variables
You need to define to follwing Environmental Variables:

 * JAVA_HOME - Home directory for the target java version. 
  `export JAVA_HOME = /usr/lib/jvm/java-8-openjdk-amd64`
 * HADOOP_HOME - Hadoop's installation home directory. 
  `export HADOOP_HOME = /home/hadoop/hadoop`
 * SPARK_HOME - Spark's installation home directory. 
  `export SPARK_HOME = $HOME/spark-3.0.1-bin-hadoop3.2`
 * HADOOP_CONF_DIR - Hadoop's configuration directory.
  `export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop`
 * PATH - System's standard environmental variable.
  `export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin`
 * PYTHONIOENCODING - Enconding code to be used by Python
  `export PYTHONIOENCODING=utf8`

## Hadoop configuration
Once you have set the environmental variables, you need to run the follow commands in a shell terminal:
* hadoopenv.sh
```
sed -i "\$aexport JAVA_HOME=$JAVA_HOME" $HADOOP_HOME/etc/hadoop/hadoopenv.sh
```
* Hadoop logs
```
if [ ! -d $HADOOP_HOME/logs ]; then mkdir $HADOOP_HOME/logs;fi
```
* Haddop core-site.xml
```
CORE_SITE=$HADOOP_HOME/etc/hadoop/core-site.xml
mv $CORE_SITE $CORE_SITE.old
cat >$CORE_SITE <<EOL
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>fs.defaultFS</name>
<value>hdfs://localhost:9000</value>
</property>
<property>
<name>hadoop.tmp.dir</name>
<value>/home/${user.name}/hadooptmp</value>
</property>
</configuration>
EOL
```
* Hadoop hdfs-site.xml
```
HDFS_SITE=$HADOOP_HOME/etc/hadoop/hdfs-site.xml
mv $HDFS_SITE $HDFS_SITE.old
cat >$HDFS_SITE <<EOL
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>dfs.replication</name>
<value>1</value>
</property>
</configuration>
EOL
```
* Hadoop namenode formatting
```
echo 'Y' | hdfs namenode -format
```

* Hadoop user work directory and tmp directory creation:
```
hdfs dfs -mkdir -p /user/$USER
hdfs dfs -chown $USER:$USER /user/$USER
hdfs dfs -mkdir /tmp
hdfs dfs -chmod 777 /tmp
```

Obs: In our case, the user name is `hadoop`.

## Starting the environment
You can start the Hadoop file system using the following commands:
```bash
hdfs --daemon start namenode
hdfs --daemon start datanode
```

## Uploading the dataset to Hadoop file system
To be able to run our solution, you need to upload the dataset to Hadoop file system. An example in how you can do it, is as follow:

```
hdfs dfs -mkdir -p tweet/donaldtrump
hdfs dfs -put hashtag_donaldtrump.csv tweet/donaldtrump
```

## Solution usage
In the file `uselection-tweet-stats.py`, at line 51, you need to inform the path of your's dataset in hdfs. In our case, the path is `/user/hadoop/tweets/donaldtrump`.
Once you have defined dataset's path, you can run the solution as follow:
```
$SPARK_HOME/bin/spark-submit uselection-tweet-stats.py
````

The code execution will print all the results to the terminal.

Enjoy. And leave your contribution... :)
