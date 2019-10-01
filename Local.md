

```python
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T

import pandas as pd
pd.set_option('display.max_columns', 500)
```


```python
config = configparser.ConfigParser()

#Normally this file should be in ~/.aws/credentials
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']
```


```python
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
```


```python
# create the spark session
spark = create_spark_session()
```


```python
# read data from my S3 bucket. This is the same data in workspace
songPath = 's3a://skuchkula/song_data/*/*/*/*.json'
logPath = 's3a://skuchkula/log_data/*.json'
```


```python
# define output paths
output = 's3a://skuchkula/schema/'
```

## process song data

### create song_table


```python
# Step 1: Read in the song data
df_song = spark.read.json(songPath)
```


```python
# check the schema
df_song.printSchema()
```

    root
     |-- artist_id: string (nullable = true)
     |-- artist_latitude: double (nullable = true)
     |-- artist_location: string (nullable = true)
     |-- artist_longitude: double (nullable = true)
     |-- artist_name: string (nullable = true)
     |-- duration: double (nullable = true)
     |-- num_songs: long (nullable = true)
     |-- song_id: string (nullable = true)
     |-- title: string (nullable = true)
     |-- year: long (nullable = true)
    



```python
# Step 2: extract columns to create songs table
song_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
```


```python
# groupby song_id and select the first record's title in the group.
t1 = df_song.select(F.col('song_id'), 'title') \
    .groupBy('song_id') \
    .agg({'title': 'first'}) \
    .withColumnRenamed('first(title)', 'title1')

t2 = df_song.select(song_cols)
```


```python
t1.toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>song_id</th>
      <th>title1</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>SOGOSOV12AF72A285E</td>
      <td>¿Dónde va Chichi?</td>
    </tr>
    <tr>
      <th>1</th>
      <td>SOMZWCG12A8C13C480</td>
      <td>I Didn't Mean To</td>
    </tr>
    <tr>
      <th>2</th>
      <td>SOUPIRU12A6D4FA1E1</td>
      <td>Der Kleine Dompfaff</td>
    </tr>
    <tr>
      <th>3</th>
      <td>SOXVLOJ12AB0189215</td>
      <td>Amor De Cabaret</td>
    </tr>
    <tr>
      <th>4</th>
      <td>SOWTBJW12AC468AC6E</td>
      <td>Broken-Down Merry-Go-Round</td>
    </tr>
  </tbody>
</table>
</div>




```python
t2.toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>song_id</th>
      <th>title</th>
      <th>artist_id</th>
      <th>year</th>
      <th>duration</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>SOBAYLL12A8C138AF9</td>
      <td>Sono andati? Fingevo di dormire</td>
      <td>ARDR4AC1187FB371A1</td>
      <td>0</td>
      <td>511.16363</td>
    </tr>
    <tr>
      <th>1</th>
      <td>SOOLYAZ12A6701F4A6</td>
      <td>Laws Patrolling (Album Version)</td>
      <td>AREBBGV1187FB523D2</td>
      <td>0</td>
      <td>173.66159</td>
    </tr>
    <tr>
      <th>2</th>
      <td>SOBBUGU12A8C13E95D</td>
      <td>Setting Fire to Sleeping Giants</td>
      <td>ARMAC4T1187FB3FA4C</td>
      <td>2004</td>
      <td>207.77751</td>
    </tr>
    <tr>
      <th>3</th>
      <td>SOAOIBZ12AB01815BE</td>
      <td>I Hold Your Hand In Mine [Live At Royal Albert...</td>
      <td>ARPBNLO1187FB3D52F</td>
      <td>2000</td>
      <td>43.36281</td>
    </tr>
    <tr>
      <th>4</th>
      <td>SONYPOM12A8C13B2D7</td>
      <td>I Think My Wife Is Running Around On Me (Taco ...</td>
      <td>ARDNS031187B9924F0</td>
      <td>2005</td>
      <td>186.48771</td>
    </tr>
  </tbody>
</table>
</div>




```python
song_table_df = t1.join(t2, 'song_id') \
                .where(F.col("title1") == F.col("title")) \
                .select(song_cols)
```


```python
song_table_df.toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>song_id</th>
      <th>title</th>
      <th>artist_id</th>
      <th>year</th>
      <th>duration</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>SOBAYLL12A8C138AF9</td>
      <td>Sono andati? Fingevo di dormire</td>
      <td>ARDR4AC1187FB371A1</td>
      <td>0</td>
      <td>511.16363</td>
    </tr>
    <tr>
      <th>1</th>
      <td>SOOLYAZ12A6701F4A6</td>
      <td>Laws Patrolling (Album Version)</td>
      <td>AREBBGV1187FB523D2</td>
      <td>0</td>
      <td>173.66159</td>
    </tr>
    <tr>
      <th>2</th>
      <td>SOBBUGU12A8C13E95D</td>
      <td>Setting Fire to Sleeping Giants</td>
      <td>ARMAC4T1187FB3FA4C</td>
      <td>2004</td>
      <td>207.77751</td>
    </tr>
    <tr>
      <th>3</th>
      <td>SOAOIBZ12AB01815BE</td>
      <td>I Hold Your Hand In Mine [Live At Royal Albert...</td>
      <td>ARPBNLO1187FB3D52F</td>
      <td>2000</td>
      <td>43.36281</td>
    </tr>
    <tr>
      <th>4</th>
      <td>SONYPOM12A8C13B2D7</td>
      <td>I Think My Wife Is Running Around On Me (Taco ...</td>
      <td>ARDNS031187B9924F0</td>
      <td>2005</td>
      <td>186.48771</td>
    </tr>
  </tbody>
</table>
</div>




```python
song_table_df.toPandas().shape
```




    (71, 5)




```python
df_song.toPandas().shape
```




    (71, 10)




```python
# Step 3: Write this to a parquet file
song_table_df.write.parquet('data/songs_table', partitionBy=['year', 'artist_id'], mode='Overwrite')
```


```python
# write this to s3 bucket
song_table_df.write.parquet(output + 'songs_table', partitionBy=['year', 'artist_id'], mode='Overwrite')
```

### create artists_table


```python
# define the cols
artists_cols = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
```


```python
df_song.printSchema()
```

    root
     |-- artist_id: string (nullable = true)
     |-- artist_latitude: double (nullable = true)
     |-- artist_location: string (nullable = true)
     |-- artist_longitude: double (nullable = true)
     |-- artist_name: string (nullable = true)
     |-- duration: double (nullable = true)
     |-- num_songs: long (nullable = true)
     |-- song_id: string (nullable = true)
     |-- title: string (nullable = true)
     |-- year: long (nullable = true)
    



```python
# groupby song_id and select the first record's title in the group.
t1 = df_song.select(F.col('artist_id'), 'artist_name') \
    .groupBy('artist_id') \
    .agg({'artist_name': 'first'}) \
    .withColumnRenamed('first(artist_name)', 'artist_name1')

t2 = df_song.select(artists_cols)
```


```python
t1.toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist_id</th>
      <th>artist_name1</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AR9AWNF1187B9AB0B4</td>
      <td>Kenny G featuring Daryl Hall</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AR0IAWL1187B9A96D0</td>
      <td>Danilo Perez</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AR0RCMP1187FB3F427</td>
      <td>Billie Jo Spears</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AREDL271187FB40F44</td>
      <td>Soul Mekanik</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ARI3BMM1187FB4255E</td>
      <td>Alice Stuart</td>
    </tr>
  </tbody>
</table>
</div>




```python
t2.toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist_id</th>
      <th>artist_name</th>
      <th>artist_location</th>
      <th>artist_latitude</th>
      <th>artist_longitude</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ARDR4AC1187FB371A1</td>
      <td>Montserrat Caballé;Placido Domingo;Vicente Sar...</td>
      <td></td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AREBBGV1187FB523D2</td>
      <td>Mike Jones (Featuring CJ_ Mello &amp; Lil' Bran)</td>
      <td>Houston, TX</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ARMAC4T1187FB3FA4C</td>
      <td>The Dillinger Escape Plan</td>
      <td>Morris Plains, NJ</td>
      <td>40.82624</td>
      <td>-74.47995</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ARPBNLO1187FB3D52F</td>
      <td>Tiny Tim</td>
      <td>New York, NY</td>
      <td>40.71455</td>
      <td>-74.00712</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ARDNS031187B9924F0</td>
      <td>Tim Wilson</td>
      <td>Georgia</td>
      <td>32.67828</td>
      <td>-83.22295</td>
    </tr>
  </tbody>
</table>
</div>




```python
artists_table_df = t1.join(t2, 'artist_id') \
                .where(F.col("artist_name1") == F.col("artist_name")) \
                .select(artists_cols)
```


```python
artists_table_df.toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist_id</th>
      <th>artist_name</th>
      <th>artist_location</th>
      <th>artist_latitude</th>
      <th>artist_longitude</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ARDR4AC1187FB371A1</td>
      <td>Montserrat Caballé;Placido Domingo;Vicente Sar...</td>
      <td></td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AREBBGV1187FB523D2</td>
      <td>Mike Jones (Featuring CJ_ Mello &amp; Lil' Bran)</td>
      <td>Houston, TX</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ARMAC4T1187FB3FA4C</td>
      <td>The Dillinger Escape Plan</td>
      <td>Morris Plains, NJ</td>
      <td>40.82624</td>
      <td>-74.47995</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ARPBNLO1187FB3D52F</td>
      <td>Tiny Tim</td>
      <td>New York, NY</td>
      <td>40.71455</td>
      <td>-74.00712</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ARDNS031187B9924F0</td>
      <td>Tim Wilson</td>
      <td>Georgia</td>
      <td>32.67828</td>
      <td>-83.22295</td>
    </tr>
  </tbody>
</table>
</div>




```python
# write this to s3 bucket
artists_table_df.write.parquet(output + 'artists_table', mode='Overwrite')
```


```python
artists_table_df.write.parquet('data/artists_table', mode='Overwrite')
```


```python
# read the partitioned data
df_artists_read = spark.read.option("mergeSchema", "true").parquet("data/artists_table")
```

## Process log data


```python
# Step 1: Read in the log data
df_log = spark.read.json(logPath)
```


```python
df_log.printSchema()
```

    root
     |-- artist: string (nullable = true)
     |-- auth: string (nullable = true)
     |-- firstName: string (nullable = true)
     |-- gender: string (nullable = true)
     |-- itemInSession: long (nullable = true)
     |-- lastName: string (nullable = true)
     |-- length: double (nullable = true)
     |-- level: string (nullable = true)
     |-- location: string (nullable = true)
     |-- method: string (nullable = true)
     |-- page: string (nullable = true)
     |-- registration: double (nullable = true)
     |-- sessionId: long (nullable = true)
     |-- song: string (nullable = true)
     |-- status: long (nullable = true)
     |-- ts: long (nullable = true)
     |-- userAgent: string (nullable = true)
     |-- userId: string (nullable = true)
    



```python
df_log.filter(F.col("page") == "NextSong").toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>auth</th>
      <th>firstName</th>
      <th>gender</th>
      <th>itemInSession</th>
      <th>lastName</th>
      <th>length</th>
      <th>level</th>
      <th>location</th>
      <th>method</th>
      <th>page</th>
      <th>registration</th>
      <th>sessionId</th>
      <th>song</th>
      <th>status</th>
      <th>ts</th>
      <th>userAgent</th>
      <th>userId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Harmonia</td>
      <td>Logged In</td>
      <td>Ryan</td>
      <td>M</td>
      <td>0</td>
      <td>Smith</td>
      <td>655.77751</td>
      <td>free</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.541017e+12</td>
      <td>583</td>
      <td>Sehr kosmisch</td>
      <td>200</td>
      <td>1542241826796</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>26</td>
    </tr>
    <tr>
      <th>1</th>
      <td>The Prodigy</td>
      <td>Logged In</td>
      <td>Ryan</td>
      <td>M</td>
      <td>1</td>
      <td>Smith</td>
      <td>260.07465</td>
      <td>free</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.541017e+12</td>
      <td>583</td>
      <td>The Big Gundown</td>
      <td>200</td>
      <td>1542242481796</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>26</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Train</td>
      <td>Logged In</td>
      <td>Ryan</td>
      <td>M</td>
      <td>2</td>
      <td>Smith</td>
      <td>205.45261</td>
      <td>free</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.541017e+12</td>
      <td>583</td>
      <td>Marry Me</td>
      <td>200</td>
      <td>1542242741796</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>26</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Sony Wonder</td>
      <td>Logged In</td>
      <td>Samuel</td>
      <td>M</td>
      <td>0</td>
      <td>Gonzalez</td>
      <td>218.06975</td>
      <td>free</td>
      <td>Houston-The Woodlands-Sugar Land, TX</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540493e+12</td>
      <td>597</td>
      <td>Blackbird</td>
      <td>200</td>
      <td>1542253449796</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>61</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Van Halen</td>
      <td>Logged In</td>
      <td>Tegan</td>
      <td>F</td>
      <td>2</td>
      <td>Levine</td>
      <td>289.38404</td>
      <td>paid</td>
      <td>Portland-South Portland, ME</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540794e+12</td>
      <td>602</td>
      <td>Best Of Both Worlds (Remastered Album Version)</td>
      <td>200</td>
      <td>1542260935796</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>80</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Step 2: filter by actions for song plays
df_log = df_log.filter(F.col("page") == "NextSong")
```


```python
df_log.toPandas().shape
```




    (6820, 18)




```python
# Step 3: extract columns for users table
users_cols = ["userId", "firstName", "lastName", "gender", "level"]
```


```python
df_log.select(users_cols).toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>userId</th>
      <th>firstName</th>
      <th>lastName</th>
      <th>gender</th>
      <th>level</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>26</td>
      <td>Ryan</td>
      <td>Smith</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>1</th>
      <td>26</td>
      <td>Ryan</td>
      <td>Smith</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>2</th>
      <td>26</td>
      <td>Ryan</td>
      <td>Smith</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>3</th>
      <td>61</td>
      <td>Samuel</td>
      <td>Gonzalez</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>4</th>
      <td>80</td>
      <td>Tegan</td>
      <td>Levine</td>
      <td>F</td>
      <td>paid</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_log.select(users_cols).toPandas().shape
```




    (6820, 5)




```python
df_log.select(users_cols).dropDuplicates().toPandas().shape
```




    (104, 5)




```python
df_log.select(users_cols).dropDuplicates().toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>userId</th>
      <th>firstName</th>
      <th>lastName</th>
      <th>gender</th>
      <th>level</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>57</td>
      <td>Katherine</td>
      <td>Gay</td>
      <td>F</td>
      <td>free</td>
    </tr>
    <tr>
      <th>1</th>
      <td>84</td>
      <td>Shakira</td>
      <td>Hunt</td>
      <td>F</td>
      <td>free</td>
    </tr>
    <tr>
      <th>2</th>
      <td>22</td>
      <td>Sean</td>
      <td>Wilson</td>
      <td>F</td>
      <td>free</td>
    </tr>
    <tr>
      <th>3</th>
      <td>52</td>
      <td>Theodore</td>
      <td>Smith</td>
      <td>M</td>
      <td>free</td>
    </tr>
    <tr>
      <th>4</th>
      <td>80</td>
      <td>Tegan</td>
      <td>Levine</td>
      <td>F</td>
      <td>paid</td>
    </tr>
  </tbody>
</table>
</div>




```python
users_table_df = df_log.select(users_cols).dropDuplicates()
```


```python
# write this to s3 bucket
users_table_df.write.parquet(output + 'users_table', mode='Overwrite')
```


```python
users_table_df.write.parquet('data/users_table', mode='Overwrite')
```

## Time table


```python
# # create timestamp column from original timestamp column
get_timestamp = udf()
```


```python
df_log.select('ts').toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ts</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1542241826796</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1542242481796</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1542242741796</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1542253449796</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1542260935796</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.withColumn('epoch', f.date_format(df.epoch.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))
```


```python

```


```python
df_log.withColumn('ts', F.date_format(df_log.ts.cast(dataType=T.TimestampType()), "yyyy-MM-dd")).toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>auth</th>
      <th>firstName</th>
      <th>gender</th>
      <th>itemInSession</th>
      <th>lastName</th>
      <th>length</th>
      <th>level</th>
      <th>location</th>
      <th>method</th>
      <th>page</th>
      <th>registration</th>
      <th>sessionId</th>
      <th>song</th>
      <th>status</th>
      <th>ts</th>
      <th>userAgent</th>
      <th>userId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Harmonia</td>
      <td>Logged In</td>
      <td>Ryan</td>
      <td>M</td>
      <td>0</td>
      <td>Smith</td>
      <td>655.77751</td>
      <td>free</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.541017e+12</td>
      <td>583</td>
      <td>Sehr kosmisch</td>
      <td>200</td>
      <td>50841-09-12</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>26</td>
    </tr>
    <tr>
      <th>1</th>
      <td>The Prodigy</td>
      <td>Logged In</td>
      <td>Ryan</td>
      <td>M</td>
      <td>1</td>
      <td>Smith</td>
      <td>260.07465</td>
      <td>free</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.541017e+12</td>
      <td>583</td>
      <td>The Big Gundown</td>
      <td>200</td>
      <td>50841-09-19</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>26</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Train</td>
      <td>Logged In</td>
      <td>Ryan</td>
      <td>M</td>
      <td>2</td>
      <td>Smith</td>
      <td>205.45261</td>
      <td>free</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.541017e+12</td>
      <td>583</td>
      <td>Marry Me</td>
      <td>200</td>
      <td>50841-09-22</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>26</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Sony Wonder</td>
      <td>Logged In</td>
      <td>Samuel</td>
      <td>M</td>
      <td>0</td>
      <td>Gonzalez</td>
      <td>218.06975</td>
      <td>free</td>
      <td>Houston-The Woodlands-Sugar Land, TX</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540493e+12</td>
      <td>597</td>
      <td>Blackbird</td>
      <td>200</td>
      <td>50842-01-24</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>61</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Van Halen</td>
      <td>Logged In</td>
      <td>Tegan</td>
      <td>F</td>
      <td>2</td>
      <td>Levine</td>
      <td>289.38404</td>
      <td>paid</td>
      <td>Portland-South Portland, ME</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540794e+12</td>
      <td>602</td>
      <td>Best Of Both Worlds (Remastered Album Version)</td>
      <td>200</td>
      <td>50842-04-21</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>80</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_time = df_log.select('ts')
```


```python
df_time.take(5)
```




    [Row(ts=1542241826796),
     Row(ts=1542242481796),
     Row(ts=1542242741796),
     Row(ts=1542253449796),
     Row(ts=1542260935796)]




```python
@udf
def gettimestamp(time):
    import datetime
    time = time/1000
    return datetime.datetime.fromtimestamp(time).strftime("%m-%d-%Y %H:%M:%S")
```


```python
df_time.withColumn("timestamp", gettimestamp("ts")).show()
```

    +-------------+-------------------+
    |           ts|          timestamp|
    +-------------+-------------------+
    |1542241826796|11-15-2018 00:30:26|
    |1542242481796|11-15-2018 00:41:21|
    |1542242741796|11-15-2018 00:45:41|
    |1542253449796|11-15-2018 03:44:09|
    |1542260935796|11-15-2018 05:48:55|
    |1542261224796|11-15-2018 05:53:44|
    |1542261356796|11-15-2018 05:55:56|
    |1542261662796|11-15-2018 06:01:02|
    |1542262057796|11-15-2018 06:07:37|
    |1542262233796|11-15-2018 06:10:33|
    |1542262434796|11-15-2018 06:13:54|
    |1542262456796|11-15-2018 06:14:16|
    |1542262679796|11-15-2018 06:17:59|
    |1542262728796|11-15-2018 06:18:48|
    |1542262893796|11-15-2018 06:21:33|
    |1542263158796|11-15-2018 06:25:58|
    |1542263378796|11-15-2018 06:29:38|
    |1542265716796|11-15-2018 07:08:36|
    |1542265929796|11-15-2018 07:12:09|
    |1542266927796|11-15-2018 07:28:47|
    +-------------+-------------------+
    only showing top 20 rows
    



```python
df_time.printSchema()
```

    root
     |-- ts: long (nullable = true)
    



```python

```


```python
get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
get_hour = F.udf(lambda x: x.hour, T.IntegerType()) 
get_day = F.udf(lambda x: x.day, T.IntegerType()) 
get_week = F.udf(lambda x: x.isocalendar()[1], T.IntegerType()) 
get_month = F.udf(lambda x: x.month, T.IntegerType()) 
get_year = F.udf(lambda x: x.year, T.IntegerType()) 
get_weekday = F.udf(lambda x: x.weekday(), T.IntegerType()) 
```


```python
df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
df_log = df_log.withColumn("hour", get_hour(df_log.timestamp))
df_log = df_log.withColumn("day", get_day(df_log.timestamp))
df_log = df_log.withColumn("week", get_week(df_log.timestamp))
df_log = df_log.withColumn("month", get_month(df_log.timestamp))
df_log = df_log.withColumn("year", get_year(df_log.timestamp))
df_log = df_log.withColumn("weekday", get_weekday(df_log.timestamp))
df_log.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>auth</th>
      <th>firstName</th>
      <th>gender</th>
      <th>itemInSession</th>
      <th>lastName</th>
      <th>length</th>
      <th>level</th>
      <th>location</th>
      <th>method</th>
      <th>...</th>
      <th>ts</th>
      <th>userAgent</th>
      <th>userId</th>
      <th>timestamp</th>
      <th>hour</th>
      <th>day</th>
      <th>week</th>
      <th>month</th>
      <th>year</th>
      <th>weekday</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Harmonia</td>
      <td>Logged In</td>
      <td>Ryan</td>
      <td>M</td>
      <td>0</td>
      <td>Smith</td>
      <td>655.77751</td>
      <td>free</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>PUT</td>
      <td>...</td>
      <td>1542241826796</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>26</td>
      <td>2018-11-15 00:30:26.796</td>
      <td>0</td>
      <td>15</td>
      <td>46</td>
      <td>11</td>
      <td>2018</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>The Prodigy</td>
      <td>Logged In</td>
      <td>Ryan</td>
      <td>M</td>
      <td>1</td>
      <td>Smith</td>
      <td>260.07465</td>
      <td>free</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>PUT</td>
      <td>...</td>
      <td>1542242481796</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>26</td>
      <td>2018-11-15 00:41:21.796</td>
      <td>0</td>
      <td>15</td>
      <td>46</td>
      <td>11</td>
      <td>2018</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Train</td>
      <td>Logged In</td>
      <td>Ryan</td>
      <td>M</td>
      <td>2</td>
      <td>Smith</td>
      <td>205.45261</td>
      <td>free</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>PUT</td>
      <td>...</td>
      <td>1542242741796</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>26</td>
      <td>2018-11-15 00:45:41.796</td>
      <td>0</td>
      <td>15</td>
      <td>46</td>
      <td>11</td>
      <td>2018</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Sony Wonder</td>
      <td>Logged In</td>
      <td>Samuel</td>
      <td>M</td>
      <td>0</td>
      <td>Gonzalez</td>
      <td>218.06975</td>
      <td>free</td>
      <td>Houston-The Woodlands-Sugar Land, TX</td>
      <td>PUT</td>
      <td>...</td>
      <td>1542253449796</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>61</td>
      <td>2018-11-15 03:44:09.796</td>
      <td>3</td>
      <td>15</td>
      <td>46</td>
      <td>11</td>
      <td>2018</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Van Halen</td>
      <td>Logged In</td>
      <td>Tegan</td>
      <td>F</td>
      <td>2</td>
      <td>Levine</td>
      <td>289.38404</td>
      <td>paid</td>
      <td>Portland-South Portland, ME</td>
      <td>PUT</td>
      <td>...</td>
      <td>1542260935796</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>80</td>
      <td>2018-11-15 05:48:55.796</td>
      <td>5</td>
      <td>15</td>
      <td>46</td>
      <td>11</td>
      <td>2018</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 25 columns</p>
</div>




```python
time_cols = ["timestamp", "hour", "day", "week", "month", "year", "weekday"]
```


```python
time_table_df = df_log.select(time_cols)
```


```python
# write to parquet file partition by 
time_table_df.write.parquet('data/time_table', partitionBy=['year', 'month'], mode='Overwrite')
```


```python
# write this to s3 bucket
time_table_df.write.parquet(output + 'time_table', partitionBy=['year', 'month'], mode='Overwrite')
```

## SongPlay table


```python
df_log.printSchema()
```

    root
     |-- artist: string (nullable = true)
     |-- auth: string (nullable = true)
     |-- firstName: string (nullable = true)
     |-- gender: string (nullable = true)
     |-- itemInSession: long (nullable = true)
     |-- lastName: string (nullable = true)
     |-- length: double (nullable = true)
     |-- level: string (nullable = true)
     |-- location: string (nullable = true)
     |-- method: string (nullable = true)
     |-- page: string (nullable = true)
     |-- registration: double (nullable = true)
     |-- sessionId: long (nullable = true)
     |-- song: string (nullable = true)
     |-- status: long (nullable = true)
     |-- ts: long (nullable = true)
     |-- userAgent: string (nullable = true)
     |-- userId: string (nullable = true)
     |-- timestamp: timestamp (nullable = true)
     |-- hour: integer (nullable = true)
     |-- day: integer (nullable = true)
     |-- week: integer (nullable = true)
     |-- month: integer (nullable = true)
     |-- year: integer (nullable = true)
     |-- weekday: integer (nullable = true)
    



```python
songplay_cols_temp = ["timestamp", "userId", "sessionId", "location", "userAgent", "level"]
```


```python
df_log.select(songplay_cols).toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>userId</th>
      <th>sessionId</th>
      <th>location</th>
      <th>userAgent</th>
      <th>level</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-11-15 00:30:26.796</td>
      <td>26</td>
      <td>583</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>free</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-11-15 00:41:21.796</td>
      <td>26</td>
      <td>583</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>free</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-11-15 00:45:41.796</td>
      <td>26</td>
      <td>583</td>
      <td>San Jose-Sunnyvale-Santa Clara, CA</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>free</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-11-15 03:44:09.796</td>
      <td>61</td>
      <td>597</td>
      <td>Houston-The Woodlands-Sugar Land, TX</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>free</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-11-15 05:48:55.796</td>
      <td>80</td>
      <td>602</td>
      <td>Portland-South Portland, ME</td>
      <td>"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...</td>
      <td>paid</td>
    </tr>
  </tbody>
</table>
</div>




```python
# read the partitioned data
df_artists_read = spark.read.option("mergeSchema", "true").parquet("data/artists_table")
df_songs_read = spark.read.option("mergeSchema", "true").parquet("data/songs_table")
```


```python
df_artists_read.toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist_id</th>
      <th>artist_name</th>
      <th>artist_location</th>
      <th>artist_latitude</th>
      <th>artist_longitude</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ARDR4AC1187FB371A1</td>
      <td>Montserrat Caballé;Placido Domingo;Vicente Sar...</td>
      <td></td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AREBBGV1187FB523D2</td>
      <td>Mike Jones (Featuring CJ_ Mello &amp; Lil' Bran)</td>
      <td>Houston, TX</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ARMAC4T1187FB3FA4C</td>
      <td>The Dillinger Escape Plan</td>
      <td>Morris Plains, NJ</td>
      <td>40.82624</td>
      <td>-74.47995</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ARPBNLO1187FB3D52F</td>
      <td>Tiny Tim</td>
      <td>New York, NY</td>
      <td>40.71455</td>
      <td>-74.00712</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ARDNS031187B9924F0</td>
      <td>Tim Wilson</td>
      <td>Georgia</td>
      <td>32.67828</td>
      <td>-83.22295</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_songs_read.toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>song_id</th>
      <th>title</th>
      <th>duration</th>
      <th>year</th>
      <th>artist_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>SOAOIBZ12AB01815BE</td>
      <td>I Hold Your Hand In Mine [Live At Royal Albert...</td>
      <td>43.36281</td>
      <td>2000</td>
      <td>ARPBNLO1187FB3D52F</td>
    </tr>
    <tr>
      <th>1</th>
      <td>SONYPOM12A8C13B2D7</td>
      <td>I Think My Wife Is Running Around On Me (Taco ...</td>
      <td>186.48771</td>
      <td>2005</td>
      <td>ARDNS031187B9924F0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>SODREIN12A58A7F2E5</td>
      <td>A Whiter Shade Of Pale (Live @ Fillmore West)</td>
      <td>326.00771</td>
      <td>0</td>
      <td>ARLTWXK1187FB5A3F8</td>
    </tr>
    <tr>
      <th>3</th>
      <td>SOYMRWW12A6D4FAB14</td>
      <td>The Moon And I (Ordinary Day Album Version)</td>
      <td>267.70240</td>
      <td>0</td>
      <td>ARKFYS91187B98E58F</td>
    </tr>
    <tr>
      <th>4</th>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>Streets On Fire (Explicit Album Version)</td>
      <td>279.97995</td>
      <td>0</td>
      <td>ARPFHN61187FB575F6</td>
    </tr>
  </tbody>
</table>
</div>




```python
# merge song and artists
df_songs_read.join(df_artists_read, 'artist_id').toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist_id</th>
      <th>song_id</th>
      <th>title</th>
      <th>duration</th>
      <th>year</th>
      <th>artist_name</th>
      <th>artist_location</th>
      <th>artist_latitude</th>
      <th>artist_longitude</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ARPBNLO1187FB3D52F</td>
      <td>SOAOIBZ12AB01815BE</td>
      <td>I Hold Your Hand In Mine [Live At Royal Albert...</td>
      <td>43.36281</td>
      <td>2000</td>
      <td>Tiny Tim</td>
      <td>New York, NY</td>
      <td>40.71455</td>
      <td>-74.00712</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ARDNS031187B9924F0</td>
      <td>SONYPOM12A8C13B2D7</td>
      <td>I Think My Wife Is Running Around On Me (Taco ...</td>
      <td>186.48771</td>
      <td>2005</td>
      <td>Tim Wilson</td>
      <td>Georgia</td>
      <td>32.67828</td>
      <td>-83.22295</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ARLTWXK1187FB5A3F8</td>
      <td>SODREIN12A58A7F2E5</td>
      <td>A Whiter Shade Of Pale (Live @ Fillmore West)</td>
      <td>326.00771</td>
      <td>0</td>
      <td>King Curtis</td>
      <td>Fort Worth, TX</td>
      <td>32.74863</td>
      <td>-97.32925</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ARKFYS91187B98E58F</td>
      <td>SOYMRWW12A6D4FAB14</td>
      <td>The Moon And I (Ordinary Day Album Version)</td>
      <td>267.70240</td>
      <td>0</td>
      <td>Jeff And Sheri Easter</td>
      <td></td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ARPFHN61187FB575F6</td>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>Streets On Fire (Explicit Album Version)</td>
      <td>279.97995</td>
      <td>0</td>
      <td>Lupe Fiasco</td>
      <td>Chicago, IL</td>
      <td>41.88415</td>
      <td>-87.63241</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_joined_songs_artists = df_songs_read.join(df_artists_read, 'artist_id').select("artist_id", "song_id", "title", "artist_name")
```


```python
songplay_cols = ["timestamp", "userId", "song_id", "artist_id", "sessionId", "location", "userAgent", "level", "month", "year"]
```


```python
# join df_logs with df_joined_songs_artists
df_log.join(df_joined_songs_artists, df_log.artist == df_joined_songs_artists.artist_name).select(songplay_cols).toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>userId</th>
      <th>song_id</th>
      <th>artist_id</th>
      <th>sessionId</th>
      <th>location</th>
      <th>userAgent</th>
      <th>level</th>
      <th>month</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-11-10 07:47:51.796</td>
      <td>44</td>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>ARPFHN61187FB575F6</td>
      <td>350</td>
      <td>Waterloo-Cedar Falls, IA</td>
      <td>Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; r...</td>
      <td>paid</td>
      <td>11</td>
      <td>2018</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-11-06 18:34:31.796</td>
      <td>97</td>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>ARPFHN61187FB575F6</td>
      <td>293</td>
      <td>Lansing-East Lansing, MI</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>paid</td>
      <td>11</td>
      <td>2018</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-11-06 16:04:44.796</td>
      <td>2</td>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>ARPFHN61187FB575F6</td>
      <td>126</td>
      <td>Plymouth, IN</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>free</td>
      <td>11</td>
      <td>2018</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-11-28 23:22:57.796</td>
      <td>24</td>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>ARPFHN61187FB575F6</td>
      <td>984</td>
      <td>Lake Havasu City-Kingman, AZ</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>paid</td>
      <td>11</td>
      <td>2018</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-11-14 13:11:26.796</td>
      <td>34</td>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>ARPFHN61187FB575F6</td>
      <td>495</td>
      <td>Milwaukee-Waukesha-West Allis, WI</td>
      <td>Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; r...</td>
      <td>free</td>
      <td>11</td>
      <td>2018</td>
    </tr>
  </tbody>
</table>
</div>




```python
songplay_table_df = df_log.join(df_joined_songs_artists, df_log.artist == df_joined_songs_artists.artist_name).select(songplay_cols)
songplay_table_df = songplay_table_df.withColumn("songplay_id", F.monotonically_increasing_id())
```


```python
songplay_table_df.toPandas().head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>userId</th>
      <th>song_id</th>
      <th>artist_id</th>
      <th>sessionId</th>
      <th>location</th>
      <th>userAgent</th>
      <th>level</th>
      <th>month</th>
      <th>year</th>
      <th>songplay_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-11-10 07:47:51.796</td>
      <td>44</td>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>ARPFHN61187FB575F6</td>
      <td>350</td>
      <td>Waterloo-Cedar Falls, IA</td>
      <td>Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; r...</td>
      <td>paid</td>
      <td>11</td>
      <td>2018</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-11-06 18:34:31.796</td>
      <td>97</td>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>ARPFHN61187FB575F6</td>
      <td>293</td>
      <td>Lansing-East Lansing, MI</td>
      <td>"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5...</td>
      <td>paid</td>
      <td>11</td>
      <td>2018</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-11-06 16:04:44.796</td>
      <td>2</td>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>ARPFHN61187FB575F6</td>
      <td>126</td>
      <td>Plymouth, IN</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>free</td>
      <td>11</td>
      <td>2018</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-11-28 23:22:57.796</td>
      <td>24</td>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>ARPFHN61187FB575F6</td>
      <td>984</td>
      <td>Lake Havasu City-Kingman, AZ</td>
      <td>"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...</td>
      <td>paid</td>
      <td>11</td>
      <td>2018</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-11-14 13:11:26.796</td>
      <td>34</td>
      <td>SOWQTQZ12A58A7B63E</td>
      <td>ARPFHN61187FB575F6</td>
      <td>495</td>
      <td>Milwaukee-Waukesha-West Allis, WI</td>
      <td>Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; r...</td>
      <td>free</td>
      <td>11</td>
      <td>2018</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>




```python
# write this to parquet file
# write to parquet file partition by 
songplay_table_df.write.parquet('data/songplays_table', partitionBy=['year', 'month'], mode='Overwrite')
```


```python

```


```python

```


```python

```


```python
from pyspark.sql import functions as F
```


```python
from glob import glob
```


```python
test_df = spark.read.json(glob("test/*.json"))
```


```python
test_df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>song_id</th>
      <th>title</th>
      <th>artist_id</th>
      <th>year</th>
      <th>duration</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>SOYMRWW12A6D4FAB14</td>
      <td>The Moon And I (Ordinary Day Album Version)</td>
      <td>ARKFYS91187B98E58F</td>
      <td>0</td>
      <td>267.70240</td>
    </tr>
    <tr>
      <th>1</th>
      <td>SOHKNRJ12A6701D1F8</td>
      <td>Drop of Rain</td>
      <td>AR10USD1187B99F3F1</td>
      <td>0</td>
      <td>189.57016</td>
    </tr>
    <tr>
      <th>2</th>
      <td>SOYMRWW12A6D4FAB14</td>
      <td>The Moon And SUN</td>
      <td>ASKFYS91187B98E58F</td>
      <td>0</td>
      <td>269.70240</td>
    </tr>
    <tr>
      <th>3</th>
      <td>SOUDSGM12AC9618304</td>
      <td>Insatiable (Instrumental Version)</td>
      <td>ARNTLGG11E2835DDB9</td>
      <td>0</td>
      <td>266.39628</td>
    </tr>
  </tbody>
</table>
</div>




```python
test_df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).groupBy('song_id').count().show()
```

    +------------------+-----+
    |           song_id|count|
    +------------------+-----+
    |SOUDSGM12AC9618304|    1|
    |SOYMRWW12A6D4FAB14|    2|
    |SOHKNRJ12A6701D1F8|    1|
    +------------------+-----+
    



```python
test_df.select(F.col('song_id'), 'title') \
    .groupBy('song_id') \
    .agg({'title': 'first'}) \
    .withColumnRenamed('first(title)', 'title1') \
    .show()
```

    +------------------+--------------------+
    |           song_id|               title|
    +------------------+--------------------+
    |SOUDSGM12AC9618304|Insatiable (Instr...|
    |SOYMRWW12A6D4FAB14|The Moon And I (O...|
    |SOHKNRJ12A6701D1F8|        Drop of Rain|
    +------------------+--------------------+
    



```python
t1 = test_df.select(F.col('song_id'), 'title') \
    .groupBy('song_id') \
    .agg({'title': 'first'}) \
    .withColumnRenamed('first(title)', 'title1')
```


```python
t2 = test_df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
```


```python
t1.join(t2, 'song_id').where(F.col("title1") == F.col("title")).select(["song_id", "title", "artist_id", "year", "duration"]).show()
```

    +------------------+--------------------+------------------+----+---------+
    |           song_id|               title|         artist_id|year| duration|
    +------------------+--------------------+------------------+----+---------+
    |SOYMRWW12A6D4FAB14|The Moon And I (O...|ARKFYS91187B98E58F|   0| 267.7024|
    |SOHKNRJ12A6701D1F8|        Drop of Rain|AR10USD1187B99F3F1|   0|189.57016|
    |SOUDSGM12AC9618304|Insatiable (Instr...|ARNTLGG11E2835DDB9|   0|266.39628|
    +------------------+--------------------+------------------+----+---------+
    

