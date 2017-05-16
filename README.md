## CloudWave Project
These are the source code that processed the large binary signal data (up to 11TB) using Hadoop MapReduce model and read header information from MySQL.

.java Files
-------
**JSONFileInputFormat.java**: where `JSONFileInputFormat.class` lies. Since there's no native class for json in hadoop, I have to extend this class to support it.

**JSONRecordReader.java**: where `JSONRecordReader.class` lies. For same reason as above, I need to implement this class so that JSON is accepted.

**MAPREDUCE_JSON.java**: This is the main logic to process the data. There're `JSONMap class`, which contains `map` function and `JSONReduce class`, which contains `reduce` function  in it. It also contain other classes to compare, sort and partition segments. Database connector is also included. `main` function is at the bottom. 
