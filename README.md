# HorizonDB

HorizonDB is a time series database that has been designed specially to store Market data.

Most of the tick databases today are in memory column oriented database(e.g. KDB).
HorizonDB takes another approach. In HorizonDB, time series are in fact a stream of records where records 
can be of different types. 

The advantage of this approach is that disk reads are extremely fast as they involve only a minimum of disk seeks. 
So, if you want to load all quotes and trades up to depth 5 for one day of the STX50 for example, it will be extremely 
fast as it will involve only one disk seek. 

The disadvantage of this approach is that if your time series contains quotes and trades up to depth 5 and that 
you are only interested by the best bid and best ask price, it will be slower than it could be as you will have to read
more data from the disk than what you actually need. Nevertheless, even in this case, the speed should still be reasonable.
If not, you should consider creating another time series with the reduced set of data that you need. As HorizonDB uses 
heavily compression the impact on disk usage should be small.
 

## Project Maturity

HorizonDB is still in a development phase and has not being officially released.

## Requirements

Oracle JDK >= 1.7 (other JDKs have not been tested)

## Inspiration

HorizonDB has been designed by trying to learn from other existing databases. 

* Cassandra (BigTable): commit log, memtables, slab allocator
* CouchDB/CouchBase: append only B+Tree 
* Membase: binary protocol headers
* Mysql: MyISAM compression 

## License

Copyright 2013-2014 Benjamin LERER

HorizonDB is licensed under the [Apache Public License 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)