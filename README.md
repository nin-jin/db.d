# db.d
Fast and compact immutable graph database

## Key features of MVP (soming soon)

* **Fast data adding.** We need not of "Write Ahead Log" and two-step commits.
* **Compact storing.** We need not to reserve memory for inplace data changing.
* **Fast lookuping.** We have direct links between records and record local small indexes.
* **Easy API.** We have same REST API over HTTP and WebSockets.
* **[ACID](https://en.wikipedia.org/wiki/ACID).** Every commit is atomic. 
* **Fast restart.** We need not to restore database at start after an unexpected server shutdown. Simply using last commited state.

## Future features

* **Multy step transaction support!**
* **Data updates subscribing!**
* **SQL support?**
* **Automatic replication!**
* **Configurable partitioning!**
* **Integrated map-reduce!**
* **Ports to other languages?**
* **Administration user interface!**
* **Full-text indexes!**
* **Spatial indexes!**
* **Schema support?**
* **Triggers support?**
* **ACL support!**
* **Drivers for most popular languages!**
