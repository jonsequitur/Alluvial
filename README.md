Alluvial [![Build Status](https://ci.appveyor.com/api/projects/status/github/jonsequitur/alluvial?svg=true)](https://ci.appveyor.com/project/jonsequitur/alluvial)
========

[Alluvial](http://en.wiktionary.org/wiki/alluvial) is for streams of data that you want to aggregate. It's intended to address the need to both aggregate historical data and also process new data in realtime, to catch up and stay caught up, or to jump into the stream at any point. Use cases include: 

* Building projections from event stores (for CQRS and event-sourced models), and keeping them updated as new events appear
* Processing and analyzing logs 
* Treating arbitrary data as a queue 
* Migrating and transorming data

If you can define your data as an ordered stream, Alluvial does the rest. 

Alluvial is very young and can use your help.

But here's what it can do so far:

* Define arbitrary data as a data stream
* Query those data streams
* Derive streams from other streams
* Track cursors that allow you to resume consumption of a stream at a later point
* Create persisted projections based on existing data
* Update persisted projections as new data appears
* Create projections on demand

Here's what's planned:

* Partitioning work across nodes
* Elastic redistribution of work partitions

