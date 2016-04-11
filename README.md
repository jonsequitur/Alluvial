Alluvial 
========

[![Build Status](https://ci.appveyor.com/api/projects/status/github/jonsequitur/alluvial?svg=true&branch=master)](https://ci.appveyor.com/project/jonsequitur/alluvial) [![NuGet Status](http://img.shields.io/nuget/v/Alluvial.svg?style=flat)](https://www.nuget.org/packages/Alluvial/)

[![Join the chat at https://gitter.im/jonsequitur/Alluvial](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/jonsequitur/Alluvial?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[Alluvial](http://en.wiktionary.org/wiki/alluvial) is for aggregating and transforming streams of data. It's intended to address the need to both aggregate historical data and also process new data in realtime, to catch up and stay caught up, or to jump into the stream at any point. Use cases include: 

* Building projections from event stores (for CQRS and event-sourced models), and keeping them updated as new events appear
* Processing and analyzing logs 
* Treating arbitrary data as a queue 
* Migrating data

If you can define a query that returns your data as an ordered stream, Alluvial does the rest. 

Here's what it can do currently:

* Define data from any source as a data stream
* Query those data streams
* Derive streams from other streams
* Track cursors that allow you to resume consumption of a stream at a later point and on a different node
* Restart streams queries from any past position
* Create persisted projections based on existing data
* Update persisted projections as new data appears
* Create projections on demand
* Distributing stream query work across nodes

Here's what's planned:

* Elastic redistribution of stream query work using Raft

