Alluvial 
========

[![Build Status](https://ci.appveyor.com/api/projects/status/github/jonsequitur/alluvial?svg=true&branch=master)](https://ci.appveyor.com/project/jonsequitur/alluvial) [![NuGet Status](http://img.shields.io/nuget/v/Alluvial.svg?style=flat)](https://www.nuget.org/packages/Alluvial/) [![Join the chat at https://gitter.im/jonsequitur/Alluvial](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/jonsequitur/Alluvial?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[Alluvial](http://en.wiktionary.org/wiki/alluvial) provides a programming model for aggregating and transforming streams of data and parallelizing and distributing workloads. The model isn't specific to any server technology. It's intended to address both aggregation of historical data and also processing of new data in realtime using the same code. You can use Alluvial to catch up and stay caught up, or replay a stream from any point. 

Here are some of the things it can be used for:

* Building projections from event stores (for event-sourced models).
* Keep projections updated as new events appear.
* Distriburing heavy workloads, such as analyzing logs. 
* Migrating large volumes of data.

If you can define a query that returns your data as an ordered stream, Alluvial does the rest. 

Here's what it can do currently:

* Define data from any source as a data stream
* Query those data streams.
* Derive streams from other streams.
* Create persisted projections based on existing data.
* Update persisted projections as new data appears.
* Create projections on demand.
* Track cursors that allow you to resume consumption of a stream at a later point and on a different node.
* Restart streams queries from any past position.
* Distributing stream query work across instances of your application.
