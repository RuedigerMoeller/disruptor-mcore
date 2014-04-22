disruptor-mcore
===============

a benchmark simulating a event processing server/component.
One solution uses disruptor and dispatches each request onto several threads decoding/processing/encoding 
in order to speed up serial processing by using multiple cores.
The other solution uses regualar ThreadPool to schedule requests to several threads. Note this way procesing is 
in sequence anymore (a property which is often required).

Run DisruptorService for disruptor results, UnorderedThreadPoolService.main for traditional multithreading results.
Differences depend a lot on hardware platform, additionally the more work is put into encoding 
(simulated by adding some dummy hashmap to each request), the better the scaling of the threading solution. For small
units of work, disruptor performas better.

Set flag in LoadFeeder to true in order to run tests with more per-request work.
