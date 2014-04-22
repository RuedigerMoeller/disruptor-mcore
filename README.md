disruptor-mcore
===============

a benchmark simulating an event processing server/component.
One solution uses disruptor and splits processing of each request onto several threads 
N for decoding, 1 for processing request logic, M for encoding.
The other solution uses regualar ThreadPool to schedule requests to several threads.

Both solutions ensure that requests are processed in order of arrival despite going parallel.

Run *DisruptorService.main* for disruptor results, 
*UnorderedThreadPoolService.main* for traditional multithreading results.
Differences depend a lot on hardware platform, additionally the more work is put into encoding 
(simulated by adding some dummy hashmap to each request), the better the scaling of the threading solution. For small
units of work, disruptor performs better. Disruptor has most probably lower latency as it splits processing per individual request, however latency is not measured.

The test requires at least 8 REAL cores (no HT) to provide meaningful results.

Set flag in *LoadFeeder* to true in order to run tests with more per-request work.
