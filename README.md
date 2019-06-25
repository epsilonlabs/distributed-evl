# distributed-evl
Extension of EVL which can leverage multi-threading on multiple machines.

To run the JMS implementation, you will need a JMS 2.0 compliant broker on the classpath, such as [Artemis](https://activemq.apache.org/components/artemis/download/).
You will also need an Interim version of [Epsilon](https://www.eclipse.org/epsilon/download/).
Command-line invocation requires [Apache Commons CLI 1.4](https://commons.apache.org/proper/commons-cli/download_cli.cgi).

Invoke the main class (as described in the META-INF/MANIFEST.MF) with the following arguments:
"relative/path/to/script.evl" -models "emf.EmfModel#cached=true,concurrent=true,fileBasedMetamodelUri=file:///relative/path/to/metamodel.ecore,modelUri=file:///relative/path/to/model.xmi" -profile -basePath "/absolute/path/to/resources" -host tcp://brokerhost:61616 -session [session_ID] -shuffle -workers [num_workers] -outfile "relative/path/to/output.log"
You can also add the following options:
-bf [number] -- The batch size (will set to use the batch-based module)
-mp [number] -- Master proportion (i.e. percetnage of jobs processed on the master, between 0 and 1)

It is strongly recommended to specify the "bf" argument. This should be at least as many as the maximum number of logical cores for any given computer in your distributed system.
You must specify the number of workers you expect to connect with the -workers option.
Enabling job order randomisation (with the -shuffle option) is strongly recommended.
You can specify multiple models to the -models option (see command-line help).
The session ID should be a unique number on each invocation of the master, otherwise you will need to reset the broker.
Message persistence should be completely disabled in your broker settings.

To run a worker, call the EvlJmsWorker class with the following arguments:
"/absolute/path/to/resources/on/this/worker/" [session_ID] tcp://brokerhost:61616

The first argument specifies where to find resources, just like the -basePath argument to the master. Note that this will typically be the same across workers (and perhaps even the master), but can be changed depending on your infrastructure.
The second argument should be the same for all workers and the master for any given invocation.
The third argument specifies the broker's URL, just like the -host option in the master.
