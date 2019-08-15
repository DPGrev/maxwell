package com.zendesk.maxwell.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.StoppableTaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;

class PubsubCallback implements ApiFutureCallback<String> {
  public static final Logger LOGGER = LoggerFactory.getLogger(PubsubCallback.class);

  private final AbstractAsyncProducer.CallbackCompleter cc;
  private final Position position;
  private final String json;
  private MaxwellContext context;

  private Counter succeededMessageCount;
  private Counter failedMessageCount;
  private Meter succeededMessageMeter;
  private Meter failedMessageMeter;

  public PubsubCallback(AbstractAsyncProducer.CallbackCompleter cc,
                        Position position, String json,
                        Counter producedMessageCount, Counter failedMessageCount,
                        Meter succeededMessageMeter, Meter failedMessageMeter,
                        MaxwellContext context) {
    this.cc = cc;
    this.position = position;
    this.json = json;
    this.succeededMessageCount = producedMessageCount;
    this.failedMessageCount = failedMessageCount;
    this.succeededMessageMeter = succeededMessageMeter;
    this.failedMessageMeter = failedMessageMeter;
    this.context = context;
  }

  @Override
  public void onSuccess(String messageId) {
    this.succeededMessageCount.inc();
    this.succeededMessageMeter.mark();

    if ( LOGGER.isDebugEnabled() ) {
      LOGGER.debug("->  " + this.json);
      LOGGER.debug("    " + this.position);
      LOGGER.debug("");
    }

    cc.markCompleted();
  }

  @Override
  public void onFailure(Throwable t) {
    this.failedMessageCount.inc();
    this.failedMessageMeter.mark();

    LOGGER.error(t.getClass().getSimpleName() + " @ " + position);
    LOGGER.error(t.getLocalizedMessage());

    if ( !this.context.getConfig().ignoreProducerError ) {
      this.context.terminate(new RuntimeException(t));
      return;
    }

    cc.markCompleted();
  }
}

public class MaxwellPubsubProducer extends AbstractProducer {
  public static final Logger LOGGER = LoggerFactory.getLogger(MaxwellPubsubProducer.class);

  private final ArrayBlockingQueue<RowMap> queue;
  private final MaxwellPubsubProducerWorker worker;

  public MaxwellPubsubProducer(MaxwellContext context, String pubsubProjectId,
                               String pubsubTopic, String ddlPubsubTopic)
                               throws IOException {
    super(context);
    this.queue = new ArrayBlockingQueue<>(100);
    this.worker = new MaxwellPubsubProducerWorker(context, pubsubProjectId,
                                                  pubsubTopic, ddlPubsubTopic,
                                                  this.queue);
    Thread thread = new Thread(this.worker, "maxwell-pubsub-worker");
    thread.setDaemon(true);
    thread.start();
  }

  @Override
  public void push(RowMap r) throws Exception {
    this.queue.put(r);
  }

  @Override
  public StoppableTask getStoppableTask() {
    return this.worker;
  }
}

class MaxwellPubsubProducerWorker
    extends AbstractAsyncProducer implements Runnable, StoppableTask {
  static final Logger LOGGER = LoggerFactory.getLogger(MaxwellPubsubProducerWorker.class);

  private final String projectId;
  private Publisher pubsub;
  private final ProjectTopicName topic;
  private final ProjectTopicName ddlTopic;
  private Publisher ddlPubsub;
  private final ArrayBlockingQueue<RowMap> queue;
  private Thread thread;
  private StoppableTaskState taskState;

  public MaxwellPubsubProducerWorker(MaxwellContext context,
                                     String pubsubProjectId, String pubsubTopic,
                                     String ddlPubsubTopic,
                                     ArrayBlockingQueue<RowMap> queue)
                                     throws IOException {
    super(context);

    // Batch settings control how the publisher batches messages
    long requestBytesThreshold = 9500000L; // default : 1 byte
    long messageCountBatchSize = 950L; // default : 1 message
    Duration publishDelayThreshold = Duration.ofMillis(50); // default : 1 ms

    // Publish request get triggered based on request size, messages count & time since last publish
    BatchingSettings batchingSettings =
        BatchingSettings.newBuilder()
            .setElementCountThreshold(messageCountBatchSize)
            .setRequestByteThreshold(requestBytesThreshold)
            .setDelayThreshold(publishDelayThreshold)
            .build();
    
    // Retry settings control how the publisher handles retryable failures
    Duration retryDelay = Duration.ofMillis(5); // default: 5 ms
    double retryDelayMultiplier = 2.0; // back off for repeated failures, default: 2.0
    Duration maxRetryDelay = Duration.ofSeconds(600); // default : Long.MAX_VALUE
    Duration totalTimeout = Duration.ofSeconds(50); // default: 10 seconds
    Duration initialRpcTimeout = Duration.ofSeconds(10); // default: 10 seconds
    Duration maxRpcTimeout = Duration.ofSeconds(10); // default: 10 seconds

    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setInitialRetryDelay(retryDelay)
            .setRetryDelayMultiplier(retryDelayMultiplier)
            .setMaxRetryDelay(maxRetryDelay)
            .setTotalTimeout(totalTimeout)
            .setInitialRpcTimeout(initialRpcTimeout)
            .setMaxRpcTimeout(maxRpcTimeout)
            .build();

    this.projectId = pubsubProjectId;
    this.topic = ProjectTopicName.of(pubsubProjectId, pubsubTopic);
    this.pubsub = Publisher.newBuilder(this.topic).setBatchingSettings(batchingSettings).setRetrySettings(retrySettings).build();

    if ( context.getConfig().outputConfig.outputDDL == true &&
         ddlPubsubTopic != pubsubTopic ) {
      this.ddlTopic = ProjectTopicName.of(pubsubProjectId, ddlPubsubTopic);
      this.ddlPubsub = Publisher.newBuilder(this.ddlTopic).setBatchingSettings(batchingSettings).setRetrySettings(retrySettings).build();
    } else {
      this.ddlTopic = this.topic;
      this.ddlPubsub = this.pubsub;
    }

    Metrics metrics = context.getMetrics();

    this.queue = queue;
    this.taskState = new StoppableTaskState("MaxwellPubsubProducerWorker");
  }

  @Override
  public void run() {
    this.thread = Thread.currentThread();
    while ( true ) {
      try {
        RowMap row = queue.take();
        if ( !taskState.isRunning() ) {
          taskState.stopped();
          return;
        }
        this.push(row);
      } catch ( Exception e ) {
        taskState.stopped();
        context.terminate(e);
        return;
      }
    }
  }

  @Override
  public void sendAsync(RowMap r, AbstractAsyncProducer.CallbackCompleter cc)
      throws Exception {
    String message = r.toJSON(outputConfig);
    ByteString data = ByteString.copyFromUtf8(message);
    PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

    if ( r instanceof DDLMap ) {
	  ApiFuture<String> apiFuture = ddlPubsub.publish(pubsubMessage);
	  PubsubCallback callback = new PubsubCallback(cc, r.getNextPosition(), message,
			  this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter, this.context);

	  ApiFutures.addCallback(apiFuture, callback, MoreExecutors.directExecutor());
    } else {
	  ApiFuture<String> apiFuture = pubsub.publish(pubsubMessage);
	  PubsubCallback callback = new PubsubCallback(cc, r.getNextPosition(), message,
			  this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter, this.context);

	  ApiFutures.addCallback(apiFuture, callback, MoreExecutors.directExecutor());
    }
  }

  @Override
  public void requestStop() throws Exception {
    taskState.requestStop();
    pubsub.shutdown();

    if ( ddlPubsub != null ) {
      ddlPubsub.shutdown();
    }
  }

  @Override
  public void awaitStop(Long timeout) throws TimeoutException {
    taskState.awaitStop(thread, timeout);
  }

  @Override
  public StoppableTask getStoppableTask() {
    return this;
  }
}
