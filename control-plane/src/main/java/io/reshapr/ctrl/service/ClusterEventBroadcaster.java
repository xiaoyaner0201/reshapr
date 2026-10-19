/*
 * Copyright The Reshapr Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reshapr.ctrl.service;

import io.reshapr.ctrl.event.ClusterEvents;
import io.reshapr.ctrl.model.Exposition;
import io.reshapr.ctrl.model.GatewayGroup;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import io.quarkus.grpc.GrpcService;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.TransactionPhase;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ClusterEventBroadcaster {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   private final HazelcastInstance hazelcast;
   private final ExpositionDiscoveryServiceHandler expositionDiscoveryServiceHandler;
   private final Event<ClusterEvents.ExpositionGatewayGroupEvent> expositionChanges;

   private ITopic<ClusterEvents.ExpositionGatewayGroupEvent> expositionChangesTopic;

   public ClusterEventBroadcaster(HazelcastInstance hazelcast,
                                  @GrpcService ExpositionDiscoveryServiceHandler expositionDiscoveryServiceHandler,
                                  Event<ClusterEvents.ExpositionGatewayGroupEvent> expositionChanges) {
      this.hazelcast = hazelcast;
      this.expositionDiscoveryServiceHandler = expositionDiscoveryServiceHandler;
      this.expositionChanges = expositionChanges;
   }

   /** Application startup method. */
   void onStart(@Observes StartupEvent ev) {
      expositionChangesTopic = hazelcast.getTopic("exposition-changes");
      expositionChangesTopic.addMessageListener(
            message -> {receiveExpositionChangeEvent(message.getMessageObject());}
      );
      logger.info("ClusterEventBroadcaster started and ready to publish/receive events.");
   }

   /**
    * Publishes an exposition creation event to the cluster.
    * @param exposition The exposition that was created.
    * @param gatewayGroup The gateway group associated with the exposition creation event.
    */
   public void publishExpositionCreationEvent(Exposition exposition, GatewayGroup gatewayGroup) {
      logger.infof("Publishing exposition creation for exposition '%s'", exposition.id);
      expositionChanges.fire(new ClusterEvents.ExpositionGatewayGroupCreationEvent(exposition, gatewayGroup));
   }

   /**
    * Publishes an exposition update event to the cluster.
    * @param exposition The exposition that was updated.
    * @param gatewayGroup The gateway group associated with the exposition update event.
    */
   public void publishExpositionUpdateEvent(Exposition exposition, GatewayGroup gatewayGroup) {
      logger.infof("Publishing exposition change for exposition '%s'", exposition.id);
      expositionChanges.fire(new ClusterEvents.ExpositionGatewayGroupUpdateEvent(exposition, gatewayGroup));
   }

   /**
    * Publishes an exposition deletion event to the cluster.
    * @param exposition The exposition that was deleted.
    * @param gatewayGroup The gateway group associated with the exposition deletion event.
    */
   public void publishExpositionDeletionEvent(Exposition exposition, GatewayGroup gatewayGroup) {
      logger.infof("Publishing exposition deletion for exposition '%s'", exposition.id);
      expositionChanges.fire(new ClusterEvents.ExpositionGatewayGroupDeletionEvent(exposition, gatewayGroup));
   }

   void publishExpositionChangeAfterCommit(
         @Observes(during = TransactionPhase.AFTER_SUCCESS) ClusterEvents.ExpositionGatewayGroupEvent event) {
      // A gateway fetching artifacts must see the committed exposition, not only a flushed row.
      expositionChangesTopic.publish(event);
   }

   private void receiveExpositionChangeEvent(ClusterEvents.ExpositionGatewayGroupEvent event) {
      logger.infof("Received exposition change event: %s for exposition '%s'", event.getClass().getName(), event.exposition().id);

      switch (event) {
         case ClusterEvents.ExpositionGatewayGroupCreationEvent creationEvent -> {
            expositionDiscoveryServiceHandler.notifyExpositionCreation(creationEvent.exposition(), creationEvent.gatewayGroup());
         }
         case ClusterEvents.ExpositionGatewayGroupUpdateEvent updateEvent -> {
            expositionDiscoveryServiceHandler.notifyExpositionUpdate(updateEvent.exposition(), updateEvent.gatewayGroup());
         }
         case ClusterEvents.ExpositionGatewayGroupDeletionEvent deletionEvent -> {
            expositionDiscoveryServiceHandler.notifyExpositionDeletion(deletionEvent.exposition(), deletionEvent.gatewayGroup());
         }
      }
   }
}
