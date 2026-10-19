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
import io.reshapr.ctrl.model.ConfigurationPlan;
import io.reshapr.ctrl.model.Exposition;
import io.reshapr.ctrl.model.GatewayGroup;
import io.reshapr.ctrl.model.Service;
import io.reshapr.ctrl.model.ServiceType;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.UserTransaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class ClusterEventBroadcasterTest {

   @Inject
   ClusterEventBroadcaster broadcaster;

   @Inject
   HazelcastInstance hazelcast;

   @Inject
   UserTransaction transaction;

   private ITopic<ClusterEvents.ExpositionGatewayGroupEvent> topic;
   private Exposition exposition;
   private GatewayGroup gatewayGroup;
   private long publishedBefore;

   @BeforeEach
   void setUp() {
      topic = hazelcast.getTopic("exposition-changes");
      publishedBefore = topic.getLocalTopicStats().getPublishOperationCount();
      exposition = new Exposition();
      exposition.id = "test-" + UUID.randomUUID();
      exposition.organizationId = "service-organization";
      gatewayGroup = new GatewayGroup();
      gatewayGroup.id = "test-group";
      gatewayGroup.organizationId = "gateway-organization";
      gatewayGroup.labels = Map.of("region", "test");
      exposition.gatewayGroup = gatewayGroup;
      Service service = new Service();
      service.id = "test-service";
      service.name = "Test API";
      service.version = "1.0";
      service.type = ServiceType.REST;
      service.organizationId = exposition.organizationId;
      exposition.service = service;
      ConfigurationPlan plan = new ConfigurationPlan();
      plan.id = "test-plan";
      plan.name = "default";
      plan.backendEndpoint = "https://backend.example.com";
      plan.service = service;
      exposition.configurationPlan = plan;
   }

   @AfterEach
   void tearDown() throws Exception {
      if (transaction.getStatus() != Status.STATUS_NO_TRANSACTION) {
         transaction.rollback();
      }
   }

   @ParameterizedTest
   @EnumSource(Change.class)
   void publishesOnlyAfterCommit(Change change) throws Exception {
      transaction.begin();
      publish(change);
      assertPublished(0);
      transaction.commit();
      assertPublished(1);
   }

   @ParameterizedTest
   @EnumSource(Change.class)
   void doesNotPublishRolledBackChanges(Change change) throws Exception {
      transaction.begin();
      publish(change);
      assertPublished(0);
      transaction.rollback();
      assertPublished(0);
   }

   @ParameterizedTest
   @EnumSource(Change.class)
   void doesNotPublishWhenAlreadyMarkedRollbackOnly(Change change) throws Exception {
      transaction.begin();
      transaction.setRollbackOnly();
      publish(change);
      assertPublished(0);
      transaction.rollback();
      assertPublished(0);
   }

   @ParameterizedTest
   @EnumSource(Change.class)
   void publishesImmediatelyWithoutTransaction(Change change) {
      publish(change);
      assertPublished(1);
   }

   private void publish(Change change) {
      switch (change) {
         case CREATED -> broadcaster.publishExpositionCreationEvent(exposition, gatewayGroup);
         case UPDATED -> broadcaster.publishExpositionUpdateEvent(exposition, gatewayGroup);
         case DELETED -> broadcaster.publishExpositionDeletionEvent(exposition, gatewayGroup);
      }
   }

   private void assertPublished(long expected) {
      assertEquals(publishedBefore + expected, topic.getLocalTopicStats().getPublishOperationCount());
   }

   enum Change {
      CREATED, UPDATED, DELETED
   }
}
