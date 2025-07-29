/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendStoreSystemInfo;
import org.apache.hugegraph.backend.store.raft.RaftGroupManager;
import org.apache.hugegraph.backend.tx.SchemaTransaction;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.TypedOption;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.kvstore.KvStore;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.masterelection.RoleElectionStateMachine;
import org.apache.hugegraph.rpc.RpcServiceConfig4Client;
import org.apache.hugegraph.rpc.RpcServiceConfig4Server;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeFeatures;
import org.apache.hugegraph.structure.KvElement;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.traversal.optimize.HugeCountStepStrategy;
import org.apache.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import org.apache.hugegraph.traversal.optimize.HugePrimaryKeyStrategy;
import org.apache.hugegraph.traversal.optimize.HugeVertexStepStrategy;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.hugegraph.type.define.LoadMode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.alipay.remoting.rpc.RpcServer;

/**
 * Graph interface for Gremlin operations
 */
public interface HugeGraph extends Graph, HugeGraphSupplier {

    //todo: zzz
    default List<String> mapIlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.indexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    static void registerTraversalStrategies(Class<?> clazz) {
        TraversalStrategies strategies = TraversalStrategies.GlobalCache
                .getStrategies(Graph.class)
                .clone();
        strategies.addStrategies(HugeVertexStepStrategy.instance(),
                                 HugeGraphStepStrategy.instance(),
                                 HugeCountStepStrategy.instance(),
                                 HugePrimaryKeyStrategy.instance());

        TraversalStrategies.GlobalCache.registerStrategies(clazz, strategies);
    }

    //todo: zzz
    default List<String> mapPkId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.propertyKey(id);
            names.add(schema.name());
        }
        return names;
    }

    // todo:zzz
    void serverStarted(GlobalMasterInfo nodeInfo);

    HugeGraph hugegraph();

    HugeGraph hugegraph(boolean required);

    SchemaManager schema();

    SchemaTransaction schemaTransaction();

    BackendStoreProvider storeProvider();

    Id getNextId(HugeType type);

    Id addPropertyKey(PropertyKey key);

    void updatePropertyKey(PropertyKey old, PropertyKey upadte);

    void updatePropertyKey(PropertyKey key);

    Id removePropertyKey(Id key);

    Id clearPropertyKey(PropertyKey propertyKey);

    Collection<PropertyKey> propertyKeys();

    PropertyKey propertyKey(String key);

    PropertyKey propertyKey(Id key);

    boolean existsPropertyKey(String key);

    boolean existsOlapTable(PropertyKey key);

    void addVertexLabel(VertexLabel label);

    void updateVertexLabel(VertexLabel label);

    Id removeVertexLabel(Id label);

    Collection<VertexLabel> vertexLabels();

    VertexLabel vertexLabel(String label);

    VertexLabel vertexLabel(Id label);

    VertexLabel vertexLabelOrNone(Id id);

    boolean existsVertexLabel(String label);

    boolean existsLinkLabel(Id vertexLabel);

    void addEdgeLabel(EdgeLabel label);

    void updateEdgeLabel(EdgeLabel label);

    Id removeEdgeLabel(Id label);

    Collection<EdgeLabel> edgeLabels();

    EdgeLabel edgeLabel(String label);

    EdgeLabel edgeLabel(Id label);

    EdgeLabel edgeLabelOrNone(Id label);

    boolean existsEdgeLabel(String label);

    void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel);

    void updateIndexLabel(IndexLabel label);

    Id removeIndexLabel(Id label);

    Id rebuildIndex(SchemaElement schema);

    Collection<IndexLabel> indexLabels();

    IndexLabel indexLabel(String label);

    IndexLabel indexLabel(Id id);

    boolean existsIndexLabel(String label);

    @Override
    Vertex addVertex(Object... keyValues);

    /**
     * old vertex is used for removing old vertex's index
     */
    Vertex updateVertex(Vertex oldVertex, Object... newVertex);

    /**
     * old vertex is used for removing old vertex's index
     */
    Vertex updateVertex(Vertex oldVertex, Vertex newVertex);

    void removeVertex(Vertex vertex);

    void removeVertex(String label, Object id);

    <V> void addVertexProperty(VertexProperty<V> property);

    <V> void removeVertexProperty(VertexProperty<V> property);

    Edge addEdge(Edge edge);

    /**
     * old edge is used for removing old edge's index
     */
    Edge updateEdge(Edge oldEdge, Edge newEdge);

    void canAddEdge(Edge edge);

    void removeEdge(Edge edge);

    void removeEdge(String label, Object id);

    <V> void addEdgeProperty(Property<V> property);

    <V> void removeEdgeProperty(Property<V> property);

    Vertex vertex(Object id);

    Vertex vertex(Object id, boolean skipCache);

    @Override
    Iterator<Vertex> vertices(Object... vertexIds);

    Iterator<Vertex> vertices(boolean skipCache, Object... vertexIds);

    Iterator<Vertex> vertices(Query query);

    Iterator<Vertex> adjacentVertex(Object id);

    Iterator<Vertex> adjacentVertexWithProp(Object... ids);

    boolean checkAdjacentVertexExist();

    Edge edge(Object id);

    Edge edge(Object id, boolean skipCache);

    @Override
    Iterator<Edge> edges(Object... edgeIds);

    Iterator<Edge> edges(boolean skipCache, Object... edgeIds);

    Iterator<Edge> edges(Query query);

    Iterator<Iterator<Edge>> edges(Iterator<Query> queryList);

    Iterator<EdgeId> edgeIds(Query query);

    Iterator<Iterator<EdgeId>> edgeIds(Iterator<Query> queryList);

    Iterator<Vertex> adjacentVertices(Iterator<Edge> edges);

    Iterator<Vertex> adjacentVertices(Iterator<Edge> edges, boolean withProperty);

    Iterator<Edge> adjacentEdges(Id vertexId);

    Number queryNumber(Query query);

    List<KvElement> queryAgg(Query query);

    String graphSpace();

    void graphSpace(String graphSpace);

    String name();

    String spaceGraphName();

    String backend();

    String backendVersion();

    BackendStoreSystemInfo backendStoreSystemInfo();

    BackendFeatures backendStoreFeatures();

    GraphMode mode();

    void mode(GraphMode mode);

    GraphReadMode readMode();

    void readMode(GraphReadMode readMode);

    void waitReady(RpcServer rpcServer);

    String nickname();

    void nickname(String nickname);

    String creator();

    void creator(String creator);

    Date createTime();

    void createTime(Date createTime);

    Date updateTime();

    void updateTime(Date updateTime);

    void refreshUpdateTime();

    void waitStarted();

    void serverStarted();

    boolean started();

    void started(boolean started);

    boolean closed();

    void closeTx();

    Vertex addVertex(Vertex vertex);

    <T> T metadata(HugeType type, String meta, Object... args);

    void initBackend();

    void clearBackend();

    void truncateBackend();

    void truncateGraph();

    void kvStore(KvStore kvStore);

    KvStore kvStore();

    void initSystemInfo();

    void createSnapshot();

    void resumeSnapshot();

    void create(String configPath, GlobalMasterInfo nodeInfo);

    void drop();

    HugeConfig cloneConfig(String newGraph);

    @Override
    HugeFeatures features();

    AuthManager authManager();

    RoleElectionStateMachine roleElectionStateMachine();

    void switchAuthManager(AuthManager authManager);

    TaskScheduler taskScheduler();

    RaftGroupManager raftGroupManager();

    void proxy(HugeGraph graph);

    boolean sameAs(HugeGraph graph);

    long now();

    <K, V> V option(TypedOption<K, V> option);

    void registerRpcServices(RpcServiceConfig4Server serverConfig,
                             RpcServiceConfig4Client clientConfig);

    default List<String> mapVlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.vertexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    default List<String> mapElId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.edgeLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    default List<Id> mapPkName2Id(Collection<String> pkeys) {
        List<Id> ids = new ArrayList<>(pkeys.size());
        for (String pkey : pkeys) {
            PropertyKey propertyKey = this.propertyKey(pkey);
            ids.add(propertyKey.id());
        }
        return ids;
    }

    default Id[] mapElName2Id(String[] edgeLabels) {
        Id[] ids = new Id[edgeLabels.length];
        for (int i = 0; i < edgeLabels.length; i++) {
            EdgeLabel edgeLabel = this.edgeLabel(edgeLabels[i]);
            if (edgeLabel.hasFather()) {
                ids[i] = edgeLabel.fatherId();
            } else {
                ids[i] = edgeLabel.id();
            }
        }
        return ids;
    }

    default Set<Pair<String, String>> mapPairId2Name(
            Set<Pair<Id, Id>> pairs) {
        Set<Pair<String, String>> results = new HashSet<>(pairs.size());
        for (Pair<Id, Id> pair : pairs) {
            results.add(Pair.of(this.vertexLabel(pair.getLeft()).name(),
                                this.vertexLabel(pair.getRight()).name()));
        }
        return results;
    }

    default Id[] mapVlName2Id(String[] vertexLabels) {
        Id[] ids = new Id[vertexLabels.length];
        for (int i = 0; i < vertexLabels.length; i++) {
            VertexLabel vertexLabel = this.vertexLabel(vertexLabels[i]);
            ids[i] = vertexLabel.id();
        }
        return ids;
    }

    default EdgeLabel[] mapElName2El(String[] edgeLabels) {
        EdgeLabel[] els = new EdgeLabel[edgeLabels.length];
        for (int i = 0; i < edgeLabels.length; i++) {
            els[i] = this.edgeLabel(edgeLabels[i]);
        }
        return els;
    }

    void applyMutation(BackendMutation mutation);

    // todo: zzz 有啥区别呢
    default LoadMode getLoadMode() {
        return LoadMode.NORMAL;
    }

    default void setLoadMode(LoadMode loadMode) {

    }
}
