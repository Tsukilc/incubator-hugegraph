package org.apache.hugegraph;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.id.Id;
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
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.hugegraph.util.LockUtil;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;

import com.alipay.remoting.rpc.RpcServer;

/**
 * 图别名
 */
public class HugeAliasGraph implements HugeGraph {

    private static final Logger LOG = Log.logger(HugeAliasGraph.class);

    static {
        HugeGraph.registerTraversalStrategies(HugeAliasGraph.class);

    }

    private final String name;
    private String nickname;
    private HugeGraph hugegraph;

    public HugeAliasGraph(String name, HugeGraph hugegraph) {

        this.name = name; // config.get(CoreOptions.STORE);
        this.hugegraph = hugegraph;
        this.proxy(hugegraph);
        try {
            LockUtil.init(this.spaceGraphName());
        } catch (Exception e) {
            LockUtil.destroy(this.spaceGraphName());
            LockUtil.init(this.spaceGraphName());
        }
    }

    public void changeHugeGraph(HugeGraph hugegraph) {
        this.hugegraph = hugegraph;
    }

    public HugeGraph getHugeGraph() {
        return this.hugegraph;
    }

    @Override
    public void serverStarted(GlobalMasterInfo nodeInfo) {

    }

    @Override
    public HugeGraph hugegraph() {
        return this.hugegraph;
    }

    @Override
    public HugeGraph hugegraph(boolean required) {
        return this.hugegraph.hugegraph(required);
    }

    @Override
    public SchemaManager schema() {
        SchemaManager schema = this.hugegraph.schema();
        schema.proxy(this.hugegraph);
        return schema;
    }

    @Override
    public SchemaTransaction schemaTransaction() {
        return this.hugegraph.schemaTransaction();
    }

    @Override
    public BackendStoreProvider storeProvider() {
        return this.hugegraph.storeProvider();
    }

    @Override
    public Id getNextId(HugeType type) {
        return this.hugegraph.getNextId(type);
    }

    @Override
    public Id addPropertyKey(PropertyKey key) {
        return this.hugegraph.addPropertyKey(key);
    }

    @Override
    public void updatePropertyKey(PropertyKey old, PropertyKey upadte) {
        this.hugegraph.updatePropertyKey(old, upadte);
    }

    @Override
    public void updatePropertyKey(PropertyKey key) {
        this.hugegraph.updatePropertyKey(key);
    }

    @Override
    public Id removePropertyKey(Id key) {
        return this.hugegraph.removePropertyKey(key);
    }

    @Override
    public Id clearPropertyKey(PropertyKey propertyKey) {
        return this.hugegraph.clearPropertyKey(propertyKey);
    }

    @Override
    public Collection<PropertyKey> propertyKeys() {
        return this.hugegraph.propertyKeys();
    }

    @Override
    public PropertyKey propertyKey(String key) {
        return this.hugegraph.propertyKey(key);
    }

    @Override
    public PropertyKey propertyKey(Id key) {
        return this.hugegraph.propertyKey(key);
    }

    @Override
    public boolean existsPropertyKey(String key) {
        return this.hugegraph.existsPropertyKey(key);
    }

    @Override
    public boolean existsOlapTable(PropertyKey key) {
        return this.hugegraph.existsOlapTable(key);
    }

    @Override
    public void addVertexLabel(VertexLabel vertexLabel) {
        this.hugegraph.addVertexLabel(vertexLabel);
    }

    @Override
    public void updateVertexLabel(VertexLabel label) {
        this.hugegraph.updateVertexLabel(label);
    }

    @Override
    public Id removeVertexLabel(Id label) {
        return this.hugegraph.removeVertexLabel(label);
    }

    @Override
    public Collection<VertexLabel> vertexLabels() {
        return this.hugegraph.vertexLabels();
    }

    @Override
    public VertexLabel vertexLabel(String label) {
        return this.hugegraph.vertexLabel(label);
    }

    @Override
    public VertexLabel vertexLabel(Id label) {
        return this.hugegraph.vertexLabel(label);
    }

    @Override
    public VertexLabel vertexLabelOrNone(Id id) {
        return this.hugegraph.vertexLabelOrNone(id);
    }

    @Override
    public boolean existsVertexLabel(String label) {
        return this.hugegraph.existsVertexLabel(label);
    }

    @Override
    public boolean existsLinkLabel(Id vertexLabel) {
        return this.hugegraph.existsLinkLabel(vertexLabel);
    }

    @Override
    public void addEdgeLabel(EdgeLabel edgeLabel) {
        this.hugegraph.addEdgeLabel(edgeLabel);
    }

    @Override
    public void updateEdgeLabel(EdgeLabel label) {
        this.hugegraph.updateEdgeLabel(label);
    }

    @Override
    public Id removeEdgeLabel(Id label) {
        return this.hugegraph.removeEdgeLabel(label);
    }

    @Override
    public Collection<EdgeLabel> edgeLabels() {
        return this.hugegraph.edgeLabels();
    }

    @Override
    public EdgeLabel edgeLabel(String label) {
        return this.hugegraph.edgeLabel(label);
    }

    @Override
    public EdgeLabel edgeLabel(Id label) {
        return this.hugegraph.edgeLabel(label);
    }

    @Override
    public EdgeLabel edgeLabelOrNone(Id label) {
        return this.hugegraph.edgeLabelOrNone(label);
    }

    @Override
    public boolean existsEdgeLabel(String label) {
        return this.hugegraph.existsEdgeLabel(label);
    }

    @Override
    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel) {
        this.hugegraph.addIndexLabel(schemaLabel, indexLabel);
    }

    @Override
    public void updateIndexLabel(IndexLabel label) {
        this.hugegraph.updateIndexLabel(label);
    }

    @Override
    public Id removeIndexLabel(Id label) {
        return this.hugegraph.removeIndexLabel(label);
    }

    @Override
    public Id rebuildIndex(SchemaElement schema) {
        return this.hugegraph.rebuildIndex(schema);
    }

    @Override
    public Collection<IndexLabel> indexLabels() {
        return this.hugegraph.indexLabels();
    }

    @Override
    public IndexLabel indexLabel(String label) {
        return this.hugegraph.indexLabel(label);
    }

    @Override
    public IndexLabel indexLabel(Id id) {
        return this.hugegraph.indexLabel(id);
    }

    @Override
    public boolean existsIndexLabel(String label) {
        return this.hugegraph.existsIndexLabel(label);
    }

    @Override
    public <C extends GraphComputer> C compute(
            Class<C> graphComputerClass) throws IllegalArgumentException {
        return this.hugegraph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return this.hugegraph.compute();
    }

    @Override
    public void removeVertex(Vertex vertex) {
        this.hugegraph.removeVertex(vertex);
    }

    @Override
    public void removeVertex(String label, Object id) {
        this.hugegraph.removeVertex(label, id);
    }

    @Override
    public <V> void addVertexProperty(VertexProperty<V> property) {
        this.hugegraph.addVertexProperty(property);
    }

    @Override
    public <V> void removeVertexProperty(VertexProperty<V> property) {
        this.hugegraph.removeVertexProperty(property);
    }

    @Override
    public Edge addEdge(Edge edge) {
        return this.hugegraph.addEdge(edge);
    }

    @Override
    public Edge updateEdge(Edge oldEdge, Edge newEdge) {
        return this.hugegraph.updateEdge(oldEdge, newEdge);
    }

    @Override
    public void canAddEdge(Edge edge) {
        this.hugegraph.canAddEdge(edge);
    }

    @Override
    public void removeEdge(Edge edge) {
        this.hugegraph.removeEdge(edge);
    }

    @Override
    public void removeEdge(String label, Object id) {
        this.hugegraph.removeEdge(label, id);
    }

    @Override
    public <V> void addEdgeProperty(Property<V> property) {
        this.hugegraph.addEdgeProperty(property);
    }

    @Override
    public <V> void removeEdgeProperty(Property<V> property) {
        this.hugegraph.removeEdgeProperty(property);
    }

    @Override
    public Vertex vertex(Object id) {
        return this.hugegraph.vertex(id);
    }

    @Override
    public Vertex vertex(Object id, boolean skipCache) {
        return this.hugegraph.vertex(id, skipCache);
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return this.hugegraph.vertices(vertexIds);
    }

    @Override
    public Iterator<Vertex> vertices(boolean skipCache, Object... vertexIds) {
        return this.hugegraph.vertices(skipCache, vertexIds);
    }

    @Override
    public Iterator<Vertex> vertices(org.apache.hugegraph.query.Query query) {
        return null;
    }

    //@Override
    //public Iterator<Vertex> vertices(Query query) {
    //    return this.hugegraph.vertices(query);
    //}

    @Override
    public Iterator<Vertex> adjacentVertex(Object id) {
        return this.hugegraph.adjacentVertex(id);
    }

    @Override
    public Iterator<Vertex> adjacentVertexWithProp(Object... ids) {
        return this.hugegraph.adjacentVertexWithProp(ids);
    }

    @Override
    public boolean checkAdjacentVertexExist() {
        return this.hugegraph.checkAdjacentVertexExist();
    }

    @Override
    public Edge edge(Object id) {
        return this.hugegraph.edge(id);
    }

    @Override
    public Edge edge(Object id, boolean skipCache) {
        return this.hugegraph.edge(id, skipCache);
    }

    @Override
    public Iterator<Edge> adjacentEdges(Id vertexId) {
        return this.hugegraph.adjacentEdges(vertexId);
    }

    @Override
    public Number queryNumber(org.apache.hugegraph.query.Query query) {
        return null;
    }

    @Override
    public List<KvElement> queryAgg(org.apache.hugegraph.query.Query query) {
        return List.of();
    }

    @Override
    public Transaction tx() {
        return this.hugegraph.tx();
    }

    @Override
    public void close() throws Exception {
        // pass
    }

    @Override
    public Variables variables() {
        return this.hugegraph.variables();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return this.hugegraph.edges(edgeIds);
    }

    @Override
    public Iterator<Edge> edges(boolean skipCache, Object... edgeIds) {
        return this.hugegraph.edges(skipCache, edgeIds);
    }

    @Override
    public Iterator<Edge> edges(org.apache.hugegraph.query.Query query) {
        return null;
    }

    @Override
    public Iterator<Iterator<Edge>> edges(
            Iterator<org.apache.hugegraph.query.Query> queryList) {
        return null;
    }

    @Override
    public Iterator<EdgeId> edgeIds(org.apache.hugegraph.query.Query query) {
        return null;
    }

    @Override
    public Iterator<Iterator<EdgeId>> edgeIds(
            Iterator<org.apache.hugegraph.query.Query> queryList) {
        return null;
    }

    @Override
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) {
        return this.hugegraph.adjacentVertices(edges);
    }

    @Override
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges,
                                             boolean withProperty) {
        return this.hugegraph.adjacentVertices(edges, withProperty);
    }

    @Override
    public String graphSpace() {
        return this.hugegraph.graphSpace();
    }

    @Override
    public void graphSpace(String graphSpace) {
        // pass
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public HugeConfig configuration() {
        return this.hugegraph.configuration();
    }

    @Override
    public String spaceGraphName() {
        return this.graphSpace() + "-" + this.name;
    }

    @Override
    public String backend() {
        return this.hugegraph.backend();
    }

    @Override
    public String backendVersion() {
        return this.hugegraph.backendVersion();
    }

    @Override
    public BackendStoreSystemInfo backendStoreSystemInfo() {
        return this.hugegraph.backendStoreSystemInfo();
    }

    @Override
    public BackendFeatures backendStoreFeatures() {
        return this.hugegraph.backendStoreFeatures();
    }

    @Override
    public GraphMode mode() {
        return this.hugegraph.mode();
    }

    @Override
    public void mode(GraphMode mode) {
        this.hugegraph.mode(mode);
    }

    @Override
    public GraphReadMode readMode() {
        return this.hugegraph.readMode();
    }

    @Override
    public void readMode(GraphReadMode readMode) {
        this.hugegraph.readMode(readMode);
    }

    @Override
    public void waitReady(RpcServer rpcServer) {

    }

    @Override
    public String nickname() {
        return this.nickname;
    }

    @Override
    public void nickname(String nickname) {
        this.nickname = nickname;
    }

    @Override
    public String creator() {
        return StringUtils.EMPTY;
    }

    @Override
    public void creator(String creator) {
        // pass
    }

    @Override
    public Date createTime() {
        return null;
    }

    @Override
    public void createTime(Date createTime) {
        // pass
    }

    @Override
    public Date updateTime() {
        return null;
    }

    @Override
    public void updateTime(Date updateTime) {
        // pass
    }

    @Override
    public void refreshUpdateTime() {
        this.hugegraph.refreshUpdateTime();
    }

    @Override
    public void waitStarted() {
        this.hugegraph.waitStarted();
    }

    @Override
    public void serverStarted() {
        this.hugegraph.serverStarted();
    }

    @Override
    public boolean started() {
        return false;
    }

    @Override
    public void started(boolean started) {
        // pass
    }

    @Override
    public boolean closed() {
        return false;
    }

    @Override
    public void closeTx() {
        this.hugegraph.closeTx();
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return this.hugegraph.addVertex(keyValues);
    }

    @Override
    public Vertex addVertex(Vertex vertex) {
        return this.hugegraph.addVertex(vertex);
    }

    @Override
    public Vertex updateVertex(Vertex oldVertex, Object... newVertex) {
        return this.hugegraph.updateVertex(oldVertex, newVertex);
    }

    @Override
    public Vertex updateVertex(Vertex oldVertex, Vertex newVertex) {
        return this.hugegraph.updateVertex(oldVertex, newVertex);
    }

    @Override
    public <T> T metadata(HugeType type, String meta, Object... args) {
        return this.hugegraph.metadata(type, meta, args);
    }

    @Override
    public void initBackend() {
        this.hugegraph.initBackend();
    }

    @Override
    public void clearBackend() {
        this.hugegraph.clearBackend();
    }

    @Override
    public void truncateBackend() {
        this.hugegraph.truncateBackend();
    }

    @Override
    public void truncateGraph() {
        this.hugegraph.truncateGraph();
    }

    @Override
    public HugeFeatures features() {
        return this.hugegraph.features();
    }

    @Override
    public AuthManager authManager() {
        return this.hugegraph.authManager();
    }

    @Override
    public RoleElectionStateMachine roleElectionStateMachine() {
        return null;
    }

    @Override
    public void switchAuthManager(AuthManager authManager) {
        this.hugegraph.switchAuthManager(authManager);
    }

    @Override
    public TaskScheduler taskScheduler() {
        return this.hugegraph.taskScheduler();
    }

    @Override
    public RaftGroupManager raftGroupManager() {
        return null;
    }

    @Override
    public void proxy(HugeGraph graph) {
        this.hugegraph.proxy(graph);
    }

    @Override
    public boolean sameAs(HugeGraph graph) {
        if (graph instanceof HugeAliasGraph) {
            graph = graph.hugegraph();
        }
        return this.hugegraph.sameAs(graph);
    }

    @Override
    public long now() {
        return this.hugegraph.now();
    }

    @Override
    public <K, V> V option(TypedOption<K, V> option) {
        return this.hugegraph.option(option);
    }

    @Override
    public void registerRpcServices(RpcServiceConfig4Server serverConfig,
                                    RpcServiceConfig4Client clientConfig) {

    }

    @Override
    public void kvStore(KvStore kvStore) {
        this.hugegraph.kvStore(kvStore);
    }

    @Override
    public KvStore kvStore() {
        return this.hugegraph.kvStore();
    }

    @Override
    public void initSystemInfo() {

    }

    @Override
    public void createSnapshot() {

    }

    @Override
    public void resumeSnapshot() {

    }

    @Override
    public void create(String configPath, GlobalMasterInfo nodeInfo) {

    }

    @Override
    public void drop() {

    }

    @Override
    public HugeConfig cloneConfig(String newGraph) {
        return null;
    }

    @Override
    public void applyMutation(BackendMutation mutation) {
        this.hugegraph.applyMutation(mutation);
    }

}
