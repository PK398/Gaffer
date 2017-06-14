/*
 * Copyright 2017. Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.parquetstore;

import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler.AddElementsFromRDDHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetAdjacentIdsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetDataframeOfElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.spark.SparkUser;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.SerialisationFactory;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;

public class ParquetStore extends Store {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetStore.class);
    private static final Set<StoreTrait> TRAITS =
            Collections.unmodifiableSet(Sets.newHashSet(
                    ORDERED,
                    INGEST_AGGREGATION,
                    PRE_AGGREGATION_FILTERING
            ));
    private Map<String, List<Tuple3<Object[], Object[], String>>> groupToIndex;
    private SchemaUtils schemaUtils;
    private FileSystem fs;
    private long currentSnapshot;

    @Override
    protected SchemaOptimiser createSchemaOptimiser() {
        return new SchemaOptimiser(new SerialisationFactory(ParquetStoreConstants.SERIALISERS));
    }

    @Override
    public void initialise(final Schema schema, final StoreProperties properties) throws StoreException {
        super.initialise(schema, properties);
        try {
            this.fs = FileSystem.get(new Configuration());
        } catch (IOException e) {
            throw new StoreException("Could not connect to the file system", e);
        }
        this.schemaUtils = new SchemaUtils(this.getSchema());
        loadIndices();
    }

    public FileSystem getFS() throws StoreException {
        return this.fs;
    }

    public SchemaUtils getSchemaUtils() {
        return this.schemaUtils;
    }

    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    public boolean isValidationRequired() {
        return false;
    }

    @Override
    protected Context createContext(final User user) {
        final SparkUser sparkUser;
        if (user instanceof SparkUser) {
            sparkUser = (SparkUser) user;
            final SparkConf conf = sparkUser.getSparkSession().sparkContext().getConf();
            final String sparkSerialiser = conf.get("spark.serializer", null);
            if (sparkSerialiser == null || !"org.apache.spark.serializer.KryoSerializer".equals(sparkSerialiser)) {
                LOGGER.warn("For the best performance you should set the spark config 'spark.serializer' = 'org.apache.spark.serializer.KryoSerializer'");
            }
            final String sparkKryoRegistrator = conf.get("spark.kryo.registrator", null);
            if (sparkKryoRegistrator == null || !"uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator".equals(sparkKryoRegistrator)) {
                LOGGER.warn("For the best performance you should set the spark config 'spark.kryo.registrator' = 'uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator'");
            }

        } else {
            LOGGER.info("Setting up the Spark session using " + this.getProperties().getSparkMaster() + " as the master URL");
            final SparkSession spark = SparkSession.builder()
                    .appName("Gaffer Parquet Store")
                    .master(this.getProperties().getSparkMaster())
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.kryo.registrator", "uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator")
                    .getOrCreate();
            sparkUser = new SparkUser(user, spark);
        }
        return super.createContext(sparkUser);
    }

    protected void addAdditionalOperationHandlers() {
        addOperationHandler(GetDataFrameOfElements.class, new GetDataframeOfElementsHandler());
        addOperationHandler(ImportRDDOfElements.class, new AddElementsFromRDDHandler());
    }

    @Override
    protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected GetAllElementsHandler getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected GetAdjacentIdsHandler getAdjacentIdsHandler() {
        return new GetAdjacentIdsHandler();
    }

    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return Serialiser.class;
    }

    @Override
    protected Object doUnhandledOperation(final Operation operation, final Context context) {
        throw new UnsupportedOperationException("Operation " + operation.getClass() + " is not supported by the ParquetStore.");
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be ParquetStoreProperties")
    @Override
    public ParquetStoreProperties getProperties() {
        return (ParquetStoreProperties) super.getProperties();
    }

    public void loadIndices() throws StoreException {
        try {
            final Map<String, List<Tuple3<Object[], Object[], String>>> newGroupToIndex = new HashMap<>();
            final FileSystem fs = getFS();
            final String rootDirString = this.getProperties().getDataDir();
            final Path rootDir = new Path(rootDirString);
            if (fs.exists(rootDir)) {
                final FileStatus[] snapshots = fs.listStatus(rootDir);
                long latestSnapshot = getCurrentSnapshot();
                for (final FileStatus fileStatus : snapshots) {
                    final String snapshot = fileStatus.getPath().getName();
                    long currentSnapshot = Long.parseLong(snapshot);
                    if (latestSnapshot < currentSnapshot) {
                        latestSnapshot = currentSnapshot;
                    }
                }
                if (latestSnapshot != 0L) {
                    for (final String group : this.schemaUtils.getEntityGroups()) {
                        final List<Tuple3<Object[], Object[], String>> index = loadIndex(group, ParquetStoreConstants.VERTEX, latestSnapshot);
                        if (!index.isEmpty()) {
                            newGroupToIndex.put(group, index);
                        }
                    }
                    for (final String group : this.schemaUtils.getEdgeGroups()) {
                        final List<Tuple3<Object[], Object[], String>> index = loadIndex(group, ParquetStoreConstants.SOURCE, latestSnapshot);
                        if (!index.isEmpty()) {
                            newGroupToIndex.put(group, index);
                        }
                        final List<Tuple3<Object[], Object[], String>> reverseIndex = loadIndex(group, ParquetStoreConstants.DESTINATION, latestSnapshot);
                        if (!reverseIndex.isEmpty()) {
                            newGroupToIndex.put(group + "_reversed", reverseIndex);
                        }
                    }
                }
                setCurrentSnapshot(latestSnapshot);
            }
            this.groupToIndex = newGroupToIndex;
        } catch (IOException e) {
            throw new StoreException("Failed to connect to the file system", e);
        }
    }

    private List<Tuple3<Object[], Object[], String>> loadIndex(final String group, final String identifier, final long currentSnapshot) throws StoreException {
        try {
            final String indexDir;
            final Path path;
            if (ParquetStoreConstants.VERTEX.equals(identifier)) {
                indexDir = getProperties().getDataDir() + "/" + currentSnapshot + "/graph/" + ParquetStoreConstants.GROUP + "=" + group + "/";
                path = new Path(indexDir + "_index");
            } else if (ParquetStoreConstants.SOURCE.equals(identifier)) {
                indexDir = getProperties().getDataDir() + "/" + currentSnapshot + "/graph/" + ParquetStoreConstants.GROUP + "=" + group + "/";
                path = new Path(indexDir + "_index");
            } else {
                indexDir = getProperties().getDataDir() + "/" + currentSnapshot + "/reverseEdges/" + ParquetStoreConstants.GROUP + "=" + group + "/";
                path = new Path(indexDir + "_index");
            }
            LOGGER.info("Loading the index from path {}", path);
            final ArrayList<Tuple3<Object[], Object[], String>> index = new ArrayList<>();
            final FileSystem fs = getFS();
            if (fs.exists(path)) {
                final FSDataInputStream reader = fs.open(path);
                while (reader.available() > 0) {
                    final int numOfCols = reader.readInt();
                    final Object[] minObjects = new Object[numOfCols];
                    final Object[] maxObjects = new Object[numOfCols];
                    for (int i = 0; i < numOfCols; i++) {
                        final int colTypeLength = reader.readInt();
                        final byte[] colType = readBytes(colTypeLength, reader);
                        final int minLength = reader.readInt();
                        final byte[] min = readBytes(minLength, reader);
                        minObjects[i] = deserialiseColumn(colType, min);
                        final int maxLength = reader.readInt();
                        final byte[] max = readBytes(maxLength, reader);
                        maxObjects[i] = deserialiseColumn(colType, max);
                    }
                    final int filePathLength = reader.readInt();
                    final byte[] filePath = readBytes(filePathLength, reader);
                    final String fileString = StringUtil.toString(filePath);
                    index.add(new Tuple3<>(minObjects, maxObjects, indexDir + fileString));
                }
                index.sort(Comparator.comparing(Tuple3::_3));
            }
            return index;
        } catch (IOException e) {
            if (ParquetStoreConstants.DESTINATION.equals(identifier)) {
                throw new StoreException("IO Exception while loading the index from " + getProperties().getDataDir() +
                        "/" + getCurrentSnapshot() + "/reverseEdges/" + ParquetStoreConstants.GROUP + "=" + group + "/_index", e);
            } else {
                throw new StoreException("IO Exception while loading the index from " + getProperties().getDataDir() +
                        "/" + getCurrentSnapshot() + "/graph/" + ParquetStoreConstants.GROUP + "=" + group + "/_index", e);
            }
        }
    }

    private byte[] readBytes(final int length, final FSDataInputStream reader) throws IOException {
        final byte[] bytes = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length && bytesRead > -1) {
            bytesRead += reader.read(bytes, bytesRead, length - bytesRead);
        }
        return bytes;
    }

    private Object deserialiseColumn(final byte[] colType, final byte[] value) {
        final String colTypeName = StringUtil.toString(colType);
        if ("long".equals(colTypeName)) {
            return BytesUtils.bytesToLong(value);
        } else if ("int".equals(colTypeName)) {
            return BytesUtils.bytesToInt(value);
        } else if ("boolean".equals(colTypeName)) {
            return BytesUtils.bytesToBool(value);
        } else if ("float".equals(colTypeName)) {
            return Float.intBitsToFloat(BytesUtils.bytesToInt(value));
        } else if (colTypeName.endsWith(" (UTF8)")) {
            return Binary.fromReusedByteArray(value).toStringUsingUTF8();
        } else {
            return value;
        }
    }

    public Map<String, List<Tuple3<Object[], Object[], String>>> getIndex() {
        return this.groupToIndex;
    }

    public void setCurrentSnapshot(final long timestamp) {
        this.currentSnapshot = timestamp;
    }

    public long getCurrentSnapshot() {
        return this.currentSnapshot;
    }
}