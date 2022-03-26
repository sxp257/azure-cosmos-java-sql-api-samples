package com.azure.cosmos.examples.queries.sync;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.OrderHeader;
import com.azure.cosmos.examples.common.OrderLine;

import com.azure.cosmos.models.CosmosQueryRequestOptions;

import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueriesQuickStartOrderLine {

    private CosmosClient client;

    private final String databaseName = "iom";
    private final String headercontainerName = "order_hdr";
    private final String linecontainerName = "order_line";

    private CosmosDatabase database;
    private CosmosContainer hdrContainer;
    private CosmosContainer lineContainer;

    protected static Logger logger = LoggerFactory.getLogger(QueriesQuickStartOrderLine.class);
    private Order order ;

    public void close() {
        client.close();
    }

    public static void main(String[] args) {
        QueriesQuickStartOrderLine p = new QueriesQuickStartOrderLine();
        try {
            logger.info("Starting SYNC main");
            p.queriesDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    private void queriesDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        // Create sync client
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildClient();

        getDatabaseIfExists();
        getHeaderContainerIfExists();
        getLineContainerIfExists();

        // createDocument();
        order = new Order();

        queryAllDocuments();

    }

    private void queryAllDocuments() throws Exception {
        logger.info("Query all documents.");

        executeQueryOrderHeaderPrintSingleResult("SELECT * FROM c");
        //executeQueryOrderLinePrintSingleResult("SELECT * FROM c");

    }

    private void executeQueryOrderHeaderPrintSingleResult(String sql) throws Exception {
        logger.info("Execute query {}", sql);

        CosmosPagedIterable<OrderHeader> filteredOrderHeaders = hdrContainer.queryItems(sql,
                new CosmosQueryRequestOptions(), OrderHeader.class);

        // Print
        if (filteredOrderHeaders.iterator().hasNext()) {
            OrderHeader orderHeader = filteredOrderHeaders.iterator().next();
            //Set Header
            order.setOrderHeader(orderHeader);
            
            //Gson gsonHeader = new GsonBuilder().setPrettyPrinting().create();
            //String jsonHeader = gsonHeader.toJson(order);
            //logger.info(String.format("First query result: filteredOrderHeader with (/id, payload) = (%s,%s)",
            //        orderHeader.getId(), jsonHeader));
              queryEquality(orderHeader.getOrder_id());
        }

        logger.info("Done.");
    }

    private void executeQueryOrderLinePrintSingleResult(String sql) {
        logger.info("Execute query {}", sql);

        CosmosPagedIterable<OrderLine> filteredOrderLines = lineContainer.queryItems(sql,
                new CosmosQueryRequestOptions(), OrderLine.class);
        //Set Lines
        filteredOrderLines.forEach(line -> order.setLines(line));
        Gson gsonLine = new GsonBuilder().setPrettyPrinting().create();
        String jsonLine = gsonLine.toJson(order);
        logger.info(String.format("First query result: filteredOrderLine with (/id) = (%s, %s)",
                order.getOrderHeader().getId(), jsonLine));

        logger.info("Done.");
    }

    private void queryEquality(String id) throws Exception {
        logger.info("Query for equality using =");
        executeQueryOrderLinePrintSingleResult("SELECT * FROM c WHERE c.order_id = '" + id + "'");
    }

    private void getHeaderContainerIfExists() throws Exception {
        logger.info("Get container " + headercontainerName + " if  exists.");
        hdrContainer = database.getContainer(headercontainerName);
        logger.info("Done.");
    }

    private void getLineContainerIfExists() throws Exception {
        logger.info("Get container " + linecontainerName + " if  exists.");
        lineContainer = database.getContainer(linecontainerName);
        logger.info("Done.");
    }

    // Database Create
    private void getDatabaseIfExists() throws Exception {
        logger.info("Get database " + databaseName + " if  exists...");
        // Get database if exists

        database = client.getDatabase(databaseName);
        logger.info("Done.");
    }

    private void shutdown() {
        try {
            client.close();
        } catch (Exception err) {
            logger.error(
                    "Deleting Cosmos DB resources failed, will still attempt to close the client. See stack trace below.");
            err.printStackTrace();
        }

        logger.info("Done with sample.");
    }

}
