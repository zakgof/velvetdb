package com.zakgof.db.velvet.dynamodb;

import java.net.URI;
import java.util.Collections;
import java.util.Map;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.common.base.Splitter;
import com.zakgof.db.txn.ITransactionCall;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.impl.AVelvetEnvironment;

public class DynamoDBVelvetEnv extends AVelvetEnvironment {

    private final DynamoDB db;
    private long readUnits = 0;
    private long writeUnits = 0;
    
    public DynamoDBVelvetEnv(URI uri) {
        String region = uri.getPath().replaceAll("/", "");
        String query = uri.getQuery();
        final Map<String, String> params = query == null ? Collections.emptyMap() : Splitter.on('&').trimResults().withKeyValueSeparator("=").split(query);

        String awsAccessKeyId = params.get("awsAccessKeyId");
        String awsSecretKey = params.get("awsSecretKey");

        ClientConfiguration clientConfig = new ClientConfiguration();
        String proxyHost = params.get("proxyHost");
        if (proxyHost != null) {
            String proxyPort = params.getOrDefault("proxyPort", "80");
            int port = proxyPort.matches("\\d+") ? Integer.parseInt(proxyPort) : 80;

            clientConfig.withProxyHost(proxyHost).withProxyPort(port);
            String proxyUser = params.get("proxyUser");
            if (proxyUser != null) {
                clientConfig.withProxyUsername(proxyUser);
            }
            String proxyPassword = params.get("proxyPassword");
            if (proxyPassword != null) {
                clientConfig.withProxyPassword(proxyPassword);
            }
        }
        clientConfig.withMaxErrorRetry(24);

        AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard()
           .withClientConfiguration(clientConfig)
           .withRegion(region);

        if (awsAccessKeyId != null) {
            builder = builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKeyId, awsSecretKey)));
        }
        
        readUnits = parseLong(params.get("readUnits"), 0L);
        writeUnits = parseLong(params.get("writeUnits"), 0L);

        this.db = new DynamoDB(builder.build());
    }

    private long parseLong(String str, long def) {
		if (str != null) {
			try {
				return Long.parseLong(str);
			} catch (NumberFormatException e) {
				throw new VelvetException("Invalid param: expected long, found : " + str);
			}
		}
		return def;
	}

	public DynamoDBVelvetEnv(AmazonDynamoDB amazonDynamoDB) {
        this(new DynamoDB(amazonDynamoDB));
    }

    public DynamoDBVelvetEnv(DynamoDB dynamoDB) {
        this.db = dynamoDB;
    }

    @Override
    public void execute(ITransactionCall<IVelvet> transaction) {
        try {
            transaction.execute(new DynamoDBVelvet(db, this::instantiateSerializer, readUnits, writeUnits));
        } catch (Throwable e) {
            if (e instanceof RuntimeException)
                throw (RuntimeException)e;
            else if (e instanceof Error)
                throw (Error)e;
            throw new VelvetException(e);
        }
    }

    @Override
    public void close() {
        db.shutdown();
    }
}
