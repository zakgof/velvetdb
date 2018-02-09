package com.zakgof.db.velvet.datastore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.URI;
import java.util.Map;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.DatastoreOptions.Builder;
import com.google.cloud.http.HttpTransportOptions;
import com.google.common.base.Splitter;
import com.zakgof.db.txn.ITransactionCall;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.impl.AVelvetEnvironment;

public class DatastoreVelvetEnv extends AVelvetEnvironment {

    private Datastore datastore;

    public DatastoreVelvetEnv(URI uri) {
        String projectId = uri.getPath().replaceAll("/", "");
        String query = uri.getQuery();
        final Map<String, String> params = Splitter.on('&').trimResults().withKeyValueSeparator("=").split(query);

        String credentialPath = params.get("credentialPath");
        String proxyHost = params.get("proxyHost");

        Proxy proxy = null;
        if (proxyHost != null) {

            String proxyPort = params.getOrDefault("proxyPort", "80");
            int port = proxyPort.matches("\\d+") ? Integer.parseInt(proxyPort) : 80;
            proxy = new Proxy(Type.HTTP, new InetSocketAddress(proxyHost, port));

            // TODO: too global
            String proxyUser = params.get("proxyUser");
            if (proxyUser != null) {
                String proxyPassword = params.getOrDefault("proxyPassword", "");
                Authenticator authenticator = new Authenticator() {
                    @Override
                    public PasswordAuthentication getPasswordAuthentication() {
                        return (new PasswordAuthentication(proxyUser, proxyPassword.toCharArray()));
                    }
                };
                Authenticator.setDefault(authenticator);
            }
        }
        File credentialFile = credentialPath == null ? null : new File(credentialPath);
        datastore = openDatastore(projectId, credentialFile, proxy);
    }

    @Override
    public void execute(ITransactionCall<IVelvet> transaction) {
        try {
            transaction.execute(new DataStoreVelvet(datastore, this::instantiateSerializer));
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
    }

    private Datastore openDatastore(String projectId, File credentialFile, Proxy proxy) {
        try {

            HttpTransportFactory tf = () -> new NetHttpTransport.Builder().setProxy(proxy).build();
            HttpTransportOptions transportOptions = HttpTransportOptions.newBuilder().setHttpTransportFactory(tf).build();

            Builder builder = DatastoreOptions.newBuilder().setTransportOptions(transportOptions).setProjectId(projectId);

            if (credentialFile != null) {
                FileInputStream serviceAccountStream = new FileInputStream(credentialFile);
                GoogleCredentials credentials = ServiceAccountCredentials.fromStream(serviceAccountStream, tf);
                builder.setCredentials(credentials);
            }

            Datastore datastore = builder.build().getService();
            return datastore;

        } catch (IOException e) {
            throw new VelvetException(e);
        }
    }
}
