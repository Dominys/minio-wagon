package io.github.dominys.minio.wagon;


import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import org.apache.maven.wagon.*;
import org.apache.maven.wagon.authentication.AuthenticationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

public class MinioWagon extends StreamWagon {
    private volatile MinioClient minioClient;
    private volatile String bucket;
    private volatile String rootDir;
    
    @Override
    protected void openConnectionInternal() throws AuthenticationException {
        if (minioClient != null) {
            return;
        }
        String user = getEnvProp("MINIO_USER", "minio.user", authenticationInfo::getUserName);
        String password = getEnvProp("MINIO_PASSWORD", "minio.password", authenticationInfo::getPassword);

        String[] connectionStrings = user.split("@");
        if (connectionStrings.length != 2) {
            throw new AuthenticationException("Invalid user format! Use: user@hostname");
        }


        this.bucket = repository.getHost();
        this.rootDir = repository.getBasedir() + (repository.getBasedir().endsWith("/") ? "" : "/");

        minioClient = MinioClient.builder()
                .endpoint(connectionStrings[1])
                .credentials(connectionStrings[0], password)
                .build();
    }

    private String getEnvProp(String envVarName, String propertyKey, Supplier<String> authenticationAccessor) {
        String value = System.getenv(envVarName);
        if (value != null) {
            return value;
        }
        value = System.getProperty(propertyKey);
        if (value != null) {
            return value;
        }
        return authenticationAccessor.get();
    }

    @Override
    public void fillInputData(InputData inputData) throws TransferFailedException, ResourceDoesNotExistException {
        try {
            inputData.setInputStream(minioClient.getObject(GetObjectArgs.builder()
                    .bucket(this.bucket)
                    .object(this.rootDir + inputData.getResource().getName())
                    .build()));
        } catch (ErrorResponseException e) {
            if (e.errorResponse().code().equals("NoSuchKey")) {
                throw new ResourceDoesNotExistException(e.getMessage(), e);
            }
            throw new TransferFailedException(e.getMessage(), e);
        } catch (Exception e) {
            throw new TransferFailedException(e.getMessage(), e);
        }
    }

    @Override
    public void fillOutputData(OutputData outputData) {
        outputData.setOutputStream(new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                try {
                    InputStream stream = new ByteArrayInputStream(toByteArray());
                    minioClient.putObject(PutObjectArgs.builder()
                                    .bucket(bucket)
                                    .object(rootDir + outputData.getResource().getName())
                                    .stream(stream, stream.available(), -1)
                            .build());
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        });
    }

    @Override
    public void closeConnection() {
        minioClient = null;
    }
}
