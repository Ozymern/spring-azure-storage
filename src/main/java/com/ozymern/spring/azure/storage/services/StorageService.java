package com.ozymern.spring.azure.storage.services;


import java.io.BufferedReader;
import java.io.File;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.blob.BlobRange;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.ListBlobsOptions;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.TransferManager;
import com.microsoft.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.storage.blob.models.ContainerCreateResponse;
import com.microsoft.azure.storage.blob.models.ContainerListBlobFlatSegmentResponse;
import com.microsoft.rest.v2.RestException;
import com.microsoft.rest.v2.util.FlowableUtil;

import io.reactivex.*;
import io.reactivex.Flowable;
import lombok.Data;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import sun.misc.BASE64Decoder;

@Component
@Data
public class StorageService {

    @Value("${azure-storage.account-name}")
    private String accountName;

    @Value("${azure-storage.account-key}")
    private String accountKey;


    @Value("${azure-storage.container-name}")
    private String containerName;

    @Value("${azure-storage.service-URL}")
    private String storageURL;

    private SharedKeyCredentials creds;

    private ServiceURL serviceURL;

    private ContainerURL containerURL;

    private BlockBlobURL blobURL;

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageService.class);

    public StorageService() {


    }

    public void init()throws InvalidKeyException, MalformedURLException {

        creds = new SharedKeyCredentials(this.accountName,this.accountKey);
        serviceURL = new ServiceURL(new URL(this.storageURL), StorageURL.createPipeline(creds, new PipelineOptions()));
        containerURL = serviceURL.createContainerURL(this.containerName);

    }

    private BlockBlobURL createBlockBlobURL(String name){
        return  containerURL.createBlockBlobURL(name);
    }

    public void uploadFile(String nameBlob, File sourceFile) throws IOException {
        final BlockBlobURL blobURL = this.createBlockBlobURL(nameBlob);
        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(sourceFile.toPath());
        TransferManager.uploadFileToBlockBlob(fileChannel, blobURL, 8 * 1024 * 1024, null, null)
            .subscribe(response -> {
                LOGGER.info("Completed upload request.");
                LOGGER.info(String.valueOf(response.response().statusCode()));
            });
    }
    public void uploadPdfBse64(String nameBlob,String data) throws Exception {
        final BlockBlobURL blobURL = this.createBlockBlobURL(nameBlob);
        BASE64Decoder decoder = new BASE64Decoder();
        byte[] decodedBytes = decoder.decodeBuffer(data);
        blobURL.upload(Flowable.just(ByteBuffer.wrap(decodedBytes)), decodedBytes.length)
            .blockingGet();

        LOGGER.info("Finished uploading text");

    }
    public void listBlobs(ContainerURL containerURL) {
        ListBlobsOptions options = new ListBlobsOptions();
        options.withMaxResults(10);

        containerURL.listBlobsFlatSegment(null, options, null).flatMap(containerListBlobFlatSegmentResponse ->
            listAllBlobs(containerURL, containerListBlobFlatSegmentResponse))
            .subscribe(response -> {
                LOGGER.info("Completed list blobs request.");
                LOGGER.info(String.valueOf(response.statusCode()));
            });
    }

    public  Single<ContainerListBlobFlatSegmentResponse> listAllBlobs(ContainerURL url, ContainerListBlobFlatSegmentResponse response) {
        if (response.body().segment() != null) {
            for (BlobItem b : response.body().segment().blobItems()) {
                String output = "Blob name: " + b.name();
                if (b.snapshot() != null) {
                    output += ", Snapshot: " + b.snapshot();
                }
                LOGGER.info(output);
            }
        } else {
            LOGGER.info("There are no more blobs to list off.");
        }

        if (response.body().nextMarker() == null) {
            return Single.just(response);
        } else {
            String nextMarker = response.body().nextMarker();


            return url.listBlobsFlatSegment(nextMarker, new ListBlobsOptions().withMaxResults(10), null)
                .flatMap(containersListBlobFlatSegmentResponse ->
                    listAllBlobs(url, containersListBlobFlatSegmentResponse));
        }
    }

    public void deleteBlob(String  nameBlob) {

        final BlockBlobURL blobURL = this.createBlockBlobURL(nameBlob);
        // Delete the blob
        blobURL.delete(null, null, null)
            .subscribe(
                response -> LOGGER.info(">> Blob deleted: " + blobURL),
                error -> LOGGER.error(">> An error encountered during deleteBlob: " + error.getMessage()));
    }

    public void getBlob(String nameBlob, File sourceFile) throws IOException {

        final BlockBlobURL blobURL = this.createBlockBlobURL(nameBlob);
        LOGGER.info(sourceFile.getName());
        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(sourceFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        LOGGER.info(sourceFile.getName());
        TransferManager.downloadBlobToFile(fileChannel, blobURL, null, null)
            .subscribe(response -> {
                LOGGER.info("Completed download request.");
                LOGGER.info("The blob was downloaded to " + sourceFile.getAbsolutePath());
            });
    }

    public  void downloadBlob(BlockBlobURL blockBlobURL, File downloadToFile) {


        LOGGER.info("Start downloading file %s to %s..." + blockBlobURL.toURL() + downloadToFile);
        FileUtils.deleteQuietly(downloadToFile);

        blockBlobURL.download(new BlobRange().withOffset(0).withCount(4 * 1024 * 1024L), null, false, null)
            .flatMapCompletable(
                response -> {
                    final AsynchronousFileChannel channel = AsynchronousFileChannel
                        .open(Paths.get(downloadToFile.getAbsolutePath()), StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE);
                    return FlowableUtil.writeFile(response.body(null), channel);
                })
            .doOnComplete(() -> LOGGER.info("File is downloaded to %s.", downloadToFile))
            .doOnError(error -> LOGGER.error("Failed to download file from blob %s with error %s.",
                blockBlobURL.toURL(), error.getMessage()));

    }
}
