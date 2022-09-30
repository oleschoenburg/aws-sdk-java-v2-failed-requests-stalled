import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;
import com.github.dockerjava.api.model.TmpfsOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

@Testcontainers
class AsyncRequestsStalledTest {
    public static final String ACCESS_KEY = "letmein";
    public static final String SECRET_KEY = "letmein1234";
    public static final int DEFAULT_PORT = 9000;

    @Container
    private static final GenericContainer<?> S3 = buildMinioContainer();

    private S3AsyncClient client;
    private String bucketName;

    @Test
    void concurrentRequestsAreStalledAfterFailures() throws IOException {
        // given -- a 64MiB test file
        final var tmpDir = Files.createTempDirectory("test-files");
        final var largeFile = Files.createFile(tmpDir.resolve("large-file"));
        Files.write(largeFile, RandomUtils.nextBytes(64 * 1024 * 1024));

        // when -- 20 putRequest 20 of the test file, some requests will fail because the bucket is full
        final var results =
                IntStream.range(0, 20)
                        .boxed()
                        .map(i -> client.putObject(
                                cfg -> cfg.bucket(bucketName).key("object-" + i),
                                AsyncRequestBody.fromFile(largeFile)))
                        .toArray(CompletableFuture[]::new);

        //then -- this should complete exceptionally
        CompletableFuture.allOf(results).join();
    }

    @BeforeEach
    void setup() {
        client = S3AsyncClient.builder()
                .endpointOverride(
                        URI.create(
                                "http://%s:%d".formatted(S3.getHost(), S3.getMappedPort(DEFAULT_PORT))))
                .region(Region.US_EAST_1)
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .build();

        bucketName = RandomStringUtils.randomAlphanumeric(12).toLowerCase();
        client.createBucket(req -> req.bucket(bucketName)).join();
    }

    private static GenericContainer<?> buildMinioContainer() {
        return new GenericContainer<>(DockerImageName.parse("quay.io/minio/minio"))
                .withCreateContainerCmdModifier(
                        cmd ->
                                cmd.withHostConfig(
                                        Objects.requireNonNull(cmd.getHostConfig())
                                                .withMounts(
                                                        List.of(
                                                                new Mount()
                                                                        .withTarget("/data")
                                                                        .withType(MountType.TMPFS)
                                                                        .withTmpfsOptions(new TmpfsOptions().withSizeBytes(1024 * 1024 * 1024L))))))
                .withCommand("server /data")
                .withExposedPorts(DEFAULT_PORT)
                .withEnv("MINIO_ACCESS_KEY", ACCESS_KEY)
                .withEnv("MINIO_SECRET_KEY", SECRET_KEY)
                .waitingFor(
                        new HttpWaitStrategy()
                                .forPath("/minio/health/ready")
                                .forPort(DEFAULT_PORT)
                                .withStartupTimeout(Duration.ofMinutes(1)));
    }
}
