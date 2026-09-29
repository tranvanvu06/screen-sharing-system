package pl.polsl.screensharing.host.net;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.imgscalr.Scalr;
import pl.polsl.screensharing.host.controller.VideoCanvasController;
import pl.polsl.screensharing.host.state.HostState;
import pl.polsl.screensharing.host.state.QualityLevel;
import pl.polsl.screensharing.host.state.StreamingState;
import pl.polsl.screensharing.host.view.HostWindow;
import pl.polsl.screensharing.lib.UnoperableException;
import pl.polsl.screensharing.lib.net.AbstractDatagramSocketThread;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import javax.swing.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.PortUnreachableException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static pl.polsl.screensharing.lib.SharedConstants.*;

@Slf4j
public class ServerDatagramSocket extends AbstractDatagramSocketThread {
    private final HostWindow hostWindow;
    @Getter
    private final HostState hostState;
    private final VideoCanvasController videoCanvasController;
    @Getter
    private final BlockingQueue<byte[]> sendPackagesQueue;
    private final FrameSenderThread frameSenderThread;

    private QualityLevel qualityLevel;
    private boolean isShowing;

    public ServerDatagramSocket(HostWindow hostWindow, VideoCanvasController videoCanvasController) {
        super();
        this.hostWindow = hostWindow;
        hostState = hostWindow.getHostState();
        this.videoCanvasController = videoCanvasController;
        qualityLevel = QualityLevel.GOOD;
        sendPackagesQueue = new ArrayBlockingQueue<>(100);
        frameSenderThread = new FrameSenderThread(this);
        initObservables();
    }

    @Override
    public void run() {
        byte[] chunk;
        int chunkOffset = 0;
        byte[] compressedData = null;
        int unprocessedDataLength = 0;
        byte countOfPackages = 0;
        byte packageIteration = 1;
        final int debugBytesLength = 2;
        final int lengthWithoutIV = PACKAGE_SIZE - debugBytesLength;

        long lastTime = System.nanoTime();
        long currentTime;
        long timer = 0, logTimer = 0;
        long sentBytes = 0;

        //Luồng (thread) này chạy lặp liên tục miễn là phiên UDP vẫn còn tồn tại.
        // Mỗi vòng lặp sẽ gửi lần lượt các phần tiếp theo của một khung hình dưới dạng các gói tin có kích thước N + 2 byte.
        // Hai byte bổ sung dùng cho mục đích kiểm tra/gỡ lỗi,
        // bao gồm thông tin về số lượng phân mảnh của một khung hình và chỉ số của phân mảnh hiện tại.
        // Các giá trị này cần thiết để thực hiện việc kiểm tra và sửa lỗi ở phía nhận.

        log.info("Started datagram thread with TID {}", getName());
        while (isThreadActive) {
            currentTime = System.nanoTime();
            timer += (currentTime - lastTime);
            logTimer += (currentTime - lastTime);
            lastTime = currentTime;
            try {
                if (compressedData == null) {
                    compressedData = loadImage();
                    unprocessedDataLength = compressedData.length;
                    countOfPackages = (byte) Math.ceil((double) compressedData.length / lengthWithoutIV);
                }
                // Gửi liên tục các gói tin khi số byte dữ liệu còn lại vẫn lớn hơn kích thước dữ liệu tối đa của một gói,
                // không tính các byte debug.
                if (unprocessedDataLength > lengthWithoutIV) {

                    chunk = new byte[FRAME_SIZE];
                    chunk[0] = countOfPackages;
                    chunk[1] = packageIteration;


                    System.arraycopy(compressedData, chunkOffset, chunk, debugBytesLength, lengthWithoutIV);
                    sendPackagesQueue.put(chunk);

                    sentBytes += FRAME_SIZE;
                    unprocessedDataLength -= lengthWithoutIV;
                    chunkOffset += lengthWithoutIV;
                    packageIteration++;
                } else {

                    chunk = new byte[unprocessedDataLength + debugBytesLength + IV_SIZE];
                    chunk[0] = countOfPackages;
                    chunk[1] = packageIteration;

                    System.arraycopy(compressedData, chunkOffset, chunk, debugBytesLength, unprocessedDataLength);
                    sendPackagesQueue.put(chunk);

                    sentBytes += (unprocessedDataLength + debugBytesLength + IV_SIZE);
                    compressedData = null;
                    chunkOffset = 0;
                    packageIteration = 1;
                }
                sleep(1);
            } catch (SocketTimeoutException | PortUnreachableException ex) {
                JOptionPane.showMessageDialog(hostWindow, ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
                log.error("Unexpected network error. Cause: {}", ex.getMessage());
                break;
            } catch (Exception ignored) {
            }
            if (logTimer >= BILION * 6L) {
                if (sentBytes > 0) {
                    log.info("Host datagram socket processed {} bytes", sentBytes);
                }
                logTimer = 0;
                System.gc();
            }
            if (timer >= BILION) {
                if (isShowing) {
                    hostState.updateSentBytesPerSec(sentBytes);
                }
                log.debug("Host datagram socket processed {} bytes", sentBytes);
                sentBytes = 0;
                timer = 0;
            }
        }
        stopAndClear();
    }

    @Override
    public void createDatagramSocket(byte[] secretKey, int port) {
        try {
            cryptoSymmetricHelper.init(secretKey);
            datagramSocket = new DatagramSocket();
        } catch (Exception ex) {
            throw new UnoperableException(ex);
        }
    }

    @Override
    public void abstractStopAndClear() {
        hostState.updateStreamingState(StreamingState.STOPPED);
        hostState.updateRealFpsBuffer(0);
        sendPackagesQueue.clear();
        System.gc();
    }

    @Override
    protected void postStart() {
        frameSenderThread.start();
    }

    private byte[] loadImage() throws IOException {
        final ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        final ImageOutputStream outputStream = ImageIO.createImageOutputStream(compressed);
        BufferedImage rawImage = videoCanvasController.getRawImage();

        final ImageWriter jpgWriter = ImageIO.getImageWritersByFormatName("JPEG").next();
        final ImageWriteParam jpgWriteParam = jpgWriter.getDefaultWriteParam();
        jpgWriteParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
        jpgWriteParam.setCompressionQuality(qualityLevel.getJpegLevel());
        jpgWriter.setOutput(outputStream);
        if (rawImage.getWidth() > MAX_FRAME_WIDTH || rawImage.getHeight() > MAX_FRAME_HEIGHT) {
            final int newHeight = (int) ((double) MAX_FRAME_WIDTH / rawImage.getWidth() * rawImage.getHeight());
            rawImage = Scalr.resize(rawImage, MAX_FRAME_WIDTH, newHeight);
        }
        jpgWriter.write(null, new IIOImage(rawImage, null, null), jpgWriteParam);

        final byte[] compressedData = compressed.toByteArray();

        jpgWriter.dispose();
        if (outputStream != null) {
            outputStream.close();
        }
        compressed.close();
        return compressedData;
    }

    @Override
    protected void initObservables() {
        hostState.wrapAsDisposable(hostState.getStreamingQualityLevel$(), qualityLevel -> {
            this.qualityLevel = qualityLevel;
        });
        hostState.wrapAsDisposable(hostState.isScreenIsShowForParticipants$(), isShowing -> {
            this.isShowing = isShowing;
        });
    }
}
