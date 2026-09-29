package pl.polsl.screensharing.client.net;

import lombok.extern.slf4j.Slf4j;
import pl.polsl.screensharing.client.controller.VideoCanvasController;
import pl.polsl.screensharing.client.state.ClientState;
import pl.polsl.screensharing.client.state.ConnectionState;
import pl.polsl.screensharing.client.state.VisibilityState;
import pl.polsl.screensharing.client.view.ClientWindow;
import pl.polsl.screensharing.client.view.fragment.VideoCanvas;
import pl.polsl.screensharing.lib.SharedConstants;
import pl.polsl.screensharing.lib.UnoperableException;
import pl.polsl.screensharing.lib.net.AbstractDatagramSocketThread;

import javax.imageio.ImageIO;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import static pl.polsl.screensharing.lib.SharedConstants.BILION;
import static pl.polsl.screensharing.lib.SharedConstants.FRAME_SIZE;

@Slf4j
public class ClientDatagramSocket extends AbstractDatagramSocketThread {
    private final ClientState clientState;
    private final VideoCanvas videoCanvas;
    private final VideoCanvasController videoCanvasController;

    private VisibilityState visibilityState;

    public ClientDatagramSocket(
        ClientWindow clientWindow, VideoCanvas videoCanvas, VideoCanvasController videoCanvasController
    ) {
        super();
        clientState = clientWindow.getClientState();
        this.videoCanvas = videoCanvas;
        this.videoCanvasController = videoCanvasController;
        visibilityState = VisibilityState.WAITING_FOR_CONNECTION;
        initObservables();
    }

    @Override
    public void run() {
        log.info("Started datagram thread with TID {}", getName());
        final ByteArrayOutputStream receivedDataBuffer = new ByteArrayOutputStream();

        final int debugBytesLength = 2; // ilość bajtów debugujących
        // Bộ đệm chứa dữ liệu nhận vào (dữ liệu + bộ đệm debug + IV).
        byte[] receiveBuffer = new byte[FRAME_SIZE];
        byte countOfPackages; // Số lượng gói tin mà phía nhận (receiver) đã nhận được.
        byte packageIteration; // Chỉ số (iterator) của gói tin mà phía nhận (receiver) đã nhận được.
        boolean isCorrupted = false;
        boolean isStarted = false;
        byte prevPackageIteration = 1;

        // Luồng (thread) nhận dữ liệu được host truyền qua kênh UDP.
// Luồng này có một hệ thống kiểm tra và sửa lỗi đơn giản.
//
// Trong mỗi vòng lặp chính, luồng sẽ nhận lần lượt các gói tin do host gửi đến.
// Từ mỗi gói có kích thước khoảng 32 KB, chương trình sẽ tách ra:
// - 3 byte dữ liệu dùng để debug/kiểm tra.
// - Các byte còn lại chứa dữ liệu của luồng ảnh JPEG.
//
// Dữ liệu trong 3 byte debug được sử dụng để kiểm tra tính chính xác dựa trên:
// - Số lượng gói tin tạo thành một khung hình.
// - Thứ tự của các gói tin.
//
// Nếu phát hiện số lượng gói tin của một khung hình không chính xác
// hoặc các gói tin được nhận sai thứ tự,
// toàn bộ dữ liệu trong bộ đệm (buffer) sẽ bị loại bỏ
// và khung hình đó sẽ không được hiển thị (render).

        long lastTime = System.nanoTime();
        long currentTime;
        long timer = 0, logTimer = 0;
        long recvBytes = 0;
        int corruptedFrames = 0;

        while (isThreadActive) {
            currentTime = System.nanoTime();
            timer += (currentTime - lastTime);
            logTimer += (currentTime - lastTime);
            lastTime = currentTime;
            try {
                final DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                datagramSocket.receive(receivePacket);
                recvBytes += receivePacket.getLength();

                // Giải mã dữ liệu bằng khóa AES và IV được mã hóa kèm theo,
// do sử dụng chế độ mã hóa CTR (Counter Mode).
                final byte[] decrypted = cryptoSymmetricHelper
                    .decrypt(receivePacket.getData(), receivePacket.getLength());

                // Chuyển 3 byte debug đã được giải mã vào các biến tương ứng.
                countOfPackages = decrypted[0];
                packageIteration = decrypted[1];

                // Nếu tham gia vào giữa quá trình truyền,
// bỏ qua các phân mảnh cho đến khi nhận được phân mảnh đầu tiên của một khung hình.
                if (!isStarted) {
                    if (packageIteration == 1) {
                        isStarted = true;
                    } else {
                        continue;
                    }
                }

                // Thêm dữ liệu đã giải mã vào bộ đệm (buffer),
// bỏ qua các byte debug và IV 128-bit.
                receivedDataBuffer.write(decrypted, debugBytesLength,
                    decrypted.length - debugBytesLength);

                // Nếu phát hiện các khung hình hoặc phân mảnh không đúng thứ tự,
// đánh dấu khung hình hiện tại là bị lỗi (corrupted).
                if (prevPackageIteration < packageIteration - 1) {
                    isCorrupted = true;
                }
                prevPackageIteration = packageIteration;

                // Ghép các phân mảnh của khung hình lại và tạo ra hình ảnh
// nếu đã nhận đầy đủ tất cả các phân mảnh và chúng không bị lỗi.
                if (countOfPackages == packageIteration) {
                    if (!isCorrupted) {
                        final byte[] receivedData = receivedDataBuffer.toByteArray();
                        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(receivedData);
                        videoCanvasController.setReceivedImage(ImageIO.read(byteArrayInputStream));
                        videoCanvas.repaint();
                        byteArrayInputStream.close();
                    } else {
                        corruptedFrames++;
                    }
                    isCorrupted = false;
                    receivedDataBuffer.reset();// Xóa (làm sạch) bộ đệm chứa các phân mảnh của khung hình.
                }
            } catch (Exception ex) {
                isCorrupted = false;
                receivedDataBuffer.reset();
            }
            if (logTimer >= BILION * 6L) {
                if (recvBytes > 0) {
                    log.info("Client datagram socket processed {} bytes. Lost frames: {}", recvBytes, corruptedFrames);
                }
                logTimer = 0;
            }
            if (timer >= BILION) {
                clientState.updateRecvBytesPerSec(recvBytes);
                log.debug("Client datagram socket processed {} bytes", recvBytes);
                clientState.updateLostFramesCount(corruptedFrames);
                corruptedFrames = 0;
                recvBytes = 0;
                timer = 0;
            }
        }
        stopAndClear();
    }

    @Override
    public void createDatagramSocket(byte[] secretKey, int port) {
        try {
            cryptoSymmetricHelper.init(secretKey);
            datagramSocket = new DatagramSocket(port);
            datagramSocket.setSoTimeout(1000);
        } catch (Exception ex) {
            clientState.updateConnectionState(ConnectionState.DISCONNECTED);
            throw new UnoperableException(ex);
        }
    }

    @Override
    protected void abstractStopAndClear() {
        if (!visibilityState.equals(VisibilityState.TEMPORARY_HIDDEN)) {
            clientState.updateVisibilityState(VisibilityState.WAITING_FOR_CONNECTION);
        }
        clientState.updateFrameAspectRation(SharedConstants.DEFAULT_ASPECT_RATIO);
        clientState.updateRecvBytesPerSec(0L);
    }

    @Override
    protected void initObservables() {
        clientState.wrapAsDisposable(clientState.getVisibilityState$(), visibilityState -> {
            isThreadActive = visibilityState.equals(VisibilityState.VISIBLE);
            this.visibilityState = visibilityState;
        });
    }
}
