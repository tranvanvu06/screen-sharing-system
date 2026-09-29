package pl.polsl.screensharing.host.net;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import pl.polsl.screensharing.host.view.HostWindow;
import pl.polsl.screensharing.lib.Utils;
import pl.polsl.screensharing.lib.net.CryptoAsymmetricHelper;
import pl.polsl.screensharing.lib.net.SocketState;
import pl.polsl.screensharing.lib.net.payload.KickReason;
import pl.polsl.screensharing.lib.net.payload.SignalState;
import pl.polsl.screensharing.lib.net.payload.VideoFrameDetails;

import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

@Slf4j
public class SendSignalsThread extends Thread {
    private final ClientThread clientThread;
    private final HostWindow hostWindow;
    private final Socket socket;
    private final ObjectMapper objectMapper;
    private final CryptoAsymmetricHelper cryptoAsymmetricHelper;

    private PrintWriter printWriter;
    @Setter
    private SocketState eventSignalState;

    public SendSignalsThread(ClientThread clientThread, HostWindow hostWindow) {
        this.clientThread = clientThread;
        this.hostWindow = hostWindow;
        socket = clientThread.getSocket();
        cryptoAsymmetricHelper = clientThread.getCryptoAsymmetricHelper();
        eventSignalState = SocketState.WAITING;
        objectMapper = new ObjectMapper();
    }

    private void signalEventLoop() throws Exception {
        switch (eventSignalState) {
            // Tín hiệu được phát khi bắt đầu truyền phát (stream) màn hình.
            case EVENT_START_STREAMING: {
                final VideoFrameDetails videoFrameDetails = VideoFrameDetails.builder()
                    .aspectRatio(Utils.calcAspectRatio(hostWindow.getVideoCanvas().getController().getRawImage()))
                    .streamingSignalState(clientThread.determinateStreamingState())
                    .build();
                performSSLSignal(videoFrameDetails, SocketState.EVENT_START_STREAMING);
                eventSignalState = SocketState.WAITING;
                log.info("(signal event) Send start sharing screen event with data {}", videoFrameDetails);
                break;
            }
            // Tín hiệu được phát khi dừng truyền phát (stream) màn hình.
            case EVENT_STOP_STREAMING: {
                final SignalState<Boolean> signalState = new SignalState<>(true);
                performSSLSignal(signalState, SocketState.EVENT_STOP_STREAMING);
                eventSignalState = SocketState.WAITING;
                log.info("(signal event) Send stop sharing screen event with data {}", signalState);
                break;
            }
            // Tín hiệu được phát khi host hiển thị/ẩn màn hình.
            case EVENT_TOGGLE_SCREEN_VISIBILITY: {
                final VideoFrameDetails videoFrameDetails = VideoFrameDetails.builder()
                    .aspectRatio(Utils.calcAspectRatio(hostWindow.getVideoCanvas().getController().getRawImage()))
                    .streamingSignalState(clientThread.determinateStreamingState())
                    .build();

                performSSLSignal(videoFrameDetails, SocketState.EVENT_TOGGLE_SCREEN_VISIBILITY);
                eventSignalState = SocketState.WAITING;
                log.info("(signal event) Send show/hide screen event with data {}", videoFrameDetails);
                break;
            }
            // Tín hiệu được phát khi một hoặc nhiều người dùng bị đẩy (kick) khỏi phiên.
            case KICK_FROM_SESSION: {
                final KickReason kickReason = new KickReason("You has been kicked from session.");
                performSSLSignal(kickReason, SocketState.KICK_FROM_SESSION);
                socket.close();
                eventSignalState = SocketState.WAITING;
                log.info("(signal event) Send kick user/s event with data {}", kickReason);
                break;
            }
            // Tín hiệu được phát khi phiên kết thúc.
            case END_UP_SESSION: {
                final KickReason kickReason = new KickReason("Session has been ended.");
                performSSLSignal(kickReason, SocketState.END_UP_SESSION);
                socket.close();
                eventSignalState = SocketState.WAITING;
                log.info("(signal event) Send end up session event with data {}", kickReason);
                break;
            }
        }
    }

    @Override
    public void run() {
        log.info("Started client send events thread with PID {}", getId());
        try (final PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            printWriter = out;
            while (socket.isConnected() && clientThread.isAlive()) {
                signalEventLoop();
            }
        } catch (SocketException ignored) {
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
        clientThread.stopAndClose();
    }

    @Override
    public synchronized void start() {
        if (!isAlive()) {
            setName("Thread-TCP-Signal-" + clientThread.getThreadId() + getId());
            super.start();
        }
    }

    private void performSSLSignal(Object resData, SocketState signal) throws Exception {
        final String rawResponse = objectMapper.writeValueAsString(resData);
        final String encrypted = cryptoAsymmetricHelper.encrypt(rawResponse, clientThread.getClientPublicKey());
        printWriter.println(signal.generateBody(encrypted));
    }
}
