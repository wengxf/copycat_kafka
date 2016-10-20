package kafka_server;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class NIOSocketClient extends Thread{

	private SocketChannel socketChannel;  
    private Selector selector;  
  
    /** 
     * @param args 
     */  
    public static void main(String[] args) {  
        NIOSocketClient client = new NIOSocketClient();  
        try {  
            client.initClient();  
            client.start();  
            // client.setDaemon(true);  
        } catch (Exception e) {  
            e.printStackTrace();  
            client.stopServer();  
        }  
    }  
  
    public void run() {  
//        while (true) {  
            try {  
                // 写消息到服务器端  
                writeMessage();  
  
//                int select = selector.select();  
//                if (select > 0) {  
//                    Set<SelectionKey> keys = selector.selectedKeys();  
//                    Iterator<SelectionKey> iter = keys.iterator();  
//                    while (iter.hasNext()) {  
//                        SelectionKey sk = iter.next();  
//                        if (sk.isReadable()) {  
//                            readMessage(sk);  
//                        }  
//                        iter.remove();  
//                    }  
//                }  
            } catch (Exception e) {  
                e.printStackTrace();  
            }  
//        }  
    }  
  
    public void readMessage(SelectionKey sk) throws IOException,  
            UnsupportedEncodingException {  
        SocketChannel curSc = (SocketChannel) sk.channel();  
        ByteBuffer buffer = ByteBuffer.allocate(8);  
        while (curSc.read(buffer) > 0) {  
            buffer.flip();  
            System.out.println("Receive from server:"  
                    + new String(buffer.array(), "UTF-8"));  
            buffer.clear();  
        }  
    }  
  
    public void writeMessage() throws IOException {  
        try {  
            String ss = "Server,how are you?";  
            ByteBuffer buffer = ByteBuffer.wrap(ss.getBytes("UTF-8"));  
            while (buffer.hasRemaining()) {  
//                System.out.println("buffer.hasRemaining() is true.");  
                socketChannel.write(buffer);  
            }  
        } catch (IOException e) {  
            if (socketChannel.isOpen()) {  
                socketChannel.close();  
            }  
            e.printStackTrace();  
        }  
    }  
  
    public void initClient() throws IOException, ClosedChannelException {  
        InetSocketAddress addr = new InetSocketAddress(8080);  
        socketChannel = SocketChannel.open();  
  
        selector = Selector.open();  
        socketChannel.configureBlocking(false);  
        socketChannel.register(selector, SelectionKey.OP_READ);  
  
        // 连接到server  
        socketChannel.connect(addr);  
  
        while (!socketChannel.finishConnect()) {  
            System.out.println("check finish connection");  
        }  
    }  
  
    /** 
     * 停止客户端 
     */  
    private void stopServer() {  
        try {  
            if (selector != null && selector.isOpen()) {  
                selector.close();  
            }  
            if (socketChannel != null && socketChannel.isOpen()) {  
                socketChannel.close();  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
}
