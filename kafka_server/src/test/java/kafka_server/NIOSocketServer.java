package kafka_server;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class NIOSocketServer extends Thread{
	private Selector selector;  
	  
    private ServerSocketChannel ssc;  
  
    /** 
     * @param args 
     */  
    public static void main(String[] args) {  
        NIOSocketServer server = new NIOSocketServer();  
        try {  
            // server.setDaemon(true);  
            server.initServer();  
            server.start();  
        } catch (Exception e) {  
            e.printStackTrace();  
            server.stopServer();  
        }  
    }  
  
    public void run() {  
        while (true) {  
            try {  
                int select = selector.select();  
                if (select > 0) {  
                    Set<SelectionKey> keys = selector.selectedKeys();  
                    Iterator<SelectionKey> iter = keys.iterator();  
                    while (iter.hasNext()) {  
                        SelectionKey key = iter.next();  
                        if (key.isAcceptable()) {  
                            doAcceptable(key);  
                        }  
                        if (key.isWritable()) {  
                            doWriteMessage(key);  
                        }  
                        if (key.isReadable()) {  
                            doReadMessage(key);  
                        }  
                        if (key.isConnectable()) {  
                            doConnectable(key);  
                        }  
                        iter.remove();  
                    }  
                }  
            } catch (Exception e) {  
                e.printStackTrace();  
            }  
        }  
    }  
  
    /** 
     * 初始化服务器端程序，开始监听端口 
     *  
     * @throws IOException 
     * @throws ClosedChannelException 
     */  
    private void initServer() throws IOException, ClosedChannelException {  
        selector = Selector.open();  
  
        ssc = ServerSocketChannel.open();  
        ssc.configureBlocking(false);  
        ssc.socket().bind(new InetSocketAddress(2181));  
        ssc.register(selector, SelectionKey.OP_ACCEPT);  
    }  
  
    /** 
     * 停止服务器端 
     */  
    private void stopServer() {  
        try {  
            if (selector != null && selector.isOpen()) {  
                selector.close();  
            }  
            if (ssc != null && ssc.isOpen()) {  
                ssc.close();  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
  
    /** 
     * 对新的客户端连接进行处理 
     *  
     * @param key 
     * @throws IOException 
     * @throws ClosedChannelException 
     */  
    private void doAcceptable(SelectionKey key) throws IOException,  
            ClosedChannelException {  
        System.out.println("is acceptable");  
        ServerSocketChannel tempSsc = (ServerSocketChannel) key.channel();  
        SocketChannel ss = tempSsc.accept();  
        ss.configureBlocking(false);  
        ss.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);  
    }  
  
    /** 
     * 写消息到客户端 
     *  
     * @param key 
     * @throws IOException 
     * @throws UnsupportedEncodingException 
     */  
    private void doWriteMessage(SelectionKey key) throws IOException,  
            UnsupportedEncodingException {  
        System.out.println("is writable");  
        SocketChannel sc = (SocketChannel) key.channel();  
        ByteBuffer buffer = ByteBuffer.wrap("how are you?".getBytes("UTF-8"));  
        while (buffer.hasRemaining()) {  
            sc.write(buffer);  
        }  
        // sk.interestOps(SelectionKey.OP_READ);  
    }  
  
    /** 
     * 读取客户端传递过来的消息 
     *  
     * @param key 
     * @throws IOException 
     * @throws UnsupportedEncodingException 
     */  
    private void doReadMessage(SelectionKey key) throws IOException,  
            UnsupportedEncodingException {  
        System.out.println("is readable");  
        SocketChannel sc = (SocketChannel) key.channel();  
  
        ByteBuffer bb = ByteBuffer.allocate(8);  
        System.out.println("receive from clint:");  
        int read = sc.read(bb);  
        while (read > 0) {  
            bb.flip();  
  
            byte[] barr = new byte[bb.limit()];  
            bb.get(barr);  
  
            System.out.print(new String(barr, "UTF-8"));  
            bb.clear();  
  
            read = sc.read(bb);  
        }  
        System.out.println("");  
        // sk.interestOps(SelectionKey.OP_WRITE);  
    }  
  
    /** 
     * 已连接 
     *  
     * @param key 
     */  
    private void doConnectable(SelectionKey key) {  
        System.out.println("is connectalbe");  
    }  

}
