package Process;

import java.io.*;
import java.util.*;
import java.net.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class KeyValueServer extends BlockingProcess {

    /**
     * A FIFO queue to store buffering message.
     */
    private PriorityQueue FIFO_Buffer;

    private ConcurrentHashMap<String, Integer> ReadWriteVars;
    private boolean inAck;

    /**
     * To tell message with which header should be delivered next.
     */
    private int sequence_cursor;

    public KeyValueServer(BlockingQueue q, int ID, ConcurrentHashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        super(q, ID, map, min_delay, max_delay);
        FIFO_Buffer = new PriorityQueue<String[]>(10, new Comparator<String[]>() {
            @Override
            public int compare(String[] s1, String[] s2) {
                return Integer.parseInt(s1[1]) - Integer.parseInt(s2[1]);
            }
        });
        sequence_cursor = 1;
        ReadWriteVars = new ConcurrentHashMap<>();
        inAck = false;
    }

    /**
     * Launch a total order process, and then start listening on other processes.
     * Wait for the console command to send, once receive a msend command, send the message
     * to the master node.
     */
    @Override
    public void run() {
        System.out.println("A Key-Value storage server is up");
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Socket s = sock.accept();
                        System.out.println("accepting: " + s.getRemoteSocketAddress() + " is connected? " + s.isConnected());
                        if (!idMapOOS.containsValue(s)) {
                            Integer newID = ipMapId.get(s.getRemoteSocketAddress());
                            System.out.println("incoming id: " + newID);
                            assert newID != null;
                            idMapSocket.put(newID, s);
                            idMapOOS.put(newID, new ObjectOutputStream(s.getOutputStream()));
                        }
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {

                                    multicast_receive(ipMapId.get(s.getRemoteSocketAddress()), new byte[8]);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }).start();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
//        System.out.println("Sleep for 3000ms");
//        try {
//            Thread.sleep(3000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        while (true) {
            try {
                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                final String msg = (String) writeQueue.poll(1, TimeUnit.DAYS);
                String parsed[] = msg.split(" ", 3);
                if (parsed[0].equals("delay")) {
                    try {
                        Thread.sleep(Integer.parseInt(parsed[1]));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else if (parsed[0].equals("dump")) {
                    for(Map.Entry <String, Integer> e : ReadWriteVars.entrySet()){
                        System.out.println(e.getKey()+":"+e.getValue());
                    }
                } else {

                    new Timer().schedule(new TimerTask() {
                        @Override
                        public void run() {
                            try {
                                //Send message to Master.
                                if (parsed.length < 2 || parsed.length > 3) {
                                    System.out.println("not a legal command");
                                    return;
                                }
                                System.out.println("delay is :" + delay);

                                if (parsed[0].equals("get") || parsed[0].equals("put")) {
                                    if (inAck)
                                        System.out.println("Waiting for Ack, can't issue operation.");
                                    else
                                        multicast_send(0, msg.getBytes());
                                } else {
                                    System.out.println("not a legal command");
                                    return;
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }, delay);
                }
                } catch(InterruptedException e){
                    e.printStackTrace();
            }
        }
    }

    /**
     * This function handles connection (client side). If this is the first message, the new established
     * Socket need to be added to global maps. Otherwise, it just pull out the record from the map.
     *
     * @param dst
     * @return
     * @throws IOException
     */
    public ObjectOutputStream MhandleSendConnection(int dst) throws IOException {
        Socket s;
        ObjectOutputStream oos = null;
        if (idMapOOS.containsKey(dst)) {
            oos = idMapOOS.get(dst);
        } else {//this is first time connection
            s = new Socket();
            s.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            s.bind(addr);
            InetSocketAddress ip;
            ip = idMapIp.get(dst);
            s.connect(ip);
            oos = new ObjectOutputStream(s.getOutputStream());
            idMapOOS.put(dst, oos);
            idMapSocket.put(dst, s);
            new Thread(() -> {
                try {
                    multicast_receive(ipMapId.get(s.getRemoteSocketAddress()), new byte[8]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        return oos;
    }


    /**
     * Send message to the corresponding process with ID=dst.
     *
     * @param dst
     * @param msg
     * @throws IOException
     */
    protected void multicast_send(int dst, byte[] msg) throws IOException {
        ObjectOutputStream oos;
        if (dst == selfID) {
            System.out.println("You are sending message to yourself! Msg: " + new String(msg));
            return;
        }
        oos = MhandleSendConnection(dst);
        writeLock.lock();
        oos.flush();
        oos.writeObject(new Message(selfID, addr, new String(msg), 0));
        writeLock.unlock();
        inAck = true;
    }

    /**
     * Handle multicast receive, once called, receives message from all processes, once received a message,
     * compare its header with the current cursor, if header equals the cursor, then deliver the message
     * immediately. Otherwise, we put the message into our buffer queue. And to see whether to poll it when
     * the cursor updates.
     *
     * @param dst
     * @param msg not used
     * @throws IOException
     */
    private void multicast_receive(int dst, byte[] msg) throws IOException {
        Socket s = idMapSocket.get(dst);
        ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
        while (true) {
            Message m = null;
            try {
                m = (Message) ois.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            String str = m.Serial;
            String[] strs = str.split(",");
            if (Integer.parseInt(strs[1]) > this.sequence_cursor) {
                FIFO_Buffer.offer(strs);
            } else {
                if(m.Sender_ID == selfID){
                    inAck = false;
                }
                DeliverMsg(strs[2]);
                this.sequence_cursor++;
                while (getHeader((String[]) FIFO_Buffer.peek()) <= this.sequence_cursor) {
                    String[] cur = (String[]) FIFO_Buffer.poll();
                    if(Integer.parseInt(cur[0]) == selfID){
                        inAck = false;
                    }
                    DeliverMsg(strs[2]);
                    this.sequence_cursor++;
                }
            }
        }
    }

    public int getHeader(String[] strs) {
        if (strs == null) return Integer.MAX_VALUE;
        return Integer.parseInt(strs[1]);
    }

    private void DeliverMsg(String msg){
        String[] msgs = msg.split(" ");
        if(msgs[0].equals("put")){
            putVal(msgs[1], msgs[2]);
            //TODO:Write to Log file.
            updateLogFile(this.selfID, msgs[0], msgs[1], "resp", Integer.parseInt(msgs[2]));
        } else if (msgs[0].equals("get")) {
            getVal(msgs[1]);
            updateLogFile(this.selfID, msgs[0], msgs[1], "resp", -1);
            //TODO:Write to Log file.
        } else {
            System.out.println("Fatal Error in Delivery!");
        }
    }

    private void getVal(String Key){
        if(!ReadWriteVars.containsKey(Key)){
            System.out.println("Get "+Key+": 0");
            ReadWriteVars.put(Key, 0);
            System.out.println("Setting "+Key+": 0");
            //TODO:Write to Log file.
            updateLogFile(this.selfID, "get", Key, "req", 0);
        } else{
            System.out.println("Get "+Key+": "+ReadWriteVars.get(Key));
            //TODO:Write to Log file.
            updateLogFile(this.selfID, "get", Key, "req", ReadWriteVars.get(Key));
        }
    }

    private void putVal(String Key, String val){
        int v = Integer.parseInt(val);
        ReadWriteVars.put(Key,v);
        System.out.println("Setting "+Key+":"+val);
        //TODO:Write to Log file.
        updateLogFile(this.selfID, "put", Key, "req", Integer.parseInt(val));
    }

    private void updateLogFile(int serverID, String OP, String Key, String OPType, int value) {
        String filename = new String("log" + p.selfID + ".txt");
        File file = new File(filename);
        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch(IOException ioe) {
                ioe.printStackTrace();
            }
        }
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        long t = timestamp.getTime();
        String str = "555," + serverID + "," + OP + "," + Key + "," + t + "," + OPType;
        if(OP.equals("put") || (OP.equals("get") && (OPType.equals("resp")))) {
            str += "," + value;
        }
        str += "\r\n";
        RandomAccessFile randfile = null;
        try {
            randfile = new RandomAccessFile(filename, "rw");
            long fileLength = randfile.length();
            randfile.seek(fileLength);
            randfile.writeBytes(str);
            randfile.close();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }

    }
}
