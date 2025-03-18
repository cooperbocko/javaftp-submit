package client2;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//TODO: Clean up method
//TODO: Process table and checking in get/put
public class MyClient2 {
    //Variables
    private static final CustomMap<String, ReentrantReadWriteLock> fileLocks = new CustomMap<>();
    private static final CustomMap<Integer, Boolean> processTable = new CustomMap<>(); // {id, isTerminated?}
    private static String machinename;
    private static int nport;
    private static int tport;
    // Client Driver
    public static void main(String[] args) {
       
        // Command Line
        // Check if machine name and nport, tport numbers are provided
        if (args.length != 3) {
            System.err.println("Error: machine name, nport, and tport numbers must be provided. Exiting...");
            System.exit(1);
        } // if 

         // client info
        machinename = args[0];
        nport = Integer.parseInt(args[1]);
        tport = Integer.parseInt(args[2]);
        Scanner input = new Scanner(System.in);
        
        try {
            // open socket
            Socket socket = new Socket(machinename, nport);
            System.out.println("Connected to " + machinename + " on port " + nport);

            // write to server
            PrintWriter out = new PrintWriter(
                    socket.getOutputStream(), true);

            // read from server
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));

            // handle requests
            String line = null;
            while (!"quit".equalsIgnoreCase(line)) {

                // get user input
                System.out.print("myftp>");
                line = input.nextLine();

                // parse line
                String[] parse = line.split(" ");

                // get command
                String cmd = parse[0];

                // handle commands
                switch (cmd.toLowerCase()) {
                    case "put": {
                        //TODO: Attempt to get write lock here instead of in method call? also clost lock as well

                        // check if filename is provided
                        if (parse.length < 2) {
                            System.out.println("Invalid, requires file name");
                            break;
                        }
                        
                        // check if file exists
                        String filename = parse[1];
                        File file = new File(filename);
                        if (!file.exists()) {
                            System.out.println("File does not exist: " + filename);
                            break;
                        }
                        
                        // send command
                        out.println(line);
                        out.flush();

                        //get id and add to process table
                        int id = Integer.parseInt(in.readLine());
                        processTable.put(id, true);
                        System.out.println("id: " + id);

                        //get file path
                        String filepath = in.readLine();

                        //check for async command
                        if (parse.length == 3 && parse[2].equals("&")) {
                            AsyncSend asyncSend = new AsyncSend(id, parse[1], filepath);
                            new Thread(asyncSend).start();
                            break;
                        }
                        
                        // send file size
                        out.println(file.length());
                        out.flush();

                        // Ensure the fileLocks map has a lock for this file
                        fileLocks.putIfAbsent(file.getAbsolutePath(), new ReentrantReadWriteLock());

                        //obtain lock
                        //System.out.println("getting lock");
                        ReentrantReadWriteLock lock = fileLocks.get(file.getAbsolutePath());
                        lock.readLock().lock(); // Acquire write lock for the specific file
                        //System.out.println("lock obtained, sleeping");
                        try {
                            Thread.sleep(5000);
                        } catch (Exception e) {}
                        
                        // send file
                        sendFile(file, socket, id);
                        
                        // get response from server
                        System.out.println(in.readLine());

                        //release lock
                        lock.readLock().unlock();
                        break;
                    }
                    case "get": {
                        //TODO: Attempt to get read lock here instead of in method call? also clost lock as well

                        // check if filename is provided
                        if (parse.length < 2) {
                            System.out.println("Invalid, requires file name");
                            break;
                        }

                        // send command to server
                        out.println(line);
                        out.flush();

                        //get response from server - #1, initial response (pending vs DNE message)

                        String cmdresponse = in.readLine();
                        if (cmdresponse != null && cmdresponse.contains(" does not exist in the current directory: ")) {
                            System.out.println(cmdresponse);
                            break;
                        }


                        //get id
                        int id = Integer.parseInt(cmdresponse);
                        processTable.put(id, true);
                        System.out.println("id: " + id);

                        //get file path
                        String filepath = in.readLine();

                        // get file size
                        cmdresponse = in.readLine();
                        long fileSize = Long.parseLong(cmdresponse);

                        //check for async command
                        if (parse.length == 3 && parse[2].equals("&")) {
                            AsyncReceive asyncReceive = new AsyncReceive(id, parse[1], filepath, fileSize);
                            new Thread(asyncReceive).start();
                            break;
                        }

                        File file = new File(parse[1]);

                        // Ensure the fileLocks map has a lock for this file
                        fileLocks.putIfAbsent(file.getAbsolutePath(), new ReentrantReadWriteLock());

                        //obtain lock
                        //System.out.println("getting lock");
                        ReentrantReadWriteLock lock = fileLocks.get(file.getAbsolutePath());
                        lock.writeLock().lock(); // Acquire write lock for the specific file
                        //System.out.println("lock obtained, sleeping");
                        try {
                            Thread.sleep(5000);
                        } catch (Exception e) {}

                        // get file
                        receiveFile(parse[1], socket, fileSize, id);

                        // send ack to server
                        out.println("File received");
                        out.flush();

                        // get response from server
                        String finalResponse = in.readLine();
                        System.out.println(finalResponse);

                        //release the lock
                        lock.writeLock().unlock();

                        break;
                    }

                    case "delete":
        
                        // Check for filename
                        if (parse.length < 2) {
                            System.out.println("Invalid, requires file name");
                            break;
                        } // if

                        // Send command to server
                        out.println(line);
                        out.flush();

                        // Print server response
                        System.out.println(in.readLine());
                        break;

                    case "ls": {
                        // Send comamnd to server
                        out.println(line);
                        out.flush();

                        // Print response
                        String response;
                        while ((response = in.readLine()) != null) {
                            System.out.println(response);
                            if (!in.ready()) {
                                break;
                            } // if
                        } // while

                        break;
                    }

                    case "mkdir":
                        if (parse.length < 2) {
                            System.out.println("Invalid, requires a directory name.");
                        } // if

                        out.println(line);
                        out.flush();

                        String cmdResponse = in.readLine();
                        System.out.println(cmdResponse);

                        break;
                    case "quit":
                        input.close();
                        socket.close();
                        break;
                    case "terminate": {
                        if (parse.length < 2) {
                            System.out.println("Error: Command ID required.");
                            break;
                        } // if                   
                        try (Socket terminateSocket = new Socket(machinename, tport);
                            PrintWriter terminateOut = new PrintWriter(terminateSocket.getOutputStream(), true); 
                            BufferedReader terminateIn = new BufferedReader(new InputStreamReader(terminateSocket.getInputStream()))){
                            //System.out.println("Connected to " + machinename + " on port " + tport);
                            terminateOut.println(line);
                            terminateOut.flush();
                            //System.out.println("Terminate command sent to server.");
                            //String response = terminateIn.readLine();
                            //System.out.println("Get ack");
                            processTable.put(Integer.parseInt(parse[1]), true);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } // try-catch 
                        break;
                    } // terminate 
                    default:
                        out.println(line);
                        out.flush();
                        System.out.println(in.readLine());
                        break;
                }
            }
            // exit handler ->
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


        // Functions
    
        private static void sendFile(File file, Socket socket, int id) throws IOException {
            //sleep for odin delay
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        
            // buffer and IO streams
            byte[] buffer = new byte[4096];
            FileInputStream fis = null;
            BufferedOutputStream bos = null;
    
            // do not close Buffered Ourput Stream or else socket will close
            try {
    
                fis = new FileInputStream(file);
                bos = new BufferedOutputStream(socket.getOutputStream());
    
                int bytesRead;
                while (isRunning(id) && (bytesRead = fis.read(buffer)) != -1) {
                    bos.write(buffer, 0, bytesRead);
                }
                bos.flush();
    
            } finally {
                if (fis != null) {
                    fis.close();
                }
            }
        }

    private static class AsyncSend implements Runnable {
        int processId;
        String fileName;
        String filePath;
        Socket socket;

        public AsyncSend(int processId, String fileName, String filePath) {
            this.processId = processId;
            this.fileName = fileName;
            this.filePath = filePath;
        }

        @Override
        public void run() {
            try {
                //establish new socket connection
                socket = new Socket(machinename, nport);

                //read and write
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                //send command to server
                out.println("asyncsend");
                out.flush();

                //send filePath
                out.println(filePath);
                out.flush();

                //get file and send size
                File file = new File(fileName);
                out.println(file.length());
                out.flush();

                //send processId
                out.println(processId);
                out.flush();

                // Ensure the fileLocks map has a lock for this file
                fileLocks.putIfAbsent(file.getAbsolutePath(), new ReentrantReadWriteLock());

                //obtain lock
                //System.out.println("getting lock");
                ReentrantReadWriteLock lock = fileLocks.get(file.getAbsolutePath());
                lock.readLock().lock(); // Acquire write lock for the specific file
                //System.out.println("lock obtained, sleeping");
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {}

                //send file
                sendFile(file, socket, processId);

                //get response from server
                String response = in.readLine();
                System.out.println(response);
                System.out.print("myftp>");

                //release lock
                lock.readLock().unlock();
                return;
            } catch (Exception e) {

            } finally {
                //close socket
                try {
                    socket.close();
                } catch (Exception e) {

                }
            }
        }
        
    }
    
    private static class AsyncReceive implements Runnable {
        int processId;
        String fileName;
        String filePath;
        Socket socket;
        long fileSize;

        public AsyncReceive(int processId, String fileName, String filePath, long fileSize) {
            this.processId = processId;
            this.fileName = fileName;
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        @Override
        public void run() {
            try {
                //establish new socket connection
                socket = new Socket(machinename, nport);

                //read and write
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                //send command to server
                out.println("asyncreceive");
                out.flush();

                //send id
                out.println(processId);
                out.flush();

                //send filePath
                out.println(filePath);
                out.flush();

                out.println(fileSize);
                out.flush();

                File file = new File(filePath);
                fileLocks.putIfAbsent(file.getAbsolutePath(), new ReentrantReadWriteLock());
                            ReentrantReadWriteLock lock = fileLocks.get(file.getAbsolutePath());
                            lock.writeLock().lock(); // Acquire write lock for the specific file
                            try {
                                Thread.sleep(5000);
                            } catch (Exception e) {}

                //send file
                receiveFile(fileName, socket, fileSize, processId);

                //get response from server
                String ack = (in.readLine());

                //release the lock
                lock.writeLock().unlock();
                return;
            } catch (Exception e) {

            } finally {
                //close socket
                try {
                    socket.close();
                } catch (Exception e) {

                }
            }
        }
        
    }

    private static void receiveFile(String filename, Socket socket, Long fileSize, int id) throws IOException {
        // buffer and IO streams
        byte[] buffer = new byte[4096];
        FileOutputStream fos = null;
        BufferedInputStream bis = null;

        while (filename.contains("/")) {
            int index = filename.indexOf("/");
            filename = filename.substring(index + 1, filename.length());
        }

        try {
            // read and create file

            fos = new FileOutputStream(filename);

            bis = new BufferedInputStream(socket.getInputStream());
            int bytesRead;
            long totalBytesRead = 0;
            while (isRunning(id) && totalBytesRead < fileSize && (bytesRead = bis.read(buffer, 0, (int) Math.min(buffer.length, fileSize))) != -1) {
                fos.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
            }

            if (totalBytesRead < fileSize){
                processTable.put(id, false);
            }

            fos.flush();

        } finally {
            if (fos != null) {
                fos.close();
            }
            if (!isRunning(id)){

                File file = new File(filename);
                file.delete();
            }

        }
    }

    private static boolean isRunning(int id) {
        return processTable.get(id);
    }

    private static class CustomMap<K, V> {
        private HashMap<K, V> map = new HashMap<>();
        private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    
        public V get(K key) {
            //lock read lock
            lock.readLock().lock();
    
            //check if key exists
            if (!map.containsKey(key)) {
                //release lock
                lock.readLock().unlock();
                return null;
            }
    
            //get value
            V value = map.get(key);
    
            //release lock
            lock.readLock().unlock();
    
            return value;
        }
    
        public void put(K key, V value) {
            //lock write lock
            lock.writeLock().lock();
    
            //insert into map
            map.put(key, value);
    
            //relese lock
            lock.writeLock().unlock();
    
            return;
        }
    
        public void putIfAbsent(K key, V value) {
            //lock write lock
            lock.writeLock().lock();
    
            //check if key exists
            if (map.containsKey(key)) {
                //relese lock
                lock.writeLock().unlock();
                return;
            }
    
            //insert into map
            map.put(key, value);
    
            //relese lock
            lock.writeLock().unlock();
    
            return;
        }
    
    
    }

}
