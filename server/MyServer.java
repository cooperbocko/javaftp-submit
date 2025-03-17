import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//TODO: Add process table and process checking to get and put
//TODO: ASYNC GET
//TODO: Clean up method


public class MyServer {
    //Variables
    private static final CustomMap<String, ReentrantReadWriteLock> fileLocks = new CustomMap<>();
    private static final CustomMap<Integer, Boolean> processTable = new CustomMap<>(); // {id, isRunning?}
    private static int id = 0;
    private static String currentdir;
    
    // Server Driver
    public static void main(String[] args) {
        // Check if nport and tport numbers provided 
        if (args.length != 2) {
            System.err.println("Error: both nport and tport numbers required. Exiting...");
            System.exit(1);
        } // if 
        int nport = Integer.parseInt(args[0]);
        int tport = Integer.parseInt(args[1]);

        Thread normalThread = new Thread(() -> createPort(nport, false));
        Thread terminateThread = new Thread(() -> createPort(tport, true));
        normalThread.start();
        terminateThread.start();

    } // main

    /*
     * Creates port for given port number to handle normal client connections
     * or termination commands based on the value of isTerminatePort
     * 
     * @param port Port number to listen on 
     * @ isTerminatePort Boolean flag indicating port type 
     */
    private static void createPort(int portNumber, boolean isTerminatePort) {
        ServerSocket serverSocket = null; 
        try {
            serverSocket = new ServerSocket(portNumber);
            serverSocket.setReuseAddress(true);
            if (isTerminatePort) {
                System.out.println("Terminate server running on port " + portNumber);
            } else {
                System.out.println("Normal server running on port " + portNumber);
            } // if-else
            while (true) { // Accept client connections 
                Socket client = serverSocket.accept();
                System.out.println("New Connection: " + client.getInetAddress().getHostAddress());
                if (isTerminatePort) {
                    System.out.println("Waiting for incoming terminate commands: needs terminate handler");
                    handleTerminateRequest(client);
                } else {
                    ClientHandler clientSocket = new ClientHandler(client);
                    new Thread(clientSocket).start();
                } // if-else
            } // while 
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } // try-catch
            } // if 
        } // try-catch
    } // createPort

    /*
     * Handle termination requests received on termination port 
     *  
     * TO-DO: Use CID to set status of command to terminate 
     *       Delete any files that were created
     *       Stop transferring data I guess 
     * @param client Socket connection 
     */
    private static void handleTerminateRequest (Socket client) {
        try (BufferedReader in = 
                new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter out = new PrintWriter(client.getOutputStream())){
                    String line = in.readLine();
                    String[] parse = line.split(" "); 
                    int cmdID = Integer.parseInt(parse[1]);
                    processTable.put(cmdID, false);

        } catch (IOException e) {
            e.printStackTrace();
        } // try-catch
    } // handleTerminateRequest 

    // Handle the client
    private static class ClientHandler implements Runnable {
        // Varibales
        private final Socket clientSocket;

        // Constructor
        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        public void run() {
            // send and recieve data
            PrintWriter out = null;
            BufferedReader in = null;

            // directory
            String homedir = System.getProperty("user.dir");
            String currentdir = homedir;
            ArrayList<String> directorytemp;

            try {
                // send to client
                out = new PrintWriter(
                        clientSocket.getOutputStream(), true);

                // read from client
                in = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream()));

                // handle requests
                String line;
                while ((line = in.readLine()) != null) {
                    // parse line
                    String[] parse = line.split(" ");
                    // get command
                    String cmd = parse[0];

                    switch (cmd.toLowerCase()) {
                        case "put": {

                            //send id
                            int processid = id;
                            out.println(id);
                            out.flush();

                            //add to processtable and increment id
                            processTable.put(id, true);
                            id++;

                            //send file path
                            out.println(currentdir + "/" + parse[1]);

                            //check for async command, if so break -> handled by a different command
                            if (parse.length == 3 && parse[2].equals("&")) {
                                break;
                            } // if

                            // get file size
                            long fileSize = Long.parseLong(in.readLine());

                            //get file
                            String filename = parse[1];
                            File file = new File(currentdir + "/" + filename);

                            // Ensure the fileLocks map has a lock for this file
                            fileLocks.putIfAbsent(file.getAbsolutePath(), new ReentrantReadWriteLock());

                            // get lock
                            ReentrantReadWriteLock lock = fileLocks.get(file.getAbsolutePath());
                            lock.writeLock().lock(); // Acquire write lock for the specific file
                            try {
                                Thread.sleep(5000);
                            } catch (Exception e) {}

                            //write to file
                            receiveFile(currentdir + "/" + filename, clientSocket, fileSize, file, processid);
                            
                            // send response
                            out.println("File: " + filename + " received successfully.");
                            out.flush();

                            //release lock
                            lock.writeLock().unlock();
                            break;
                        }


                        case "get": {

                            // get file
                            String filename = parse[1];
                            File file = new File(currentdir + "/" + filename);

                            // make sure file still exists
                            if (!file.exists()) {
                                out.println("" + filename + " does not exist in the current directory: " + currentdir);
                                out.flush();
                                break;
                            }


                            //send id
                            out.println(id);
                            out.flush();
                            processTable.put(id, true);
                            int pid = id;
                            id++;

                            out.println(currentdir + "/" + parse[1]);
                            out.flush();

                            // send file size
                            out.println(file.length());
                            out.flush();

                            // check for async, if so, let be handled by asyncreceive
                            if (parse.length == 3 && parse[2].equals("&")) {
                                break;
                            }

                            // Ensure the fileLocks map has a lock for this file
                            fileLocks.putIfAbsent(file.getAbsolutePath(), new ReentrantReadWriteLock());

                            //obtain lock
                            ReentrantReadWriteLock lock = fileLocks.get(file.getAbsolutePath());
                            lock.readLock().lock(); // Acquire write lock for the specific file
                            try {
                            Thread.sleep(5000);
                            } catch (Exception e) {}

                            // send file
                            sendFile(file, clientSocket, pid);

                            // recieve ack
                            System.out.println(in.readLine());

                            // send response
                            out.println("File: " + filename + " sent successfully.");
                            out.flush();

                            //release lock
                            lock.readLock().unlock();
                            break;
                        }

                        case "delete": {

                            // Check for filename
                            if (parse.length < 2) {
                                out.println("Invalid, requires file name.");
                                out.flush();
                                break;
                            } // if

                            // Filename of file to delete
                            String deleteFile = parse[1];
                            String path = currentdir + "/" + deleteFile;

                            File file = new File(path);

                            // Ensure the fileLocks map has a lock for this file
                            fileLocks.putIfAbsent(file.getAbsolutePath(), new ReentrantReadWriteLock());

                            // get lock
                            ReentrantReadWriteLock lock = fileLocks.get(file.getAbsolutePath());
                            lock.writeLock().lock(); // Acquire write lock for the specific file
                            try {
                                Thread.sleep(5000);
                            } catch (Exception e) {}

                            // Attempt to delete file
                            if (file.exists() && file.delete()) {
                                out.println("File: " + deleteFile + " deleted successfully.");
                            } else if (!file.exists()) { // File doesn't exist
                                out.println("File: " + deleteFile + " not found.");
                            } else { // File couldn't be deleted
                                out.println("Failed to delete file: " + deleteFile);
                            } // if-else

                            out.flush();

                            //release lock
                            lock.writeLock().unlock();
                            break;
                        }

                        case "ls": {

                            // Get remote directory

                            File dir = new File(currentdir);

                            if (dir.exists() && dir.isDirectory()) {
                                StringBuilder list = new StringBuilder();
                                File[] files = dir.listFiles();

                                if (files != null && files.length > 0) {
                                    for (File file : files) {
                                        String fileName = file.getName();

                                        // Skip hidden files
                                        if (fileName.startsWith(".")) {
                                            continue;
                                        } // if

                                        if (file.isDirectory()) {
                                            list.append("[DIR] ")
                                                    .append(fileName)
                                                    .append("\n");
                                        } else {
                                            list.append(fileName)
                                                    .append("\n");
                                        } // if-else
                                    } // for-each
                                } else {
                                    list.append("No files or directories found.");
                                } // if-else

                                // Send list to client
                                out.println(list.toString().trim());
                            } else {
                                out.println("Unable to access remote directory.");
                            } // if-else

                            out.flush();
                            break;
                        }

                        case "pwd": {
                            out.println(System.getProperty("user.dir"));
                            break;
                        }

                        case "cd": {
                            if (parse.length < 2) {
                                System.setProperty("user.dir", homedir);
                                currentdir = homedir;

                            } else if ((parse[1].charAt(0) == '/')) {
                                Path path = Paths.get(parse[1]);
                                if (Files.exists(path)) {
                                    try {
                                        System.setProperty("user.dir", path.toString());
                                        currentdir = path.toString();
                                    } catch (Exception e) {
                                        out.println(e);
                                    }
                                } else {
                                    out.println("Directory cannot be accessed.");
                                } // else

                            } else if ((parse[1].length() > 1) && (parse[1].charAt(0) == '~')
                                    && (parse[1].charAt(1) == '/')) {
                                Path path = Paths.get(homedir, parse[1].substring(2));
                                if (Files.exists(path)) {
                                    try {
                                        System.setProperty("user.dir", path.toString());
                                        currentdir = path.toString();
                                    } catch (Exception e) {
                                        out.println(e);
                                    }
                                } else {
                                    out.println("Directory cannot be accessed.");
                                } // else

                            } else if (parse[1].charAt(0) == '~') {
                                Path path = Paths.get(homedir);
                                if (Files.exists(path)) {
                                    try {
                                        System.setProperty("user.dir", path.toString());
                                        currentdir = path.toString();
                                    } catch (Exception e) {
                                        out.println(e);
                                    }
                                } else {
                                    out.println("Directory cannot be accessed.");
                                } // else

                            } else {
                                Path path = Paths.get(currentdir, parse[1]);
                                if (Files.exists(path)) {
                                    try {
                                        System.setProperty("user.dir", path.toString());
                                        currentdir = path.toString();
                                    } catch (Exception e) {
                                        out.println(e);
                                    }
                                } else {
                                    out.println("Directory cannot be accessed.");
                                } // if
                            } // if
                            directorytemp = new ArrayList<String>(Arrays.asList(currentdir.split("/")));
                            for (int i = 0; i < directorytemp.size(); i++) {
                                if (directorytemp.get(i).matches("..")) {
                                    directorytemp.remove(i);
                                    directorytemp.remove(i - 1);
                                    i = i - 2;
                                } // if
                            } // for
                            currentdir = "";
                            for (String dirFragment : directorytemp) {
                                if (dirFragment.length() > 0) {
                                    currentdir = currentdir + "/" + dirFragment;
                                } // if
                            }
                            out.println("Current directory: " + currentdir);
                            break;
                        }

                        case "mkdir":
                            String curDir = System.getProperty("user.dir");
                            String dirToCreate = curDir;

                            if (curDir == null) {
                                curDir = homedir;
                            } // if

                            // Add a / in front of the directory to create if it is not there already
                            if (!curDir.endsWith("/")) {
                                dirToCreate = dirToCreate + "/" + parse[1];
                            } else {
                                dirToCreate = dirToCreate + parse[1];
                            } // if

                            Files.createDirectories(Paths.get(dirToCreate));
                            out.println(parse[1] + " was successfully created.");

                            break;

                        case "asyncsend": {
                            //get file path
                            String filePath = in.readLine();

                            //get file size
                            long fileSize = Long.parseLong(in.readLine());

                            //get processId
                            int id = Integer.parseInt(in.readLine());

                            String filename = filePath;

                            while (filename.contains("/")) {
                                int index = filename.indexOf("/");
                                filename = filename.substring(index + 1, filename.length());
                            }

                            //get file
                            File file = new File(filename);

                            // Ensure the fileLocks map has a lock for this file
                            fileLocks.putIfAbsent(file.getAbsolutePath(), new ReentrantReadWriteLock());

                            // get lock
                            ReentrantReadWriteLock lock = fileLocks.get(file.getAbsolutePath());
                            lock.writeLock().lock(); // Acquire write lock for the specific file
                            try {
                                Thread.sleep(5000);
                            } catch (Exception e) {}

                            //write to file
                            receiveFile(filePath, clientSocket, fileSize, file, id);

                            //send response
                            if (isRunning(id)) {
                                out.println("File sent successfully.");
                                out.flush();
                            } else {
                                out.println("Put terminated successfully");
                            } //if

                            //release lock
                            lock.writeLock().unlock();
                            break;
                        }

                        case "asyncreceive": {
                            //get id
                            int pid = Integer.parseInt(in.readLine());

                            //get file path
                            String filePath = in.readLine();

                            //get file size
                            long fileSize = Long.parseLong(in.readLine());

                            //get file
                            File file = new File(filePath);

                            // Ensure the fileLocks map has a lock for this file
                            fileLocks.putIfAbsent(file.getAbsolutePath(), new ReentrantReadWriteLock());

                            // get lock
                            ReentrantReadWriteLock lock = fileLocks.get(file.getAbsolutePath());
                            lock.writeLock().lock(); // Acquire write lock for the specific file
                            try {
                                Thread.sleep(5000);
                            } catch (Exception e) {}

                            //write to file
                            sendFilePlus(file, clientSocket, pid);

                            //send response
                            /*out.println("rec");
                            out.flush();*/

                            //release lock
                            lock.writeLock().unlock();
                            break;
                        }

                        case "quit":
                            out.println("Closing connection...");
                            out.flush();
                            return;

                        default:
                            out.println("Unknown command: " + cmd);
                            out.flush();
                            break;
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    String x = clientSocket.getInetAddress().getHostAddress();
                    if (out != null) {
                        out.close();
                    }
                    if (in != null) {
                        in.close();
                        clientSocket.close();
                    }
                    System.out.println("Closed Connection: " + x);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // Functions

    private static void receiveFile(String filename, Socket socket, Long fileSize, File file, int pid) throws IOException {

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
            while (isRunning(pid) && totalBytesRead < fileSize
                    && (bytesRead = bis.read(buffer, 0, (int) Math.min(buffer.length, fileSize))) != -1) {
    
                //System.out.println("Bytes Read: " + bytesRead);
                fos.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
                }
                fos.flush();

        } finally {
            if (fos != null) {
                fos.close();
            }
            //cleanup
            if (!isRunning(pid)) {
                file.delete();
            }
    
        }
    }


    private static void sendFile(File file, Socket socket, int pid) throws IOException {

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
            file.createNewFile();
            int bytesRead = 0;
            while (isRunning(pid) && (bytesRead = fis.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
            bos.flush();
            // was hung on termination while loop in client, this was added to force stop

        } finally {
            if (fis != null) {
                fis.close();
            }

        }
    }

    private static void sendFilePlus(File file, Socket socket, int pid) throws IOException {

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
            file.createNewFile();
            int bytesRead = 0;
            while (isRunning(pid) && (bytesRead = fis.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
            bos.flush();
            // was hung on termination while loop in client, this was added to force stop
            if (bytesRead < file.length()) {
                socket.close();
                
            }

        } finally {
            if (fis != null) {
                fis.close();
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
