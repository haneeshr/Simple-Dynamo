package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String[] REMOTE_PORTS = {"11108","11112","11116","11120","11124"};
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;

	static final String DB = "MYDB";
	static final String HEAD = "HEAD";
	static final String MIDDLE = "MIDDLE";
	static final String TAIL = "TAIL";
	static final String COOD = "COOD";
	static final String KEY_FIELD = "key";
	static final String VALUE_FIELD = "value";
	static final String SEP = "==";

	static final int INSERT = 0;
	static final int QUERY = 1;
	//	static final int DELETE = 2;
	static final int RECOVERY = 3;

	SQLiteOpenHelper sqLiteOpenHelper;
	SQLiteDatabase sqLiteDatabase;
	String myport;
	String myhash;

	LinkedList<String> hashvals;
	HashMap<String, String> hash_ports;

	String succ;
	String pred;
	String failure_backup = "";
	boolean recovered = false;

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length()-4);
		myport = String.valueOf(Integer.parseInt(portStr)*2);


		try {
			myhash = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		hashvals = new LinkedList<String>();
		hash_ports = new HashMap<String, String>();

		sqLiteOpenHelper = new SQLiteOpenHelper(getContext(), DB, null, 1) {
			String CREATE_TABLE_HEAD = "CREATE TABLE IF NOT EXISTS " + HEAD + " ( key TEXT PRIMARY KEY, value TEXT)";

			@Override
			public void onCreate(SQLiteDatabase db) {
				db.execSQL(CREATE_TABLE_HEAD);
			}

			@Override
			public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

			}
		};

		init_ring();

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(RECOVERY)).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		return false;
	}

	private void init_ring() {
		int start = 5554;
		for(int i=0; i<5; i++, start+=2){
			try{
				String hashval = genHash(String.valueOf(start));
				hash_ports.put(hashval, String.valueOf(start*2));
				hashvals.add(hashval);
			}catch (NoSuchAlgorithmException e){
				Log.e("doInBackground: ", e.toString());
			}
		}

		Collections.sort(hashvals);

		int index = hashvals.indexOf(myhash);
		succ = hash_ports.get(hashvals.get((index+1)%5));
		pred = hash_ports.get(hashvals.get((index+4)%5));

		Log.i(TAG, "init_ring: "+hashvals);
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key = (String) values.get(KEY_FIELD);
		String value = (String) values.get(VALUE_FIELD);
		Log.d(TAG, "insert: request to insert"+key+"&&"+value);
		String msg = key +"&&"+value;

		sqLiteDatabase = sqLiteOpenHelper.getReadableDatabase();
		Cursor cursor = sqLiteDatabase.query(HEAD, null, KEY_FIELD + " = ?", new String[]{key}, null, null, null);
		try {
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(INSERT), msg).get();
		}catch (Exception e){
			e.printStackTrace();
		}
		Log.d(TAG, "insert: done");
		return null;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
		if(selection.equals("@")){
			sqLiteDatabase = sqLiteOpenHelper.getReadableDatabase();
			return sqLiteDatabase.rawQuery("select * from "+HEAD, null);

		}else {
			try {
				Log.d(TAG, "query: "+selection);
				String result = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(QUERY), selection).get();

				Log.d(TAG, "query: result for "+ selection +":"+result);
				String keyvals[] = result.split(SEP);
				for (String keyval : keyvals) {
					matrixCursor.addRow(new String[]{keyval.split("&&")[0], keyval.split("&&")[1]});
				}
			} catch (Exception e){
				e.printStackTrace();
			}
		}
		return matrixCursor;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		String clause = "key=?";

		try {
			sqLiteDatabase = sqLiteOpenHelper.getWritableDatabase();
			if (selection.equals("*") || selection.equals("@")) {
				sqLiteDatabase.delete(HEAD, null, null);
			} else {
				sqLiteDatabase.delete(HEAD, clause, new String[]{selection});
			}
		}catch (Exception e){
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private ArrayList<String> getPorts(String key) {
		ArrayList<String> res = new ArrayList<String>();
		String hash = "";

		try {
			hash = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		for(int i=0; i<hashvals.size(); i++){
			if(hash.compareTo(hashvals.get(i)) < 0){
				res.add(hash_ports.get(hashvals.get(i)));
				res.add(hash_ports.get(hashvals.get((i+1)%5)));
				res.add(hash_ports.get(hashvals.get((i+2)%5)));
				break;
			}
		}

		if(res.size() == 0){
			res.add(hash_ports.get(hashvals.get(0)));
			res.add(hash_ports.get(hashvals.get(1)));
			res.add(hash_ports.get(hashvals.get(2)));
		}
		return res;
	}

	private class ServerTask extends AsyncTask<ServerSocket, Void, Void>{

		@Override
		protected Void doInBackground(ServerSocket... params) {

			ServerSocket serverSocket = params[0];
			Socket client;
			InputStream inputStream;
			BufferedReader bufferedReader;
			OutputStream outputStream;
			BufferedWriter bufferedWriter;
			String msgin;

			while(true){

				try {
					client = serverSocket.accept();
					inputStream = client.getInputStream();
					bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
					msgin = bufferedReader.readLine();
					if(msgin == null){
						continue;
					}
					String msginsplit[] = msgin.split(SEP);
					int operation = Integer.parseInt(msginsplit[0]);

					switch (operation){
						case RECOVERY:

							if(failure_backup.equals("")){
								failure_backup = "NOTHINGTORECOVER";
							}
							outputStream = client.getOutputStream();
							bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
							bufferedWriter.write(failure_backup+"\n");
							bufferedWriter.flush();
							client.close();
							failure_backup = "";
							break;
						case INSERT:
//							msgin formatted as OPERATION==SOURCE==DESTINATION==PORTSTOFORWARD==KEY&&VAL
							String source = msginsplit[1];
							String destination = msginsplit[2];
							String keyval = msginsplit[4];
							ContentValues contentValues = new ContentValues();
							contentValues.put(KEY_FIELD, keyval.split("&&")[0]);
							contentValues.put(VALUE_FIELD, keyval.split("&&")[1]);
							Log.d(TAG, "doInBackground: insert req " + keyval);
							sqLiteDatabase = sqLiteOpenHelper.getWritableDatabase();
							sqLiteDatabase.insertWithOnConflict(HEAD, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
							Log.d(TAG, "doInBackground: inserted " + keyval);

							String remoteport;
							String msgToSend;

							if(destination.equals(TAIL)){
								outputStream = client.getOutputStream();
								bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
								bufferedWriter.write("inserted"+"\n");
								bufferedWriter.flush();
								client.close();
							}else{
								if(destination.equals(HEAD)){//I am head for this key
									remoteport = msginsplit[3].split("&&")[0];
									String tailPort = msginsplit[3].split("&&")[1];
									msgToSend = INSERT + SEP + HEAD + SEP + MIDDLE + SEP + tailPort + SEP + keyval;
									String ack = sendMessage(msgToSend, remoteport);
									if(ack == null){
//										middle node dead....send msg to tail
										failure_backup += keyval + SEP;
										msgToSend = INSERT + SEP + HEAD + SEP + TAIL + SEP + "DUMMYPORT" + SEP + keyval;
										sendMessage(msgToSend, tailPort);
									}
									outputStream = client.getOutputStream();
									bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
									bufferedWriter.write("inserted"+"\n");
									bufferedWriter.flush();
									client.close();

								}else{//I am middle node for this key
									remoteport = msginsplit[3];
									msgToSend = INSERT+SEP+MIDDLE+SEP+TAIL+SEP+"DUMMYPORT"+SEP+keyval;
									if(source.equals(HEAD)) {
										outputStream = client.getOutputStream();
										bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
										bufferedWriter.write("inserted" + "\n");
										bufferedWriter.flush();
										client.close();
										String ack = sendMessage(msgToSend, remoteport);
										if(ack == null){
											failure_backup += keyval + SEP;
										}
									}else{
										failure_backup += keyval + SEP;
										sendMessage(msgToSend, remoteport);
										outputStream = client.getOutputStream();
										bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
										bufferedWriter.write("inserted" + "\n");
										bufferedWriter.flush();
										client.close();
									}
								}
							}

							break;
						case QUERY:
							destination = msgin.split(SEP)[1];
							String key = msgin.split(SEP)[2];
							String ack = "";
							Cursor cursor;

							if(recovered) {
								sqLiteDatabase = sqLiteOpenHelper.getReadableDatabase();
								if (key.equals("*")) {
//								return all values in your table would do this
									cursor = sqLiteDatabase.rawQuery("select * from " + HEAD, null);
								} else {
									cursor = sqLiteDatabase.query(HEAD, null, KEY_FIELD + " = ?", new String[]{key}, null, null, null);
								}

								cursor.moveToFirst();
								int keyindex = cursor.getColumnIndex(KEY_FIELD);
								int valindex = cursor.getColumnIndex(VALUE_FIELD);
								while (!cursor.isAfterLast()) {
									ack += cursor.getString(keyindex) + "&&" + cursor.getString(valindex) + SEP;
									cursor.moveToNext();
								}
								cursor.close();
							}

							outputStream = client.getOutputStream();
							bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
							bufferedWriter.write(ack+"\n");
							bufferedWriter.flush();
							client.close();
							Log.d(TAG, "doInBackground: responed to query = " +ack);
							break;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private String sendMessage(String msgToSend, String remote_port) {

		try{
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote_port));
			OutputStream outputStream = socket.getOutputStream();
			BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
			bufferedWriter.write(msgToSend+"\n");
			bufferedWriter.flush();

			InputStream inputStream = socket.getInputStream();
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
			String ack = bufferedReader.readLine();
			socket.close();
			return ack;
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

	private class ClientTask extends AsyncTask<String, Void, String>{

		@Override
		protected String doInBackground(String... params) {

			String key;
			ArrayList<String> toPorts;
			String msgToSend;
			String ack = null;
			int operation = Integer.parseInt(params[0]);

			switch (operation){
				case RECOVERY:
					msgToSend = RECOVERY + SEP + HEAD + SEP + TAIL;
					ack = sendMessage(msgToSend, succ);
					if(ack != null) {
						if (!ack.contains("NOTHINGTORECOVER")) {
							for (String keyval : ack.split(SEP)) {
								ContentValues contentValues = new ContentValues();
								contentValues.put(KEY_FIELD, keyval.split("&&")[0]);
								contentValues.put(VALUE_FIELD, keyval.split("&&")[1]);
								sqLiteDatabase = sqLiteOpenHelper.getWritableDatabase();
								sqLiteDatabase.insertWithOnConflict(HEAD, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
							}
						}
						Log.d(TAG, "doInBackground: recovered " + ack );
					}
					ack = sendMessage(msgToSend, pred);
					if(ack != null) {
						if (!ack.contains("NOTHINGTORECOVER")) {
							for (String keyval : ack.split(SEP)) {
								ContentValues contentValues = new ContentValues();
								contentValues.put(KEY_FIELD, keyval.split("&&")[0]);
								contentValues.put(VALUE_FIELD, keyval.split("&&")[1]);
								sqLiteDatabase = sqLiteOpenHelper.getWritableDatabase();
								sqLiteDatabase.insertWithOnConflict(HEAD, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
							}
						}
						Log.d(TAG, "doInBackground: recovered " + ack );
					}
					recovered = true;
					break;
				case INSERT:
					key = params[1].split("&&")[0];
					toPorts = getPorts(key);
					String succ_ports = toPorts.get(1) + "&&" + toPorts.get(2);
					msgToSend = INSERT + SEP + COOD + SEP + HEAD + SEP + succ_ports + SEP + params[1];

					ack = sendMessage(msgToSend, toPorts.get(0));
					if(ack == null){
//							handle failure
						msgToSend = INSERT + SEP + COOD + SEP + MIDDLE + SEP + toPorts.get(2) + SEP + params[1];
						sendMessage(msgToSend, toPorts.get(1));
					}
					break;
				case QUERY:
					key = params[1];
					msgToSend = QUERY + SEP + HEAD + SEP + key;
					if(key.equals("*")){
//						query all the nodes
						ack = "";
						for(String remote_port : REMOTE_PORTS){
							String response = sendMessage(msgToSend, remote_port);
							if(response!=null) {
								ack += response;
							}
						}
					}else{
//						query the target node
						toPorts = getPorts(key);
						for(String port : toPorts){
							Log.d(TAG, "doInBackground: querying"+port);
							ack  = sendMessage(msgToSend, port);
							if(ack!=null && !ack.equals("")){
								Log.d(TAG, "doInBackground: query result"+ack);
								break;
							}
						}
					}
					break;
			}
			return ack;
		}
	}
}
