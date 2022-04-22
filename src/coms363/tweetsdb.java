package coms363;

import javax.swing.*;
import javax.swing.border.LineBorder;
import java.util.ArrayList;
import java.util.Iterator;

import java.awt.*;
import java.sql.*;

/*
 * Author: ComS 363 Teaching Staff
 * @Anji Xu
 */
public class tweetsdb {
	private static JFrame frame;
	private static JPanel pane;

	public static String[] loginDialog() {
		// asking for a username and password to access the database.
		String result[] = new String[2];
		JPanel panel = new JPanel(new GridBagLayout());
		GridBagConstraints cs = new GridBagConstraints();

		cs.fill = GridBagConstraints.HORIZONTAL;

		JLabel lbUsername = new JLabel("Username: ");
		cs.gridx = 0;
		cs.gridy = 0;
		cs.gridwidth = 1;
		panel.add(lbUsername, cs);

		JTextField tfUsername = new JTextField(20);
		cs.gridx = 1;
		cs.gridy = 0;
		cs.gridwidth = 2;
		panel.add(tfUsername, cs);

		JLabel lbPassword = new JLabel("Password: ");
		cs.gridx = 0;
		cs.gridy = 1;
		cs.gridwidth = 1;
		panel.add(lbPassword, cs);

		JPasswordField pfPassword = new JPasswordField(20);
		cs.gridx = 1;
		cs.gridy = 1;
		cs.gridwidth = 2;
		panel.add(pfPassword, cs);
		panel.setBorder(new LineBorder(Color.GRAY));

		String[] options = new String[] { "OK", "Cancel" };
		int ioption = JOptionPane.showOptionDialog(null, panel, "Login",
				JOptionPane.OK_OPTION,
				JOptionPane.PLAIN_MESSAGE, null, options, options[0]);

		// store the username in the first slot.
		// store the password in the second slot.

		if (ioption == 0) // pressing OK button
		{
			result[0] = tfUsername.getText();
			result[1] = new String(pfPassword.getPassword());
		}
		return result;
	}

	/**
	 * insert new tweet
	 * 
	 * @param conn Valid database connection
	 *             tidInput: id of tweet to check, post_day: tweet post day,
	 *             post_month: tweet post month, post_year: tweet post year,text:
	 *             tweet text, retweetCt: retweet count, user_screen_name: name of
	 *             user to check
	 */
	private static void insertNewTweet(Connection conn, String tidInput, int post_day, int post_month, int post_year,
			String texts, int retweetCt, String user_screen_name) {
		int tid = 0;
		if (tidInput.matches("[0-9]+")) {
			tid = Integer.parseInt(tidInput);
		} else {
			JOptionPane.showMessageDialog(null, "Invalid input",
					"Error", JOptionPane.ERROR_MESSAGE);
			throw new NullPointerException();
		}

		if (conn == null || user_screen_name.equals("") || tid == 0) {
			JOptionPane.showMessageDialog(null, "Invalid input",
					"Error", JOptionPane.ERROR_MESSAGE);
			throw new NullPointerException();
		}
		try {
			/*
			 * we want to make sure that all SQL statements for insertion
			 * of a new food are considered as one unit.
			 * That is all SQL statements between the commit and previous commit
			 * get stored permanently in the DBMS or all the SQL statements
			 * in the same transaction are rolled back.
			 * 
			 * By default, the isolation level is TRANSACTION_REPEATABLE_READ
			 * By default, each SQL statement is one transaction
			 * 
			 * conn.setAutoCommit(false) is to
			 * specify what SQL statements are in the same transaction
			 * by a developer.
			 * Several SQL statements can be put in one transaction.
			 */
			conn.setAutoCommit(false);
			// full protection against interference from other transaction
			// prevent dirty read
			// prevent unrepeatable reads
			// prevent phantom reads
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

			PreparedStatement ststmt = conn.prepareStatement(
					" insert into users (screen_name) values(?) ");
			PreparedStatement inststmt = conn.prepareStatement(
					" insert into tweets (tid, post_day, post_month, post_year, texts, retweetCt,user_screen_name) values(?,?,?,?,?,?,?) ");
			ststmt.setString(1, user_screen_name);
			inststmt.setInt(1, tid);
			inststmt.setInt(2, post_day);
			inststmt.setInt(3, post_month);
			inststmt.setInt(4, post_year);
			inststmt.setString(5, texts);
			inststmt.setInt(6, retweetCt);
			inststmt.setString(7, user_screen_name);

			// tell DBMS to insert the food into the table
			int rowC = ststmt.executeUpdate();
			int rowcount = inststmt.executeUpdate();
			// show how many rows are impacted, should be one row if
			// successful
			// if not successful, SQLException occurs.
			System.out.println("Number of rows in users updated:" + rowC);
			ststmt.close();
			System.out.println("Number of rows in tweets updated:" + rowcount);
			inststmt.close();

			// Tell DBMS to make sure all the changes you made from
			// the prior commit is saved to the database
			conn.commit();

			// Reset the autocommit to commit per SQL statement
			conn.setAutoCommit(true);

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * insert user
	 * 
	 * @param conn Valid database connection
	 *             user_screen_name: the name of the user to check
	 */
	private static void deleteUser(Connection conn, String user_screen_name) {
		if (conn == null || user_screen_name == null) {
			throw new NullPointerException();
		}
		try {
			conn.setAutoCommit(false);
			// full protection against interference from other transaction
			// prevent dirty read
			// prevent unrepeatable reads
			// prevent phantom reads
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			PreparedStatement ststmt = conn.prepareStatement(
					"delete from tweets where user_screen_name = ?");

			PreparedStatement inststmt = conn.prepareStatement(
					"delete from users where screen_name= ?");
			ststmt.setString(1, user_screen_name);
			inststmt.setString(1, user_screen_name);
			// tell DBMS to insert the food into the table
			int rowC = ststmt.executeUpdate();
			int rowcount = inststmt.executeUpdate();
			// show how many rows are impacted, should be one row if
			// successful
			// if not successful, SQLException occurs.
			System.out.println("Number of rows updated:" + rowC);
			ststmt.close();
			System.out.println("Number of rows updated:" + rowcount);
			inststmt.close();

			// Tell DBMS to make sure all the changes you made from
			// the prior commit is saved to the database
			conn.commit();

			// Reset the autocommit to commit per SQL statement
			conn.setAutoCommit(true);

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private static void top5User(Connection conn, String hashtag_name, int post_month, int post_year, String state) {
		if (conn == null || hashtag_name.equals("") || post_month == 0 || post_year == 0 || state.equals("")) {
			JOptionPane.showMessageDialog(null, "Invalid input",
					"Error", JOptionPane.ERROR_MESSAGE);
			throw new NullPointerException();
		}
		try {
			conn.setAutoCommit(false);
			// full protection against interference from other transaction
			// prevent dirty read
			// prevent unrepeatable reads
			// prevent phantom reads
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			PreparedStatement ststmt1 = conn.prepareStatement("DROP procedure if exists findUserPostingHashtag;");
			PreparedStatement procedure = conn.prepareStatement(
					"create procedure findUserPostingHashtag(hashName varchar(80),month int,year int,state varchar(80)) "
							+
							"BEGIN\n" +
							"select count(tweets.tid) as tweet_count ,users.screen_name,users.category " +
							"from hashtags, tweets,users " +
							"where hashtags.name = hashName and hashtags.tid=tweets.tid and " +
							"tweets.post_month = month and tweets.post_year = year " +
							"and tweets.user_screen_name=users.screen_name " +
							"and users.state = state " +
							"group by tweets.user_screen_name order by count(tweets.tid) desc limit 5;\n " +
							"END;");
			ststmt1.execute();
			ststmt1.close();
			procedure.execute();
			procedure.close();
			System.out.println("Stored Procedure created!");

			CallableStatement CallProd = conn.prepareCall("{call findUserPostingHashtag(?,?,?,?)}");

			// CallProd.registerOutParameter(1,Types.VARCHAR);
			// CallProd.registerOutParameter(2, Types.INTEGER);
			// CallProd.registerOutParameter(3, Types.INTEGER);
			// CallProd.registerOutParameter(4, Types.VARCHAR);

			CallProd.setString(1, hashtag_name);
			CallProd.setInt(2, post_month);
			CallProd.setInt(3, post_year);
			CallProd.setString(4, state);
			// CallProd.executeUpdate();
			CallProd.execute();
			System.out.println("Stored Procedure called!\n");

			ResultSet rs = CallProd.executeQuery();
			// while (rs.next()) {
			// System.out.println(
			// rs.getInt("tweet_count") + "\t" + rs.getString("screen_name") + "\t"
			// + rs.getString("category"));
			// }
			ResultSetMetaData resultSetMetaData = rs.getMetaData();
			int ColumnCount = resultSetMetaData.getColumnCount();
			int[] columnMaxLengths = new int[ColumnCount];
			String[] columnStr = new String[ColumnCount];
			ArrayList<String[]> results = new ArrayList<>();
			// 按行遍历
			while (rs.next()) {
				// 保存当前行所有列
				columnStr = new String[ColumnCount];
				// 获取属性值.
				for (int i = 0; i < ColumnCount; i++) {
					// 获取一列
					columnStr[i] = rs.getString(i + 1);
					// 计算当前列的最大长度
					columnMaxLengths[i] = Math.max(columnMaxLengths[i],
							(columnStr[i] == null) ? 0 : columnStr[i].length());
				}
				// 缓存这一行.
				results.add(columnStr);
			}
			printSeparator(columnMaxLengths);
			printColumnName(resultSetMetaData, columnMaxLengths);
			printSeparator(columnMaxLengths);

			// 遍历集合输出结果
			Iterator<String[]> iterator = results.iterator();
			while (iterator.hasNext()) {
				columnStr = iterator.next();
				for (int i = 0; i < ColumnCount; i++) {
					// System.out.printf("|%" + (columnMaxLengths[i] + 1) + "s", columnStr[i]);
					System.out.printf("|%" + columnMaxLengths[i] + "s", columnStr[i]);
				}
				System.out.println("|");
			}
			printSeparator(columnMaxLengths);
			rs.close();
			CallProd.close();

			conn.commit();

			// Reset the autocommit to commit per SQL statement
			conn.setAutoCommit(true);

		} catch (

		SQLException e) {
			e.printStackTrace();
		}
	}

	private static void printColumnName(ResultSetMetaData resultSetMetaData,
			int[] columnMaxLengths) throws SQLException {
		int columnCount = resultSetMetaData.getColumnCount();
		for (int i = 0; i < columnCount; i++) {
			// System.out.printf("|%" + (columnMaxLengths[i] + 1) + "s",
			resultSetMetaData.getColumnName(i + 1);
			System.out.printf("|%" + columnMaxLengths[i] + "s",
					resultSetMetaData.getColumnName(i + 1));
		}
		System.out.println("|");
	}

	private static void printSeparator(int[] columnMaxLengths) {
		for (int i = 0; i < columnMaxLengths.length; i++) {
			System.out.print("+");
			// for (int j = 0; j < columnMaxLengths[i] + 1; j++) {
			for (int j = 0; j < columnMaxLengths[i]; j++) {
				System.out.print("-");
			}
		}
		System.out.println("+");
	}

	public static void main(String[] args) {
		// useSSL=false means plain text allowed
		// String dbServer = "jdbc:mysql://localhost:3306/fooddb?useSSL=false";
		// useSSL=true; data are encrypted when sending between DBMS and
		// this program

		// String dbServer = "jdbc:mysql://localhost:3306/fooddb?useSSL=true";
		// String dbServer = "jdbc:mysql://127.0.0.1:3306/project?useSSL=true";
		String dbServer = "jdbc:mysql://localhost:3306/project?useSSL=true";
		String userName = "";
		String password = "";

		String result[] = loginDialog();
		userName = result[0];
		password = result[1];

		Connection conn = null;
		Statement stmt = null;
		if (result[0] == null || result[1] == null) {
			System.out.println("Terminating: No username nor password is given");
			return;
		}
		try {
			// load JDBC driver
			// must be in the try-catch-block
			Class.forName("com.mysql.cj.jdbc.Driver");
			conn = DriverManager.getConnection(dbServer, userName, password);

			stmt = conn.createStatement();

			String option = "";
			String instruction = "Option a: Insert a new Tweet into the tweets relation.\n"
					+ "Option b: Delete a user from the users relation given the user screen name.\n"
					+ "Option c: Report top 5 Twitter users who used a give hastag the most in their tweets posted in given month of a given year\n"
					+ "Option e: Exit problem \n";

			while (true) {
				option = JOptionPane.showInputDialog(instruction);
				if (option.equals("a")) {
					String tid = null;
					String user_screen_name = null;
					int post_day = 0;
					int post_month = 0;
					int post_year = 0;
					String texts = null;
					int retweetCt = 0;

					JTextField tidField;
					JTextField user_screen_nameField;
					JTextField post_dayField;
					JTextField post_monthField;
					JTextField post_yearField;
					JTextField textsField;
					JTextField retweetCtField;

					pane = new JPanel();
					pane.setLayout(new GridLayout(0, 2, 2, 2));

					tidField = new JTextField(5);
					user_screen_nameField = new JTextField(5);
					post_dayField = new JTextField(5);
					post_monthField = new JTextField(5);
					post_yearField = new JTextField(5);
					textsField = new JTextField(5);
					retweetCtField = new JTextField(5);

					pane.add(new JLabel("Enter tid: "));
					pane.add(tidField);

					pane.add(new JLabel("Enter user_screen_name: "));
					pane.add(user_screen_nameField);

					pane.add(new JLabel("Enter the post_day: "));
					pane.add(post_dayField);

					pane.add(new JLabel("Enter the post_month: "));
					pane.add(post_monthField);

					pane.add(new JLabel("Enter the post_year: "));
					pane.add(post_yearField);

					pane.add(new JLabel("Enter the text post: "));
					pane.add(textsField);

					pane.add(new JLabel("Enter the retweetCt number: "));
					pane.add(retweetCtField);

					int newoption = JOptionPane.showConfirmDialog(frame, pane, "Please fill the fields",
							JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.INFORMATION_MESSAGE);

					if (newoption == JOptionPane.YES_OPTION) {
						String tidInput = tidField.getText();
						String usernameInput = user_screen_nameField.getText();
						String post_dayInput = post_dayField.getText();
						String post_monthInput = post_monthField.getText();
						String post_yearInput = post_yearField.getText();
						String textInput = textsField.getText();
						String retweetCtInput = retweetCtField.getText();

						try {
							tid = tidInput;
							user_screen_name = usernameInput;
							post_day = Integer.parseInt(post_dayInput);
							post_month = Integer.parseInt(post_monthInput);
							post_year = Integer.parseInt(post_yearInput);
							texts = textInput;
							retweetCt = Integer.parseInt(retweetCtInput);
						} catch (Exception e) {
							e.printStackTrace();
						}
						// int tid = 0;
						// tid = Integer.parseInt(JOptionPane.showInputDialog("Enter tid: "));
						// int post_day = 0;
						// post_day = Integer.parseInt(JOptionPane.showInputDialog("Enter the post_day:
						// "));
						// int post_month = 0;
						// post_month = Integer.parseInt(JOptionPane.showInputDialog("Enter the
						// post_month: "));
						// int post_year = 0;
						// post_year = Integer.parseInt(JOptionPane.showInputDialog("Enter the
						// post_year: "));
						// String texts = null;
						// texts = JOptionPane.showInputDialog("Enter the text post: ");
						// int retweetCt = 0;
						// retweetCt = Integer.parseInt(JOptionPane.showInputDialog("Enter the
						// retweetCt: "));
						// String user_screen_name = null;
						// user_screen_name = JOptionPane.showInputDialog("Enter user_screen_name: ");
						insertNewTweet(conn, tid, post_day, post_month, post_year, texts, retweetCt,
								user_screen_name);

					}
				} else if (option.equals("b")) {
					String user_screen_name = null;
					// String usernameInput = null;
					String confirm = null;
					try {
						user_screen_name = JOptionPane.showInputDialog("Enter user_screen_name: ");
						confirm = JOptionPane.showInputDialog("Enter 'y' to continue: ");
						if (confirm == "y") {
							deleteUser(conn, user_screen_name);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					deleteUser(conn, user_screen_name);
				} else if (option.equals("c")) {
					String hashtag_name = null;
					int post_month = 0;
					int post_year = 0;
					String state = null;

					JTextField hashtagField;
					JTextField post_monthField;
					JTextField post_yearField;
					JTextField stateField;

					pane = new JPanel();
					pane.setLayout(new GridLayout(0, 2, 2, 2));

					hashtagField = new JTextField(5);
					post_monthField = new JTextField(5);
					post_yearField = new JTextField(5);
					stateField = new JTextField(5);

					pane.add(new JLabel("Enter the hashtag_name: "));
					pane.add(hashtagField);

					pane.add(new JLabel("Enter the post_month: "));
					pane.add(post_monthField);

					pane.add(new JLabel("Enter the post_year: "));
					pane.add(post_yearField);

					pane.add(new JLabel("Enter the state: "));
					pane.add(stateField);

					int newoption = JOptionPane.showConfirmDialog(frame, pane, "Please fill the fields",
							JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.INFORMATION_MESSAGE);
					if (newoption == JOptionPane.YES_OPTION) {
						String hashtageInput = hashtagField.getText();
						String post_monthInput = post_monthField.getText();
						String post_yearInput = post_yearField.getText();
						String stateInput = stateField.getText();

						try {
							hashtag_name = hashtageInput;
							post_month = Integer.parseInt(post_monthInput);
							post_year = Integer.parseInt(post_yearInput);
							state = stateInput;
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					top5User(conn, hashtag_name, post_month, post_year, state);
				} else if (option.equals("e")) {
					break;
				}
			}
			// close the statement
			if (stmt != null)
				stmt.close();
			// close the connection
			if (conn != null)
				conn.close();
		} catch (

		Exception e) {

			System.out.println("Program terminates due to errors or user cancelation");
			e.printStackTrace(); // for debugging;
		}
	}

}