package coms363;

import com.mysql.cj.jdbc.exceptions.MySQLQueryInterruptedException;

import javax.swing.*;
import javax.swing.border.LineBorder;
import java.awt.*;
import java.sql.*;

/*
 * Author: Anji Xu and ComS 363 Teaching Staff
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
	 * @param stmt
	 * @param sqlQuery
	 * @throws SQLException
	 */
	// run a static SQL query
	private static void runQuery(Statement stmt, String sqlQuery) throws SQLException {
		// ResultSet is used to store the data returned by DBMS when issuing a static
		// query
		ResultSet rs;

		// ResultSetMetaData is used to find meta data about the data returned
		ResultSetMetaData rsMetaData;
		String toShow;

		// Send the SQL query to the DBMS
		rs = stmt.executeQuery(sqlQuery);

		// get information about the returned result.
		rsMetaData = rs.getMetaData();
		System.out.println(sqlQuery);
		toShow = "";

		// iterate through each item in the returned result
		while (rs.next()) {
			// concatenate the columns in each row
			for (int i = 0; i < rsMetaData.getColumnCount(); i++) {

				toShow += rs.getString(i + 1) + ", ";
			}
			toShow += "\n";
		}
		// show the dialog box with the returned result by DBMS
		JOptionPane.showMessageDialog(null, toShow);
		rs.close();
	}

	private static void insertNewTweet(Connection conn, String tidInput, int post_day, int post_month, int post_year, String texts, int retweetCt, String user_screen_name) {
		int tid = 0;
		if(tidInput.matches("[0-9]+")){
			tid = Integer.parseInt(tidInput);
		}else{
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
			System.out.println("flag1");
			PreparedStatement ststmt = conn.prepareStatement(
					"delete from tweets where user_screen_name = ?");

			PreparedStatement inststmt = conn.prepareStatement(
					"delete from users where screen_name= ?");
					System.out.println("flag2");
			ststmt.setString(1, user_screen_name);
			inststmt.setString(1, user_screen_name);
			System.out.println("flag3");
			// tell DBMS to insert the food into the table
			int rowC = ststmt.executeUpdate();
			int rowcount = inststmt.executeUpdate();
			System.out.println("flag4");
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
			String sqlQuery = "";

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
						} catch (NumberFormatException e) {

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
					//String usernameInput = null;
					String confirm = null;
					try {
						user_screen_name = JOptionPane.showInputDialog("Enter user_screen_name: ");
						confirm = JOptionPane.showInputDialog("Enter 'y' to continue: ");
						if(confirm == "y"){
							deleteUser(conn, user_screen_name);
						}
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
					deleteUser(conn, user_screen_name);
				} else if (option.equals("c")) {
					sqlQuery = "select f.fname from food f where f.fid not in (select r.fid from recipe r inner join ingredient i on i.iid = r.iid where i.iname = 'Green Onion');";
					runQuery(stmt, sqlQuery);
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
