import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;

/**
 * Runs queries against a back-end database
 */
public class Query
{
  private String configFilename;
  private Properties configProps = new Properties();

  private String jSQLDriver;
  private String jSQLUrl;
  private String jSQLUser;
  private String jSQLPassword;

  // DB Connection
  private Connection conn;

  // Logged In User
  private String username; // customer username is unique
  private boolean isLoggedIn = false;
  private int possibleBookingCount = 0;
  private Flight[] bookingOptions;
  private int masterResId = 1;
  private int resetCount = 0;
  private Flight[] capacityReset = new Flight[500];
  private Set<Integer> f = new HashSet<Integer>();

  // Canned queries

  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Capacities WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;

  private static final String MAKE_NEW_USER = "INSERT INTO Users VALUES (?, ?, ?)";
  private PreparedStatement newUserStatement;

  private static final String LOGIN_CHECK = "SELECT COUNT(*) as status FROM Users WHERE username = ? AND password = ?";
  private PreparedStatement loginStatement;

  private static final String SEARCH_ONE_HOP =    "SELECT TOP (?) F.fid AS fid, F.day_of_month AS day_of_month, F.carrier_id AS carrier_id, "
                                                + "F.flight_num AS flight_num, F.origin_city AS origin_city, F.dest_city AS dest_city, F.actual_time AS actual_time, "
						+ "F.capacity AS capacity, F.price AS price "
						+ "FROM Flights AS F "
						+ "WHERE F.origin_city = ? AND F.dest_city = ? AND F.day_of_month = ? AND F.actual_time IS NOT NULL AND F.canceled = 0 "
						+ "ORDER BY F.actual_time ASC, F.fid ASC";
  private PreparedStatement searchOneHopStatement;

  private static final String SEARCH_TWO_HOP =    "SELECT TOP (?) F.fid AS fid, F.day_of_month AS day_of_month, F.carrier_id AS carrier_id, "
						+ "F.flight_num AS flight_num, F.origin_city AS origin_city, F.dest_city AS dest_city, F.actual_time AS actual_time, "
  						+ "F.capacity AS capacity, F.price AS price, "
						+ "F2.fid AS fid2, F2.day_of_month AS day_of_month2, F2.carrier_id AS carrier_id2, "
						+ "F2.flight_num AS flight_num2, F2.origin_city AS origin_city2, F2.dest_city AS dest_city2, "
						+ "F2.actual_time AS actual_time2, F.actual_time + F2.actual_time AS total_time, "
						+ "F2.capacity AS capacity2, F2.price AS price2 "
						+ "FROM Flights F, Flights F2 "
						+ "WHERE F.origin_city = ? AND F2.dest_city = ? AND F.day_of_month = ? AND F.actual_time IS NOT NULL AND F.canceled = 0 "
						+ "AND F.day_of_month = F2.day_of_month AND F.dest_city = F2.origin_city AND F2.actual_time IS NOT NULL "
						+ "AND F2.canceled = 0 "
						+ "ORDER BY total_time ASC, F.fid ASC";
  private PreparedStatement searchTwoHopStatement;

  private static final String CHECK_RESERVATIONS = "SET NOCOUNT ON; SELECT COUNT(*) AS numRes FROM Reservations WHERE username = ? AND reservationDate = ? ";
  private PreparedStatement checkReservationsStatement;

  private static final String REDUCE_CAPACITY = "UPDATE Capacities SET capacity = capacity - 1 WHERE fid = ? ";
  private PreparedStatement reduceCapacityStatement; 

  private static final String CREATE_RESERVATION = "INSERT INTO Reservations VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
  private PreparedStatement createReservationStatement;

  private static final String CURRENT_RESERVATIONS = "SET NOCOUNT ON; "
						   + "SELECT F.fid AS fid, F.day_of_month AS day_of_month, F.carrier_id AS carrier_id, "
						   + "F.flight_num AS flight_num, F.origin_city AS origin_city, F.dest_city AS dest_city, F.actual_time AS actual_time, "
  						   + "F.capacity AS capacity, F.price AS price, "
						   + "F2.fid AS fid2, F2.day_of_month AS day_of_month2, F2.carrier_id AS carrier_id2, "
						   + "F2.flight_num AS flight_num2, F2.origin_city AS origin_city2, F2.dest_city AS dest_city2, "
						   + "F2.actual_time AS actual_time2, F.actual_time + F2.actual_time AS total_time, "
						   + "F2.capacity AS capacity2, F.price AS price2, R.reservation_id as resID, R.paid as paid, R.canceled as canceled "
						   + "FROM Reservations R INNER JOIN Flights F ON R.flight_id = F.fid LEFT OUTER JOIN Flights F2 ON R.flight_id2 = F2.fid "
                                                   + "WHERE R.username = ? AND R.canceled = 0" ;

  private PreparedStatement retrieveReservationsStatement;
 
  private static final String USER_RESERVATIONS =  "SET NOCOUNT ON; SELECT * FROM Reservations WHERE reservation_id = ? AND username = ? ";
  private PreparedStatement listReservationStatement; 

  private static final String USER_BALANCE = "SELECT * FROM Users WHERE username = ? ";
  private PreparedStatement retrieveBalanceStatement;

  private static final String MAKE_PAYMENT = "UPDATE Users SET initAmount = ? WHERE username = ?; "
					   + "UPDATE Reservations SET paid = ? WHERE reservation_id = ? ";
  private PreparedStatement makePaymentStatement;

  private static final String GET_REFUND = "UPDATE Reservations SET paid = 0 WHERE reservation_id = ?; "
  					 + "UPDATE Reservations SET canceled = 1 WHERE reservation_id = ?; "
					 + "UPDATE Users SET initAmount = initAmount + ? WHERE username = ? ";
  private PreparedStatement refundStatement;

  private static final String CLEAR_USERS = "DELETE FROM Users ";
  private PreparedStatement clearUsersStatement;
  
  private static final String CLEAR_ITINERARIES = "DELETE FROM Itineraries";
  private PreparedStatement clearItinierariesStatement;

  private static final String CLEAR_RESERVATIONS = "DELETE FROM Reservations";
  private PreparedStatement clearReservationsStatement;

  private static final String CANCEL_ADJUST = "UPDATE Reservations SET canceled = 1 WHERE reservation_id = ?; "
                                            + "UPDATE Capacities SET capacity = ? WHERE fid = ? ";
  private PreparedStatement cancelAdjustStatement;
  
  private static final String RESET_CAPACITY = "UPDATE Capacities SET capacity = Flights.capacity FROM Flights WHERE fid = ? ";
  private PreparedStatement adjustCapacityStatement; 

  //private static final String COUNT_RESERVATIONS = "SELECT COUNT(*) AS count FROM Reservations ";
  //private PreparedStatement checkBookingCountStatement;

  // transactions
  private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
  private PreparedStatement beginTransactionStatement;

  private static final String COMMIT_SQL = "COMMIT TRANSACTION";
  private PreparedStatement commitTransactionStatement;

  private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
  private PreparedStatement rollbackTransactionStatement;

  
  class Flight
  {
    public int itineraryNum;
   
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public int flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    public int fid2 = 0;
    public int dayOfMonth2 = 0;
    public String carrierId2;
    public int flightNum2 = 0;
    public String originCity2;
    public String destCity2;
    public int time2 = 0;
    public int capacity2 = 0;
    public int price2 = 0;

   
    // constructor for single hop 
    public Flight(int fid, int dayOfMonth, String carrierId, int flightNum, String originCity, 
                  String destCity, int time, int capacity, int price) {
      this.fid = fid;
      this.dayOfMonth = dayOfMonth;
      this.carrierId = carrierId;
      this.flightNum = flightNum;
      this.originCity = originCity;
      this.destCity = destCity;
      this.time = time;
      this.capacity = capacity;
      this.price = price;
      this.itineraryNum = itineraryNum;	
    }

    // constructor for two hop 
    public Flight(int fid, int dayOfMonth, String carrierId, int flightNum, String originCity, 
                  String destCity, int time, int capacity, int price, int fid2, int dayOfMonth2, String carrierId2, int flightNum2, String originCity2, 
                  String destCity2, int time2, int capacity2, int price2) {
      
      this.fid = fid;
      this.dayOfMonth = dayOfMonth;
      this.carrierId = carrierId;
      this.flightNum = flightNum;
      this.originCity = originCity;
      this.destCity = destCity;
      this.time = time;
      this.capacity = capacity;
      this.price = price;

      this.fid2 = fid2;
      this.dayOfMonth2 = dayOfMonth2;
      this.carrierId2 = carrierId2;
      this.flightNum2 = flightNum2;
      this.originCity2 = originCity2;
      this.destCity2 = destCity2;
      this.time2 = time2;
      this.capacity2 = capacity2;
      this.price2 = price2;
    }
   
    //constructor for single hop Flight which needs to have its capacities reset when clearTables() is called
    public Flight(int fid, int capacity) {
      this.fid = fid;
      this.capacity = capacity;
    }

    //constructor for double hop Flights which need to have their capacities reset when clearTables() is called
    public Flight(int fid, int fid2, int capacity, int capacity2) {
      this.fid = fid;
      this.fid2 = fid2;
      this.capacity = capacity;
      this.capacity2 = capacity2;	
    }

    @Override
    public String toString()
    {
      return  "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price + "\n";
    }

    public String oneHopString() {
      return  "1 flight(s), " + time + " minutes" + "\n" + 
              "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price + "\n";  
    }

    public String twoHopString() {
      return  "2 flight(s), " + (time + time2) + " minutes" + "\n" +
              "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price + "\n" +
              "ID: " + fid2 + " Day: " + dayOfMonth2 + " Carrier: " + carrierId2 +
              " Number: " + flightNum2 + " Origin: " + originCity2 + " Dest: " + destCity2 + " Duration: " + time2 +
              " Capacity: " + capacity2 + " Price: " + price2 + "\n";  
    }

  }

  public Query(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /* Connection code to SQL Azure.  */
  public void openConnection() throws Exception
  {
    configProps.load(new FileInputStream(configFilename));

    jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    jSQLUrl = configProps.getProperty("flightservice.url");
    jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement

    /* You will also want to appropriately set the transaction's isolation level through:
       conn.setTransactionIsolation(...)
       See Connection class' JavaDoc for details.
    */
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created. Do not drop any tables and do not
   * clear the flights table. You should clear any tables you use to store reservations
   * and reset the next reservation ID to be 1.
   */
  public void clearTables ()
  {
    try {    
       
      fixCapacity();

      clearReservationsStatement.clearParameters();
      clearReservationsStatement.executeUpdate();

      clearUsersStatement.clearParameters();
      clearUsersStatement.executeUpdate();

      clearItinierariesStatement.clearParameters();
      clearItinierariesStatement.executeUpdate();
           
      masterResId = 1;
      resetCount = 0;
      f = null;
      
    } catch(Exception E) {
      //E.printStackTrace();
    }
  }
   
  // helper method to reset flights to their original capacities
  private void fixCapacity() throws SQLException {
    for(Integer I : f) {
      int tempID = I.intValue();
      adjustCapacityStatement.setInt(1, tempID);
      adjustCapacityStatement.executeUpdate();
    }
  }


  /**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
    commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
    rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
    

    /* . . . . . . */

    newUserStatement = conn.prepareStatement(MAKE_NEW_USER);
    loginStatement = conn.prepareStatement(LOGIN_CHECK);
    searchOneHopStatement = conn.prepareStatement(SEARCH_ONE_HOP);
    searchTwoHopStatement = conn.prepareStatement(SEARCH_TWO_HOP);
    checkReservationsStatement = conn.prepareStatement(CHECK_RESERVATIONS);
    reduceCapacityStatement = conn.prepareStatement(REDUCE_CAPACITY);
    createReservationStatement = conn.prepareStatement(CREATE_RESERVATION);
    retrieveReservationsStatement = conn.prepareStatement(CURRENT_RESERVATIONS);
    listReservationStatement = conn.prepareStatement(USER_RESERVATIONS);
    retrieveBalanceStatement = conn.prepareStatement(USER_BALANCE);
    makePaymentStatement = conn.prepareStatement(MAKE_PAYMENT);
    refundStatement = conn.prepareStatement(GET_REFUND);
    clearUsersStatement = conn.prepareStatement(CLEAR_USERS);
    clearItinierariesStatement = conn.prepareStatement(CLEAR_ITINERARIES);
    clearReservationsStatement = conn.prepareStatement(CLEAR_RESERVATIONS);
    cancelAdjustStatement = conn.prepareStatement(CANCEL_ADJUST);
    adjustCapacityStatement = conn.prepareStatement(RESET_CAPACITY);

    //checkBookingCountStatement = conn.prepareStatement(COUNT_RESERVATIONS);
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username
   * @param password
   *
   * @return If someone has already logged in, then return "User already logged in\n"
   * For all other errors, return "Login failed\n".
   *
   * Otherwise, return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password)
  {
	int status = 0;

	if(this.isLoggedIn) {
           return "User already logged in\n";
	}

  	try {
	   loginStatement.clearParameters();
	   loginStatement.setString(1, username);
	   loginStatement.setString(2, password);    
	   ResultSet loginSet = loginStatement.executeQuery();
	   loginSet.next();
	   status = loginSet.getInt("status");
	   loginSet.close();
	   if(status > 0 && status <= 1) {
	      this.possibleBookingCount = 0;
              this.bookingOptions = null;
	      this.isLoggedIn = true;
	      this.username = username;
	      return "Logged in as " + username + "\n";
	   }
	}
	catch (Exception E) {
	   return "Login failed\n";
	}	
  return "Login failed\n";
  }

  /**
   * Implement the create user function.
   *
   * @param username new user's username. User names are unique the system.
   * @param password new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
   */
  public String transaction_createCustomer (String username, String password, int initAmount)
  {
     try {
	if(initAmount < 0) {
          return "Failed to create user\n";
        }
        newUserStatement.clearParameters();
	newUserStatement.setString(1, username);
	newUserStatement.setString(2, password);
	newUserStatement.setInt(3, initAmount);
	newUserStatement.executeUpdate();

	return "Created user " + username + "\n";	
     }
     
     catch(Exception E) {
       //System.out.println(E.toString());
       return "Failed to create user\n";
     }
  }
  
  

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise is searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth, int numberOfItineraries)
  {
    //StringBuffer sb = new StringBuffer();
    int flightCount = 0;
    bookingOptions = new Flight[numberOfItineraries];
    try {
      searchOneHopStatement.clearParameters();
      searchOneHopStatement.setInt(1, numberOfItineraries);
      searchOneHopStatement.setString(2, originCity);
      searchOneHopStatement.setString(3, destinationCity);
      searchOneHopStatement.setInt(4, dayOfMonth);
      ResultSet oneHopSearchResults = searchOneHopStatement.executeQuery();
          
      while(oneHopSearchResults.next()) {
        int result_fid = oneHopSearchResults.getInt("fid");
        int result_dayOfMonth = oneHopSearchResults.getInt("day_of_month");
        String result_carrierId = oneHopSearchResults.getString("carrier_id");
        int result_flightNum = oneHopSearchResults.getInt("flight_num");
        String result_origin = oneHopSearchResults.getString("origin_city");
        String result_dest = oneHopSearchResults.getString("dest_city");
        int result_time = oneHopSearchResults.getInt("actual_time");
        int result_capacity = oneHopSearchResults.getInt("capacity");
        int result_price = oneHopSearchResults.getInt("price");
        Flight tempFlight = new Flight(result_fid, result_dayOfMonth, result_carrierId, result_flightNum, 
                                       result_origin, result_dest, result_time, result_capacity, result_price);       
        bookingOptions[flightCount] = tempFlight;
	flightCount++;        
      }
      oneHopSearchResults.close();

      if(!directFlight) {        
        searchTwoHopStatement.clearParameters();
        searchTwoHopStatement.setInt(1, numberOfItineraries - flightCount);
        searchTwoHopStatement.setString(2, originCity);
        searchTwoHopStatement.setString(3, destinationCity);
        searchTwoHopStatement.setInt(4, dayOfMonth);
        ResultSet twoHopSearchResults = searchTwoHopStatement.executeQuery();
        
        while(twoHopSearchResults.next()) {
          
          int result_fid = twoHopSearchResults.getInt("fid");
          int result_dayOfMonth = twoHopSearchResults.getInt("day_of_month");
          String result_carrierId = twoHopSearchResults.getString("carrier_id");
          int result_flightNum = twoHopSearchResults.getInt("flight_num");
          String result_origin = twoHopSearchResults.getString("origin_city");
          String result_dest = twoHopSearchResults.getString("dest_city");
          int result_time = twoHopSearchResults.getInt("actual_time");
          int result_capacity = twoHopSearchResults.getInt("capacity");
          int result_price = twoHopSearchResults.getInt("price");

          int result_fid2 = twoHopSearchResults.getInt("fid2");
          int result_dayOfMonth2 = twoHopSearchResults.getInt("day_of_month2");
          String result_carrierId2 = twoHopSearchResults.getString("carrier_id2");
          int result_flightNum2 = twoHopSearchResults.getInt("flight_num2");
          String result_origin2 = twoHopSearchResults.getString("origin_city2");
          String result_dest2 = twoHopSearchResults.getString("dest_city2");
          int result_time2 = twoHopSearchResults.getInt("actual_time2");
          int result_capacity2 = twoHopSearchResults.getInt("capacity2");
          int result_price2 = twoHopSearchResults.getInt("price2");

          int totalTime = result_time + result_time2;
          
          Flight tempTwoHopFlightA = new Flight(result_fid, result_dayOfMonth, result_carrierId, result_flightNum, 
                                                result_origin, result_dest, result_time, result_capacity, result_price);

          Flight tempTwoHopFlightB = new Flight(result_fid2, result_dayOfMonth2, result_carrierId2, result_flightNum2, 
                                                result_origin2, result_dest2, result_time2, result_capacity2, result_price2);

	  Flight tempTwoHopFlightStore = new Flight(result_fid, result_dayOfMonth, result_carrierId, result_flightNum, 
                                                    result_origin, result_dest, result_time, result_capacity, result_price, 
						    result_fid2, result_dayOfMonth2, result_carrierId2, result_flightNum2, 
                                                    result_origin2, result_dest2, result_time2, result_capacity2, result_price2);
         
	  bookingOptions[flightCount] = tempTwoHopFlightStore;
          flightCount++;   
        } 
        twoHopSearchResults.close();
      }
      
    if(flightCount == 0) {
      return "No flights match your selection\n";
    }
    possibleBookingCount = flightCount;
    
    
    
    Flight temp = new Flight(0, 0, "", 0, "", "", 0, 0, 0, 0, 0, "", 0, "", "", 0, 0, 0);
    for (int i = 0; i <= bookingOptions.length; i++) 
    {
        for (int j = i+1; j < bookingOptions.length; j++)
        {
            if ((bookingOptions[i].time + bookingOptions[i].time2) > (bookingOptions[j].time + bookingOptions[j].time2)) 
            {
                temp = bookingOptions[i];
                bookingOptions[i] = bookingOptions[j];
                bookingOptions[j] = temp;
               
            } else if ((bookingOptions[i].time + bookingOptions[i].time2) == (bookingOptions[j].time + bookingOptions[j].time2)) {
              if(bookingOptions[i].fid > bookingOptions[j].fid) {
                temp = bookingOptions[i];
                bookingOptions[i] = bookingOptions[j];
                bookingOptions[j] = temp;
               
              }
            }
        }
    }
   
    String results = "";
    for(int i = 0; i < flightCount; i++) {
      results = results + "Itinerary " + i + ": ";
      if(bookingOptions[i].fid2 != 0) {
        results = results + bookingOptions[i].twoHopString();  
      } else {
        results = results + bookingOptions[i].oneHopString();  
      }
    }
    return results;
  
    } catch(Exception E) {      
      //E.printStackTrace();
      return "Failed to search\n";
    } 
  }

  /**
   * Same as {@code transaction_search} except that it only performs single hop search and
   * do it in an unsafe manner.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight
   * @param dayOfMonth
   * @param numberOfItineraries
   *
   * @return The search results. Note that this implementation *does not conform* to the format required by
   * {@code transaction_search}.
   */
  private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
                                          int dayOfMonth, int numberOfItineraries)
  {
    StringBuffer sb = new StringBuffer();

    try
    {
      // one hop itineraries
      String unsafeSearchSQL =
              "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
                      + "FROM Flights "
                      + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
                      + "ORDER BY actual_time ASC";

      Statement searchStatement = conn.createStatement();
      ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

      while (oneHopResults.next())
      {
        int result_dayOfMonth = oneHopResults.getInt("day_of_month");
        String result_carrierId = oneHopResults.getString("carrier_id");
        String result_flightNum = oneHopResults.getString("flight_num");
        String result_originCity = oneHopResults.getString("origin_city");
        String result_destCity = oneHopResults.getString("dest_city");
        int result_time = oneHopResults.getInt("actual_time");
        int result_capacity = oneHopResults.getInt("capacity");
        int result_price = oneHopResults.getInt("price");

        sb.append("Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum + " Origin: " + result_originCity 
		+ " Destination: " + result_destCity + " Duration: " + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n");
      }
      oneHopResults.close();
    } catch (SQLException E) { 
      //E.printStackTrace();
    }

    return sb.toString();
  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
   * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
   * If the user already has a reservation on the same day as the one that they are trying to book now, then return
   * "You cannot book two flights in the same day\n".
   * For all other errors, return "Booking failed\n".
   *
   * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
   * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
   * successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId)
  {
    if(username == null) {
      return "Cannot book reservations, not logged in\n";
    }
    
    if(itineraryId < 0 || itineraryId  >= possibleBookingCount) {
      return "No such itinerary " + itineraryId + "\n";
    }
    String failure = "";
    try {
 
      beginTransaction();
      Flight targetBook = bookingOptions[itineraryId];
      checkReservationsStatement.clearParameters();
      checkReservationsStatement.setString(1, username);
      checkReservationsStatement.setInt(2, targetBook.dayOfMonth);
      ResultSet bookings = checkReservationsStatement.executeQuery();
      bookings.next();
      int numOfRes = bookings.getInt("numRes");
      bookings.close();

      if(numOfRes >= 1) {
        //need to stop transaction here, since this is an invalid action
        rollbackTransaction();
        return "You cannot book two flights in the same day\n";
      }

      if(checkFlightCapacity(targetBook.fid) >= 1) {        
        if(targetBook.fid2 != 0) {
          if(checkFlightCapacity(targetBook.fid2) >= 1) {
          // double hop flight case

          Integer tempA = new Integer(targetBook.fid);
          Integer tempB = new Integer(targetBook.fid2);

          if(!f.contains(tempA) && !f.contains(tempB)) {
            int originalDoubleCapacityA = checkFlightCapacity(targetBook.fid);
            int originalDoubleCapacityB = checkFlightCapacity(targetBook.fid2);
            Flight tempHopA = new Flight(targetBook.fid, originalDoubleCapacityA);
            Flight tempHopB = new Flight(targetBook.fid2, originalDoubleCapacityB);

            Integer intObjA = new Integer(targetBook.fid);
            Integer intObjB = new Integer(targetBook.fid2);       
    
            f.add(intObjA);
            f.add(intObjB);

            capacityReset[resetCount] = tempHopA;
            resetCount++;
            capacityReset[resetCount] = tempHopB;
            resetCount++;
          }

	  reduceCapacityStatement.clearParameters();
	  reduceCapacityStatement.setInt(1, targetBook.fid);
          reduceCapacityStatement.executeUpdate();

          reduceCapacityStatement.clearParameters();
	  reduceCapacityStatement.setInt(1, targetBook.fid2);
          reduceCapacityStatement.executeUpdate();
    
          int combinedPrice = targetBook.price + targetBook.price2;
          createReservationStatement.clearParameters();
	  createReservationStatement.setInt(1, targetBook.dayOfMonth);
          createReservationStatement.setString(2, username);
          createReservationStatement.setInt(3, targetBook.fid);
	  createReservationStatement.setInt(4, targetBook.fid2);
 	  createReservationStatement.setInt(5, combinedPrice);
	  createReservationStatement.setInt(6, 0);
          createReservationStatement.setInt(7, 0);
	  createReservationStatement.setInt(8, masterResId);
          createReservationStatement.executeUpdate();
          //System.out.println("value of id is: " + masterResId);
	  masterResId++;
	//  System.out.println("RIGHT BEEFORE COMMIT");
	  commitTransaction();
	  return "Booked flight(s), reservation ID: " + (masterResId - 1) + "\n";

          } else {
            System.out.println("RIGHT BEEFORE ROLLBACK1");
            rollbackTransaction();
            return "Booking failed\n";
            //return "second flight has no capacity";

          }  

        } else {
	  // direct flight case
          Integer tempC = new Integer(targetBook.fid);
          if(!f.contains(tempC)) {
	    int originalSingleCapacity = checkFlightCapacity(targetBook.fid);
            Flight tempFix = new Flight(targetBook.fid, originalSingleCapacity);
            Integer intObjC = new Integer(targetBook.fid);
            f.add(intObjC);
            capacityReset[resetCount] = tempFix;
            resetCount++;
          }

	  reduceCapacityStatement.clearParameters();
	  reduceCapacityStatement.setInt(1, targetBook.fid);
          reduceCapacityStatement.executeUpdate();
        
          createReservationStatement.clearParameters();
	  createReservationStatement.setInt(1, targetBook.dayOfMonth);
          createReservationStatement.setString(2, username);
          createReservationStatement.setInt(3, targetBook.fid);
	  createReservationStatement.setNull(4, Types.INTEGER);
 	  createReservationStatement.setInt(5, targetBook.price);
	  createReservationStatement.setInt(6, 0);
          createReservationStatement.setInt(7, 0);
	  createReservationStatement.setInt(8, masterResId);
          createReservationStatement.executeUpdate();
	  //System.out.println("value of id is: " + masterResId);
	  masterResId++;
	  //System.out.println("value of id after increment is: " + masterResId);

          return "Booked flight(s), reservation ID: " + (masterResId - 1) + "\n";
        } 
      } else {
       // System.out.println("RIGHT BEEFORE ROLLBACK2");
        rollbackTransaction();
	return "Booking failed\n";
	//return "first flight has no capacity";
      }
    } catch(Exception E) {
      //E.printStackTrace();
      //rollbackTransaction();
      failure = "Booking failed\n";
    }
    //System.out.println("RIGHT BEEFORE ROLLBACK3");
    try {rollbackTransaction();} catch(Exception E) {}
    return failure;
  }

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
   * If the user has no reservations, then return "No reservations found\n"
   * For all other errors, return "Failed to retrieve reservations\n"
   *
   * Otherwise return the reservations in the following format:
   *
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * ...
   *
   * Each flight should be printed using the same format as in the {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations()
  {
    try {
      boolean foundRes = false;
      if(username == null) {
        return "Cannot view reservations, not logged in\n";
      }
      StringBuffer sb2 = new StringBuffer();
      retrieveReservationsStatement.clearParameters();
      retrieveReservationsStatement.setString(1, username);
      ResultSet currentReservations = retrieveReservationsStatement.executeQuery();
      
      while(currentReservations.next()) {
        
        foundRes = true;
        boolean isPaid = false;
        boolean isCanceled = false;

        if(currentReservations.getInt("paid") == 1) {
          isPaid = true;
        }
     
        if(currentReservations.getInt("canceled") == 1) {
          isCanceled = true;
        }

        int result_fid = currentReservations.getInt("fid");
        int result_dayOfMonth = currentReservations.getInt("day_of_month");
        String result_carrierId = currentReservations.getString("carrier_id");
        int result_flightNum = currentReservations.getInt("flight_num");
        String result_origin = currentReservations.getString("origin_city");
        String result_dest = currentReservations.getString("dest_city");
        int result_time = currentReservations.getInt("actual_time");
        int result_capacity = currentReservations.getInt("capacity");
        int result_price = currentReservations.getInt("price");

        int result_fid2 = currentReservations.getInt("fid2");
        int result_dayOfMonth2 = currentReservations.getInt("day_of_month2");
        String result_carrierId2 = currentReservations.getString("carrier_id2");
        int result_flightNum2 = currentReservations.getInt("flight_num2");
        String result_origin2 = currentReservations.getString("origin_city2");
        String result_dest2 = currentReservations.getString("dest_city2");
        int result_time2 = currentReservations.getInt("actual_time2");
        int result_capacity2 = currentReservations.getInt("capacity2");
        int result_price2 = currentReservations.getInt("price2");
        
        
        Flight tempF = new Flight(result_fid, result_dayOfMonth, result_carrierId, result_flightNum, 
                                  result_origin, result_dest, result_time, result_capacity, result_price);

        if(!isCanceled) { 
          sb2.append("Reservation " + currentReservations.getInt("resID") + " paid: " + isPaid + ":" + "\n");      
          sb2.append(tempF.toString());
          currentReservations.getInt("fid2");
          if(!currentReservations.wasNull()) {
            Flight tempTwoHopFlightB = new Flight(result_fid2, result_dayOfMonth2, result_carrierId2, result_flightNum2, 
                                                result_origin2, result_dest2, result_time2, result_capacity2, result_price2);
            sb2.append(tempTwoHopFlightB.toString());
          }
        }
      }
      currentReservations.close();
      if(!foundRes) {
        return "No reservations found\n";
      }
      
      return sb2.toString();
      
      
    } catch(Exception E) {
      //E.printStackTrace();
      return "Failed to retrieve reservations\n";
    }

  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
   * For all other errors, return "Failed to cancel reservation [reservationId]"
   *
   * If successful, return "Canceled reservation [reservationId]"
   *
   * Even though a reservation has been canceled, its ID should not be reused by the system.
   */
  public String transaction_cancel(int reservationId)
  {
    // only implement this if you are interested in earning extra credit for the HW!
    if(username == null) {
      return "Cannot cancel reservations, not logged in\n"; 
    }
    try {
      beginTransaction();
      listReservationStatement.clearParameters();
      listReservationStatement.setInt(1, reservationId);
      listReservationStatement.setString(2, username);
      ResultSet cancelTarget = listReservationStatement.executeQuery();

      if(!cancelTarget.next() || cancelTarget.getInt("canceled") == 1) {
        cancelTarget.close();
        rollbackTransaction();
        //System.out.println("check to see if canceled = 1");
        return "Failed to cancel reservation " + reservationId + "\n";
      } else {
        int flight1 = cancelTarget.getInt("flight_id");  
        int f1cap = checkFlightCapacity(flight1) + 1;     
        cancelAdjustStatement.clearParameters(); // sets Reservations.canceled = 1 and increments capacity by 1 
        cancelAdjustStatement.setInt(1, reservationId);
        cancelAdjustStatement.setInt(2, f1cap);
       // System.out.println("incrementing capacity");
        cancelAdjustStatement.setInt(3, flight1);
        cancelAdjustStatement.executeUpdate();

             
        int flight2 = 0;
        int f2cap = 0;
        cancelTarget.getInt("flight_id2");
        if(!cancelTarget.wasNull()) {
          flight2 = cancelTarget.getInt("flight_id2");  
          f2cap = checkFlightCapacity(flight2) + 1;
        }

        if(flight2 != 0) {
          cancelAdjustStatement.clearParameters();
          cancelAdjustStatement.setInt(1, reservationId);
          cancelAdjustStatement.setInt(2, f2cap);
          System.out.println("incrementing capacity");
          cancelAdjustStatement.setInt(3, flight2);
          cancelAdjustStatement.executeUpdate();  
        } 
        if(cancelTarget.getInt("paid") == 1) {

        int refundAmount = cancelTarget.getInt("price");
                                
          refundStatement.clearParameters(); // sets paid = 0, canceled = 1, refunds money for flight
          refundStatement.setInt(1, reservationId);
          refundStatement.setInt(2, reservationId);
          refundStatement.setInt(3, refundAmount);
          refundStatement.setString(4, username); 
          refundStatement.executeUpdate();       
        }
        cancelTarget.close();
        commitTransaction();
        return "Canceled reservation " + reservationId + "\n";
      }  
    }
    catch (Exception E) {}
    try{rollbackTransaction();}
    catch (Exception E) {
      //E.printStackTrace();      
    }          
    return "Failed to cancel reservation " + reservationId + "\n";
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n"
   * If the reservation is not found / not under the logged in user's name, then return
   * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
   * If the user does not have enough money in their account, then return
   * "User has only [balance] in account but itinerary costs [cost]\n"
   * For all other errors, return "Failed to pay for reservation [reservationId]\n"
   *
   * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
   * where [balance] is the remaining balance in the user's account.
   */
  public String transaction_pay (int reservationId)
  {
    if(username == null) {
      return "Cannot pay, not logged in\n"; 
    }
    String failure = "";
    try {
      beginTransaction();      
      listReservationStatement.clearParameters();
      listReservationStatement.setInt(1, reservationId);
      listReservationStatement.setString(2, username);
      ResultSet paymentResults = listReservationStatement.executeQuery();
      if (!paymentResults.next() || paymentResults.getInt("paid") == 1) {
        paymentResults.close();
        rollbackTransaction();
        return "Cannot find unpaid reservation " + reservationId + " under user: " + username + "\n";        
      }
      
      int fee = paymentResults.getInt("price");
      retrieveBalanceStatement.clearParameters();
      retrieveBalanceStatement.setString(1, username);
      ResultSet balances = retrieveBalanceStatement.executeQuery();
      balances.next();
      int currBal = balances.getInt("initAmount");
      if(currBal < fee) {
        rollbackTransaction();
        return "User has only " + currBal + " in account but itinerary costs " + fee + "\n";
      }
      int newBal = currBal - fee;
      balances.close();
      
      makePaymentStatement.clearParameters();
      makePaymentStatement.setInt(1, newBal);
      makePaymentStatement.setString(2, username);
      makePaymentStatement.setInt(3, 1);
      makePaymentStatement.setInt(4, reservationId);
      makePaymentStatement.executeUpdate();
      commitTransaction();
      return "Paid reservation: " + reservationId + " remaining balance: " + newBal + "\n";
    }
    catch (Exception E) {
      //E.printStackTrace();
      failure = "Failed to pay for reservation " + reservationId + "\n";
    }
    try {rollbackTransaction();} catch(Exception E) {}
    return failure;
  }

  /* some utility functions below */

  public void beginTransaction() throws SQLException
  {
    conn.setAutoCommit(false);
    beginTransactionStatement.executeUpdate();
  }

  public void commitTransaction() throws SQLException
  {
    commitTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  public void rollbackTransaction() throws SQLException
  {
    rollbackTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  /**
   * Shows an example of using PreparedStatements after setting arguments. You don't need to
   * use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }
}
