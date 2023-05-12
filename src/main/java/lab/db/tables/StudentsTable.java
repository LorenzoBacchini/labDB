package lab.db.tables;

 import java.sql.Connection;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.Statement;
 import java.sql.SQLException;
 import java.sql.SQLIntegrityConstraintViolationException;
 import java.util.ArrayList;
 import java.util.Date;
 import java.util.List;
 import java.util.Objects;
 import java.util.Optional;

 import lab.utils.Utils;
 import lab.db.Table;
 import lab.model.Student;

 public final class StudentsTable implements Table<Student, Integer> {    
     public static final String TABLE_NAME = "students";

     private final Connection connection; 

     public StudentsTable(final Connection connection) {
         this.connection = Objects.requireNonNull(connection);
     }

     @Override
     public String getTableName() {
         return TABLE_NAME;
     }

     @Override
     public boolean createTable() {
         // 1. Create the statement from the open connection inside a try-with-resources
         try (final Statement statement = this.connection.createStatement()) {
             // 2. Execute the statement with the given query
             statement.executeUpdate(
                 "CREATE TABLE " + TABLE_NAME + " (" +
                         "id INT NOT NULL PRIMARY KEY," +
                         "firstName CHAR(40)," + 
                         "lastName CHAR(40)," + 
                         "birthday DATE" + 
                     ")");
             return true;
         } catch (final SQLException e) {
             // 3. Handle possible SQLExceptions
             return false;
         }
     }

     @Override
     public Optional<Student> findByPrimaryKey(final Integer id) {
        String query = "Select * from " + TABLE_NAME + " where id = ?";
        PreparedStatement ps;
        try{
            ps = this.connection.prepareStatement(query);
            ps.setInt(1,id);
            ResultSet rs = ps.executeQuery();
            return readStudentsFromResultSet(rs).stream().findFirst();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
     }

     /**
      * Given a ResultSet read all the students in it and collects them in a List
      * @param resultSet a ResultSet from which the Student(s) will be extracted
      * @return a List of all the students in the ResultSet
      */
     private List<Student> readStudentsFromResultSet(final ResultSet resultSet) {
         // Create an empty list, then
         List<Student> studentList = new ArrayList<>();
         // Inside a loop you should:
         //      1. Call resultSet.next() to advance the pointer and check there are still rows to fetch
         //      2. Use the getter methods to get the value of the columns
         //      3. After retrieving all the data create a Student object
         //      4. Put the student in the List
         try{
            while(resultSet.next()){
                int id = resultSet.getInt("id");
                String firstName = resultSet.getString("firstName");
                String lastName = resultSet.getString("lastName");
                Optional<Date> birthday = Optional.ofNullable(Utils.sqlDateToDate(resultSet.getDate("birthday")));
                Student student = new Student(id, firstName, lastName, birthday);
                studentList.add(student);
            }
         }catch(SQLException e){}
         
         // Then return the list with all the found students
         return studentList;

         // Helpful resources:
         // https://docs.oracle.com/javase/7/docs/api/java/sql/ResultSet.html
         // https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html
     }

     @Override
     public List<Student> findAll() {
        String query = "Select * from " + TABLE_NAME;
        Statement s;
        try{
            s = this.connection.createStatement();
            ResultSet rs = s.executeQuery(query);
            return readStudentsFromResultSet(rs);
        }catch(SQLException e){
            throw new IllegalStateException(e);
        }
     }

     public List<Student> findByBirthday(final Date date) {
        String query = "Select * from " + TABLE_NAME + " where birthday = ?";
        PreparedStatement ps;
        try{
            ps = this.connection.prepareStatement(query);
            ps.setDate(1, Utils.dateToSqlDate(date));
            ResultSet rs = ps.executeQuery();
            return readStudentsFromResultSet(rs);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
     }

     @Override
     public boolean dropTable() {
        String query = "Drop table " + TABLE_NAME;
        Statement s;
        try{
            s = this.connection.createStatement();
            s.executeUpdate(query);
            return true;
        }catch(SQLException e){
            return false;
        }
     }

     @Override
     public boolean save(final Student student) {
        String query = "Insert into " + TABLE_NAME + " (id, firstName, lastName, birthday) values (?, ?, ?, ?)";
        PreparedStatement ps;
        try{
            ps = this.connection.prepareStatement(query);
            ps.setInt(1, student.getId());
            ps.setString(2, student.getFirstName());
            ps.setString(3, student.getLastName());
            ps.setDate(4, Utils.dateToSqlDate(student.getBirthday().isPresent() ? student.getBirthday().get() : null));
            ps.executeUpdate();
            return true;
        }catch(SQLException e){
            return false;
        }
     }

     @Override
     public boolean delete(final Integer id) {
        String query = "Delete from " + TABLE_NAME + " where id = ?";
        PreparedStatement ps;
        try{
            ps = this.connection.prepareStatement(query);
            ps.setInt(1, id);
            return (ps.executeUpdate() > 0);
        }catch(SQLException e){
            throw new IllegalStateException(e);
        }
     }

     @Override
     public boolean update(final Student student) {
        String query = 
            "Update " + TABLE_NAME + " Set " + 
                "firstName = ?," + 
                "lastName =  ?," +
                "birthday ? " + 
            "where id = ?";
        PreparedStatement ps;
        try{
            ps = this.connection.prepareStatement(query);
            ps.setString(1, student.getFirstName());
            ps.setString(2, student.getLastName());
            ps.setDate(3, Utils.dateToSqlDate(student.getBirthday().get()));
            ps.setInt(4, student.getId());
            return (ps.executeUpdate() > 0);
        }catch(SQLException e){
            throw new IllegalStateException(e);
        }
     }
 }