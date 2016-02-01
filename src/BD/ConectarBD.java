package BD;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
public class ConectarBD {
    public Connection con;
    //Metodo que me permite establecer conexion
    public Connection getConnection(String Base) throws ClassNotFoundException {
        try {
            String driver = "com.mysql.jdbc.Driver";
            Class.forName(driver);
            con = DriverManager.getConnection("jdbc:mysql://localhost/" + Base, "root", "");
        } catch (SQLException e) {
            e.getErrorCode();
        }
        return con;
    }
    public void CerrarConexion() throws SQLException {
        con.close();
    }
}
