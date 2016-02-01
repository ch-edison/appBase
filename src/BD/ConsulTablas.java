package BD;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ConsulTablas {
    //se instancia un objeto para relizar la conexion con la Base de Datos
    ConectarBD c = new ConectarBD();
    //Metodo que permite consultar todos los valores de la tabla que se ingrese
    public ResultSet Consultar_Valores(String tabla,String Base) throws ClassNotFoundException, SQLException {        
        Statement st = c.getConnection(Base).createStatement();
        String Sentencia = "SELECT * FROM " + tabla;
        ResultSet rs = st.executeQuery(Sentencia);
        return rs;
    }
    //Metodo que permite consultar todos los valores de la tabla que se ingrese
    public ResultSet Consultar_Valoress(String tabla,String Base) throws ClassNotFoundException, SQLException {      
        Statement st = c.getConnection(Base).createStatement();
        String Sentencia = "SELECT * FROM " + tabla;
        ResultSet rs = st.executeQuery(Sentencia);
        return rs;
    }
    //Metodo que consulta todas las bases existentes
    public ResultSet Consultar_Base(String Base) throws ClassNotFoundException, SQLException {     
        Statement st = c.getConnection(Base).createStatement();
        String Sentencia = "show databases";
        ResultSet rs = st.executeQuery(Sentencia);
        return rs;
    }
    //Metodo que me permite consultar todas las tablas de una base de datos
    public ResultSet Consultar_Tablas(String valor,String Base) throws ClassNotFoundException, SQLException {       
        Statement st = c.getConnection(Base).createStatement();
        String Sentencia = "SHOW TABLES FROM "+valor;
        ResultSet rs = st.executeQuery(Sentencia);
        return rs;
    }
    //Metodo que me permite consultar dos tablas y hacer el Join
    public ResultSet consultarJoin(String tabla,String tabla2,String Base,String valor1,String valor2) throws ClassNotFoundException, SQLException {
        Statement st = c.getConnection(Base).createStatement();
        String Sentencia = "SELECT * FROM " + tabla+" id INNER JOIN "+tabla2+" idd "+"ON "+"id."+valor1+"= idd."+valor2;
        ResultSet rs = st.executeQuery(Sentencia);
       
        return rs;
    }
    //Metodo que me permite consultar los valores de una tabla pero haciendo el Join
    public ResultSet consultarJoinT(String tabla,String tabla2,String Base,String valor1,String valor2,String valCol) throws ClassNotFoundException, SQLException {
        Statement st = c.getConnection(Base).createStatement();
        String Sentencia = "SELECT "+valCol+" FROM " + tabla+" id INNER JOIN "+tabla2+" idd "+"ON "+"id."+valor1+"= idd."+valor2;
        ResultSet rs = st.executeQuery(Sentencia);
       
        return rs;
    }
    
}