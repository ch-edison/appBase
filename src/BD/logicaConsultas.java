/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package BD;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 *
 * @author EdisonChavez
 */
public class logicaConsultas {
    //declaracion de Arraylit y inicializacion de un objeto
    ConsulTablas ManejadorConsultas = new ConsulTablas();
    ArrayList<String> ArrB;
    ArrayList<String> ArrT;
    ArrayList<String> ArrC;
    ArrayList<String> ArrJ;
    //Metodo que me permite consultar las base de datos
    public ArrayList<String> ConsultarBase(String Base) throws ClassNotFoundException, SQLException {
        ArrB = new ArrayList<String>();
        int intIncremento = 0;
        ResultSet rs;
        rs = ManejadorConsultas.Consultar_Base(Base);
        //Envia datos y los guarda en una lista
        while (rs.next()) {
            String valor = "";
            String value = rs.getString(1);
            valor = value;

            ArrB.add(valor);
            intIncremento = intIncremento + 1;
        }

        return ArrB;
    }
    //Metodo que consulta tablas, recibe el nombre del tabla y la base
    public ArrayList<String> ConsultarTablas(String valor, String Base) throws SQLException, ClassNotFoundException {
        ArrT = new ArrayList<String>();
        int intIncremento = 0;
        ResultSet rs;
        rs = ManejadorConsultas.Consultar_Tablas(valor, Base);
        while (rs.next()) {
            String v = rs.getString(1);
            ArrT.add(v);
            intIncremento = intIncremento + 1;

        }
        return ArrT;
    }
    //Metodo que me permite consultar el Join, muestra todas las consultas del join
    public ArrayList<String> consultarJoin(String tabla1, String tabla2, String Base, String col1, String col2, int totCol) throws SQLException, ClassNotFoundException {
        ArrC = new ArrayList<String>();
        int intIncremento = 0;
        ResultSet rs;
        rs = ManejadorConsultas.consultarJoin(tabla1, tabla2, Base, col1, col2);
        int n = 0;
        while (rs.next()) {
            for (int i = 1; i < totCol; i++) {
                if (n == totCol - 1) {
                    ArrC.add("\n");
                    n = 0;
                }
                String v = rs.getString(i);
                ArrC.add(v + "\t");
                n = n + 1;
                intIncremento = intIncremento + 1;
            }
        }
        return ArrC;
    }
    //Metodo que consulta los Join 
    public ArrayList<String> consulJoin(String tabla1, String tabla2, String Base, String col1, String col2) throws SQLException, ClassNotFoundException {

        ArrJ = new ArrayList<String>();
        int intIncremento = 0;
        ResultSet rs;
        rs = ManejadorConsultas.consultarJoin(tabla1, tabla2, Base, col1, col2);        
        ResultSetMetaData rm = rs.getMetaData();       
        int columnCount = rm.getColumnCount();
        ArrayList<String> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = rm.getColumnName(i);
            columns.add(columnName);
        }
        
        String value="";
        while (rs.next()) {
            for (String columnName : columns) {
                value = rs.getString(columnName);
                ArrJ.add(value);               
            }
            
            
           
            intIncremento = intIncremento + 1;
        }
        return ArrJ;
    }
    //Metodo que me permite hacer la consulta de relacion de 2 tablas.
    public ArrayList<String> consulJoinT(String tabla1, String tabla2, String Base, String col1, String col2,String valCol) throws SQLException, ClassNotFoundException {

        ArrJ = new ArrayList<String>();
        int intIncremento = 0;
        ResultSet rs;
        rs = ManejadorConsultas.consultarJoinT(tabla1, tabla2, Base, col1, col2,valCol);
        //para desglosar los datos que estan dentro del resultSet
        ResultSetMetaData rm = rs.getMetaData();
        //Recupera las columnas de la tabla
        int columnCount = rm.getColumnCount();
        //columnas totales 
        ArrayList<String> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = rm.getColumnName(i);
            columns.add(columnName);
        }
        
        String value="";
        while (rs.next()) {
            for (String columnName : columns) {
                value = rs.getString(columnName);
                ArrJ.add(value);               
            }
            intIncremento = intIncremento + 1;
        }
        return ArrJ;
    }
}
