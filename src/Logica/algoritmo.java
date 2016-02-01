package Logica;

import BD.ConsulTablas;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

public class algoritmo {

    //inicializo la variable 

    ConsulTablas ManejadorConsulta = new ConsulTablas();

    //este metodo tiene la funcion de comparar las dos tablas y dar coo resultado que columnas pueden hacer join

    public String Verificacion(String tabla1, String tabla2, String Base) throws SQLException, ClassNotFoundException {
        //Inicializo variables que voy a utilizar
        ArrayList<String> nomCol1 = new ArrayList<>();
        ArrayList<String> nomCol2 = new ArrayList<>();
        ArrayList<String> tipoCol1 = new ArrayList<>();
        ArrayList<String> tipoCol2 = new ArrayList<>();
        ArrayList<String> datCol1 = new ArrayList<>();
        ArrayList<String> datCol2 = new ArrayList<>();
        ArrayList<String> resuCol1 = new ArrayList<>();
        ArrayList<String> relCol = new ArrayList<>();
        ArrayList<String> relCol1 = new ArrayList<>();
        ArrayList<String> relCol2 = new ArrayList<>();
        ArrayList<String[]> resCol1 = new ArrayList<String[]>();
        ArrayList<String[]> resCol2 = new ArrayList<String[]>();
        ArrayList<String> porceR = new ArrayList<>();
        int con = 0;
        int c = 0;
        int in = 0;
        int k = 0;
        int g = 0;
        int z = 0;
        String nombreCol1 = "";
        String nombreCol2 = "";

        //consultas para que se puedan conectar a la base de datos y arrojar datos.
        ResultSet rsTabla1 = ManejadorConsulta.Consultar_Valores(tabla1, Base);
        ResultSet rsTabla2 = ManejadorConsulta.Consultar_Valoress(tabla2, Base);

        ResultSet rsTabla3 = ManejadorConsulta.Consultar_Valores(tabla1, Base);
        ResultSet rsTabla4 = ManejadorConsulta.Consultar_Valores(tabla2, Base);
        //con estas variables podemos obtener los datos que
        ResultSetMetaData mdT1 = rsTabla1.getMetaData();
        ResultSetMetaData mdT2 = rsTabla2.getMetaData();
        //Recupera el numero de columnas que tiene la tablas
        int columnCount = mdT1.getColumnCount();
        int columnCount2 = mdT2.getColumnCount();

        // 2 metodos for su funcion es obtener nombre y tipo de la columna de cada tabla
        for (int i = 0; i < columnCount; i++) {
            nomCol1.add(mdT1.getColumnName(i + 1));
            tipoCol1.add(mdT1.getColumnTypeName(i + 1));
        }
        for (int i = 0; i < columnCount2; i++) {
            nomCol2.add(mdT2.getColumnName(i + 1));
            tipoCol2.add(mdT2.getColumnTypeName(i + 1));
        }
        //los dos while tienen como funcion recoger los tados y guardarlos en un arrayList respectivamente
        while (rsTabla1.next()) {
            for (String v : nomCol1) {
                if (g < columnCount) {
                    String valor = rsTabla1.getString(v);
                    datCol1.add(g, valor);
                    g = g + 1;
                }
            }
        }
        while (rsTabla2.next()) {
            for (String valor : nomCol2) {
                if (z < columnCount2) {
                    String valorR = rsTabla2.getString(valor);
                    datCol2.add(z, valorR);
                    z = z + 1;
                }
            }
        }
        //Este for permite ir recorriendo la listas de las tablas 1 y 2 y poder asi hacer una condicion si son iguales 
        //que me recoga el valor y guarde los nombres de las columnas iguales en una lista (cabe recalcar que aqui primero
        //se compara los datos)
        for (in = 0; in < columnCount; in++) {
            for (k = 0; k < columnCount2; k++) {
                if (datCol1.get(in).equals(datCol2.get(k))) {
                    nombreCol1 = mdT1.getColumnName(in + 1);
                    nombreCol2 = mdT2.getColumnName(k + 1);
                    relCol1.add(nombreCol1);
                    relCol2.add(nombreCol2);
                    relCol.add(nombreCol1 + "-----" + nombreCol2);
                }
            }
            if (k == columnCount2 - 1) {
                k = 0;
            }
        }
        //los dos whiles tiene como funcion una vez recogida las columnas que son iguales 
        // se van a guardar todos los datos respectivamente en la lista
        // para luego sacar el porcentaje de acierto en dato
        while (rsTabla3.next()) {
            String Fila[] = new String[relCol1.size()];
            for (int x = 0; x < relCol1.size(); ++x) {
                Fila[x] = rsTabla3.getObject(relCol1.get(x)).toString();
            }
            resCol1.add(Fila);
        }
        while (rsTabla4.next()) {
            String Fila2[] = new String[relCol2.size()];
            for (int x = 0; x < relCol2.size(); ++x) {
                Fila2[x] = rsTabla4.getObject(relCol2.get(x)).toString();
            }
            resCol2.add(Fila2);

        }
        double por = 0;
        double po = 0;
        String r = "";
        po = resCol1.size() * resCol2.size();
        //el for tiene como funcion recorrer las listas que contienen los datos e igualar si so iguales se va
        //aumentar el contador una vez que finalice se tendra un numero total de relacion que ha tenido
        
        for (int i = 0; i < relCol1.size(); i++) {
            for (int j = 0; j < relCol2.size(); j++) {
                for (String[] val : resCol1) {
                    for (String[] va : resCol2) {
                        if (val[i].toUpperCase().equals(va[j].toUpperCase())) {
                            if (i == j) {
                                c = c + 1;
                            }
                        }
                    }
                }

            }
            por = (c / po) * 100;
            if (por >= 100) {
                por = 100;
            }
            //se guarda el porcentaje en una lista respectivamente
            porceR.add(Double.toString(por));
        }
        String retorno = "";
        // Este metodo lo que hace es guardar en una lista los nombres de las columnas que vamos hacer join
        // y porcentaje que han tenido
        for (int i = 0; i < relCol.size(); i++) {
            retorno = retorno + relCol.get(i) + "------>" + porceR.get(i)+" %"+ "\n";
        }

        return "Se recomienda hacer Join con los que tienen un porcentaje mayor a 50% \n\n"+retorno;
    }

    public String valorJoin(String tabla1, String tabla2, String Base) throws SQLException, ClassNotFoundException {

        ArrayList<String> nomCol1 = new ArrayList<>();
        ArrayList<String> nomCol2 = new ArrayList<>();
        ArrayList<String> tipoCol1 = new ArrayList<>();
        ArrayList<String> tipoCol2 = new ArrayList<>();
        ArrayList<String> datCol1 = new ArrayList<>();
        ArrayList<String> datCol2 = new ArrayList<>();
        ArrayList<String> relCol = new ArrayList<>();
        ArrayList<String> relCol1 = new ArrayList<>();
        ArrayList<String> relCol2 = new ArrayList<>();
        ArrayList<String[]> resCol1 = new ArrayList<String[]>();
        ArrayList<String[]> resCol2 = new ArrayList<String[]>();

        int in = 0;
        int k = 0;
        int g = 0;
        int z = 0;
        String nombreCol1 = "";
     

        //Recogemos los datos de la conexion (BD - Tablas)
        ResultSet rsTabla1 = ManejadorConsulta.Consultar_Valores(tabla1, Base);
        ResultSet rsTabla2 = ManejadorConsulta.Consultar_Valoress(tabla2, Base);

        ResultSet rsTabla3 = ManejadorConsulta.Consultar_Valores(tabla1, Base);
        ResultSet rsTabla4 = ManejadorConsulta.Consultar_Valores(tabla2, Base);
        //para desglosar los datos que estan dentro del resultSet(de esta forma podremos obtener los datos que se requiera)
        ResultSetMetaData mdT1 = rsTabla1.getMetaData();
        ResultSetMetaData mdT2 = rsTabla2.getMetaData();
        //Recupera el numero de columnas que tiene la tablas
        int columnCount = mdT1.getColumnCount();
        int columnCount2 = mdT2.getColumnCount();

        //Creamos arreglos bidimensionales para guardar (numbre columna, tipo de dato de columna)
        for (int i = 0; i < columnCount; i++) {
            nomCol1.add(mdT1.getColumnName(i + 1));
            tipoCol1.add(mdT1.getColumnTypeName(i + 1));
        }
        for (int i = 0; i < columnCount2; i++) {
            nomCol2.add(mdT2.getColumnName(i + 1));
            tipoCol2.add(mdT2.getColumnTypeName(i + 1));
        }

        while (rsTabla1.next()) {
            for (String v : nomCol1) {
                if (g < columnCount) {
                    String valor = rsTabla1.getString(v);
                    datCol1.add(g, valor);
                    g = g + 1;
                }
            }
        }
        while (rsTabla2.next()) {
            for (String valor : nomCol2) {
                if (z < columnCount2) {
                    String valorR = rsTabla2.getString(valor);
                    datCol2.add(z, valorR);
                    z = z + 1;
                }
            }
        }
        for (in = 0; in < columnCount; in++) {
            for (k = 0; k < columnCount2; k++) {
                if (datCol1.get(in).equals(datCol2.get(k))) {
                    nombreCol1 = mdT1.getColumnName(in + 1);
                    relCol1.add(nombreCol1);

                }
            }
            if (k == columnCount2 - 1) {
                k = 0;
            }
        }

        String retorno = "";
        for (String v : relCol1) {
            retorno = retorno + v + ",";
        }

        return retorno;
    }

    public String valorJoin2(String tabla1, String tabla2, String Base) throws SQLException, ClassNotFoundException {

        ArrayList<String> nomCol1 = new ArrayList<>();
        ArrayList<String> nomCol2 = new ArrayList<>();
        ArrayList<String> tipoCol1 = new ArrayList<>();
        ArrayList<String> tipoCol2 = new ArrayList<>();
        ArrayList<String> datCol1 = new ArrayList<>();
        ArrayList<String> datCol2 = new ArrayList<>();     
        ArrayList<String> relCol2 = new ArrayList<>();
    
        int in = 0;
        int k = 0;
        int g = 0;
        int z = 0;      
        String nombreCol2 = "";

        //Recogemos los datos de la conexion (BD - Tablas)
        ResultSet rsTabla1 = ManejadorConsulta.Consultar_Valores(tabla1, Base);
        ResultSet rsTabla2 = ManejadorConsulta.Consultar_Valoress(tabla2, Base);

        ResultSet rsTabla3 = ManejadorConsulta.Consultar_Valores(tabla1, Base);
        ResultSet rsTabla4 = ManejadorConsulta.Consultar_Valores(tabla2, Base);
        //para desglosar los datos que estan dentro del resultSet(de esta forma podremos obtener los datos que se requiera)
        ResultSetMetaData mdT1 = rsTabla1.getMetaData();
        ResultSetMetaData mdT2 = rsTabla2.getMetaData();
        //Recupera el numero de columnas que tiene la tablas
        int columnCount = mdT1.getColumnCount();
        int columnCount2 = mdT2.getColumnCount();

        //Creamos arreglos bidimensionales para guardar (numbre columna, tipo de dato de columna)
        for (int i = 0; i < columnCount; i++) {
            nomCol1.add(mdT1.getColumnName(i + 1));
            tipoCol1.add(mdT1.getColumnTypeName(i + 1));
        }
        for (int i = 0; i < columnCount2; i++) {
            nomCol2.add(mdT2.getColumnName(i + 1));
            tipoCol2.add(mdT2.getColumnTypeName(i + 1));
        }

        while (rsTabla1.next()) {
            for (String v : nomCol1) {
                if (g < columnCount) {
                    String valor = rsTabla1.getString(v);
                    datCol1.add(g, valor);
                    g = g + 1;
                }
            }
        }
        while (rsTabla2.next()) {
            for (String valor : nomCol2) {
                if (z < columnCount2) {
                    String valorR = rsTabla2.getString(valor);
                    datCol2.add(z, valorR);
                    z = z + 1;
                }
            }
        }
        for (in = 0; in < columnCount; in++) {
            for (k = 0; k < columnCount2; k++) {
                if (datCol1.get(in).equals(datCol2.get(k))) {                   
                    nombreCol2 = mdT2.getColumnName(k + 1);
                    relCol2.add(nombreCol2);
                }
            }
            if (k == columnCount2 - 1) {
                k = 0;
            }
        }

        String retorno = "";
        for (String v : relCol2) {
            retorno = retorno + v + ",";
        }

        return retorno;
    }

}
