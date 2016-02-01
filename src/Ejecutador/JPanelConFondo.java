package Ejecutador;
import java.awt.Graphics;
import java.awt.Image;
import javax.swing.ImageIcon;
import javax.swing.JPanel;
public class JPanelConFondo extends JPanel
{
    private Image imagen;
    public JPanelConFondo() 
    {
    }
    //constructor con parametro para setear la imagen que queremos mostrar cargandola como ruta
    public JPanelConFondo(String nombreImagen) 
    {
        if (nombreImagen != null) 
        {
            imagen = new ImageIcon(getClass().getResource(nombreImagen)).getImage();
        }
    }
    //constructor con parametro para setear la imagen que queremos mostrar cargandola como una imagen almacenada e algun atributo tipo Image
    public JPanelConFondo(Image imagenInicial) 
    {
        if (imagenInicial != null) 
        {
            imagen = imagenInicial;
        }
    }
    //de la misma forma que en el primer constructor tengo mi set de la imagen desde una ruta como String  y usando el repaint para actualizar
    //el componente de la imagen contenida
    //  repaint(): Lo llamamos explícitamente para que cuando cambiemos la imagen esta se muestre inmediatamente.

    //  Si el parámetro nuevaImagen es null, veremos el fondo por defecto.
    public void setImagen(String nombreImagen) 
    {
        if (nombreImagen != null) 
        {
            imagen = new ImageIcon(getClass().getResource(nombreImagen)).getImage();
        }
        else 
        {
            imagen = null;
        }
        repaint();
    }
    //igual como el segundo constructor tengo mi set de la imagen desde un atributo tipo Image  y usando el repaint para actualizar
    //el componente de la imagen contenida
    public void setImagen(Image nuevaImagen) 
    {
        imagen = nuevaImagen;
        repaint();
    }
    //sobreecribo mi metodo paint agregandole los valores de la imagen ya seteada (imagen con su ancho y alto)
    @Override
    public void paint(Graphics g) 
    {
        if (imagen != null) 
        {
            //g.drawImage: dibujamos la imagen (guardada en el atributo “imagen”) en toda la extension del panel
            g.drawImage(imagen, 0, 0, getWidth(), getHeight(), this);
            //    setOpaque(false): le indicamos al panel que no dibuje su fondo por defecto (sino este taparía la imagen)
            setOpaque(false);
        }
        else
        {
            setOpaque(true);
        } 
        //super.paint(g): le indicamos al panel que continúe dibujando el resto de los componentes
        super.paint(g);
    }
}