CREATE TABLE IF NOT EXISTS public.clientes (
                                               cod_cliente VARCHAR(10) PRIMARY KEY,
    nombre VARCHAR(100),
    apellido1 VARCHAR(100),
    apellido2 VARCHAR(100),
    dni VARCHAR(20),
    correo VARCHAR(150),
    telefono VARCHAR(30),
    dni_ok BOOLEAN,
    dni_ko BOOLEAN,
    telefono_ok BOOLEAN,
    telefono_ko BOOLEAN,
    correo_ok BOOLEAN,
    correo_ko BOOLEAN
    );

-- TARJETAS: 1 fila por cod_cliente (evita duplicados)
CREATE TABLE IF NOT EXISTS public.tarjetas (
                                               cod_cliente VARCHAR(10) PRIMARY KEY,
    fecha_exp VARCHAR(7),
    numero_tarjeta_masked VARCHAR(25),
    numero_tarjeta_hash VARCHAR(80) NOT NULL,
    CONSTRAINT fk_tarjetas_cliente
    FOREIGN KEY (cod_cliente)
    REFERENCES public.clientes(cod_cliente)
    ON UPDATE CASCADE ON DELETE CASCADE
    );

