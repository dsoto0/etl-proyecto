-- schema.sql (PostgreSQL)
-- Crea las tablas cleaned: clientes y tarjetas

BEGIN;

CREATE TABLE IF NOT EXISTS clientes (
  cod_cliente   VARCHAR(10) PRIMARY KEY,
  nombre        VARCHAR(60)  NOT NULL,
  apellido1     VARCHAR(60)  NOT NULL,
  apellido2     VARCHAR(60),
  dni           VARCHAR(20),
  correo        VARCHAR(120),
  telefono      VARCHAR(20),

  dni_ok        BOOLEAN,
  dni_ko        BOOLEAN,
  telefono_ok   BOOLEAN,
  telefono_ko   BOOLEAN,
  correo_ok     BOOLEAN,
  correo_ko     BOOLEAN,

  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tarjetas (
  id_tarjeta            BIGSERIAL PRIMARY KEY,
  cod_cliente           VARCHAR(10) NOT NULL,
  fecha_exp             VARCHAR(7)  NOT NULL,    
  numero_tarjeta_masked VARCHAR(25) NOT NULL,
  numero_tarjeta_hash   CHAR(64)    NOT NULL,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT fk_tarjetas_clientes
    FOREIGN KEY (cod_cliente)
    REFERENCES clientes (cod_cliente)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);


DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'uq_tarjetas_cliente_hash'
  ) THEN
    ALTER TABLE tarjetas
      ADD CONSTRAINT uq_tarjetas_cliente_hash UNIQUE (cod_cliente, numero_tarjeta_hash);
  END IF;
END $$;


CREATE INDEX IF NOT EXISTS idx_tarjetas_cod_cliente
  ON tarjetas (cod_cliente);

COMMIT;
