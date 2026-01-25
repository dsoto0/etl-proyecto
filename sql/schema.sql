CREATE TABLE IF NOT EXISTS clientes (
  cod_cliente   VARCHAR(10) PRIMARY KEY,
  nombre        VARCHAR(60)  NOT NULL,
  apellido1     VARCHAR(60)  NOT NULL,
  apellido2     VARCHAR(60),
  dni           VARCHAR(9),
  correo        VARCHAR(120),
  telefono      VARCHAR(20),

  dni_ok        BOOLEAN,
  dni_ko        BOOLEAN,
  telefono_ok   BOOLEAN,
  telefono_ko   BOOLEAN,
  correo_ok     BOOLEAN,
  correo_ko     BOOLEAN,

  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT chk_fecha_flags_dni
    CHECK (NOT (dni_ok IS TRUE AND dni_ko IS TRUE)),
  CONSTRAINT chk_fecha_flags_tel
    CHECK (NOT (telefono_ok IS TRUE AND telefono_ko IS TRUE)),
  CONSTRAINT chk_fecha_flags_correo
    CHECK (NOT (correo_ok IS TRUE AND correo_ko IS TRUE))
);

-- Tabla: tarjetas
CREATE TABLE IF NOT EXISTS tarjetas (
  id_tarjeta             BIGSERIAL PRIMARY KEY,
  cod_cliente            VARCHAR(10) NOT NULL,
  fecha_exp              VARCHAR(7)  NOT NULL,      -- "YYYY-MM"
  numero_tarjeta_masked  VARCHAR(25) NOT NULL,      -- XXXX-XXXX-XXXX-9012
  numero_tarjeta_hash    CHAR(64)    NOT NULL,      -- hash hex SHA-256
  created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT fk_tarjetas_clientes
    FOREIGN KEY (cod_cliente)
    REFERENCES clientes (cod_cliente)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  CONSTRAINT uq_tarjeta_hash UNIQUE (numero_tarjeta_hash),

  CONSTRAINT chk_fecha_exp_formato
    CHECK (fecha_exp ~ '^[0-9]{4}-[0-9]{2}$'),

  CONSTRAINT chk_hash_hex
    CHECK (numero_tarjeta_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_tarjetas_cod_cliente
  ON tarjetas (cod_cliente);

ALTER TABLE tarjetas DROP CONSTRAINT IF EXISTS uq_tarjeta_hash;
ALTER TABLE tarjetas ADD CONSTRAINT uq_tarjeta_hash_cliente UNIQUE (cod_cliente, numero_tarjeta_hash);