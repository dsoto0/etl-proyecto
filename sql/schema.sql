-- Tabla: tarjetas (1 tarjeta por cliente)
CREATE TABLE IF NOT EXISTS tarjetas (
     id_tarjeta             BIGSERIAL PRIMARY KEY,
     cod_cliente            VARCHAR(10) NOT NULL UNIQUE,   -- 1 fila por cliente
    fecha_exp              VARCHAR(7)  NOT NULL,          -- "YYYY-MM"
    numero_tarjeta_masked  VARCHAR(25) NOT NULL,          -- XXXX-XXXX-XXXX-9012
    numero_tarjeta_hash    CHAR(64)    NOT NULL,          -- hash hex SHA-256
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_tarjetas_clientes
    FOREIGN KEY (cod_cliente)
    REFERENCES clientes (cod_cliente)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

    CONSTRAINT chk_fecha_exp_formato
    CHECK (fecha_exp ~ '^[0-9]{4}-[0-9]{2}$'),

    CONSTRAINT chk_hash_hex
    CHECK (numero_tarjeta_hash ~ '^[0-9a-f]{64}$')
    );

CREATE INDEX IF NOT EXISTS idx_tarjetas_cod_cliente
    ON tarjetas (cod_cliente);
