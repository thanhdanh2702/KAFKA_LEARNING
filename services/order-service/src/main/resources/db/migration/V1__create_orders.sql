
CREATE TABLE IF NOT EXISTS orders (
    order_id        VARCHAR(255)    NOT NULL,
    event_id        VARCHAR(255)    NOT NULL,
    customer_id     VARCHAR(255)    NOT NULL,
    product_id      VARCHAR(255)    NOT NULL,

    quantity        INTEGER         NOT NULL,
    unit_price      NUMERIC(12, 2)  NOT NULL,
    total_amount    NUMERIC(14, 2)  NOT NULL,

    currency        VARCHAR(10)     NOT NULL,
    status          VARCHAR(50),

    idempotency_key VARCHAR(255)    NOT NULL,

    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_orders
        PRIMARY KEY (order_id),

    CONSTRAINT uq_orders_event_id
        UNIQUE (event_id),

    CONSTRAINT uq_orders_idempotency_key
        UNIQUE (idempotency_key),

    CONSTRAINT chk_orders_quantity
        CHECK (quantity > 0),

    CONSTRAINT chk_orders_unit_price
        CHECK (unit_price >= 0),

    CONSTRAINT chk_orders_total_amount
        CHECK (total_amount >= 0)
);