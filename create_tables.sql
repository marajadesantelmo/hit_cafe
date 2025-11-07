-- Supabase (PostgreSQL) DDL to create tables for the provided DataFrames.
-- Tables are prefixed with 'hc_' and column names preserve Title Case with spaces via quoted identifiers.

-- 2) Ventas por día y categoría
CREATE TABLE IF NOT EXISTS hc_ventas_por_dia_y_categoria (
	"Fecha" DATE NOT NULL,
	"Categoria" TEXT NOT NULL,
	"Cantidad Total" NUMERIC(18,2),
	"Venta Total" NUMERIC(18,2),
	CONSTRAINT hc_vxdyc_uq UNIQUE ("Fecha", "Categoria")
);

CREATE INDEX IF NOT EXISTS idx_hc_vxdyc_fecha
	ON hc_ventas_por_dia_y_categoria ("Fecha");

CREATE INDEX IF NOT EXISTS idx_hc_vxdyc_categoria
	ON hc_ventas_por_dia_y_categoria ("Categoria");


-- 3) Ventas por día y producto, con promedios
CREATE TABLE IF NOT EXISTS hc_ventas_por_dia_y_producto (
	"Fecha" DATE NOT NULL,
	"Producto" TEXT NOT NULL,
	"Cantidad Total" NUMERIC(18,2),
	"Venta Total" NUMERIC(18,2),
	"Promedio Cantidad Diaria" DOUBLE PRECISION,
	"Promedio Venta Diaria" DOUBLE PRECISION,
	CONSTRAINT hc_vxdyprod_uq UNIQUE ("Fecha", "Producto")
);

CREATE INDEX IF NOT EXISTS idx_hc_vxdyprod_fecha
	ON hc_ventas_por_dia_y_producto ("Fecha");

CREATE INDEX IF NOT EXISTS idx_hc_vxdyprod_producto
	ON hc_ventas_por_dia_y_producto ("Producto");


-- 4) Ventas por sucursal por día
CREATE TABLE IF NOT EXISTS hc_ventas_por_sucursal (
	"Sucursal" TEXT NOT NULL,
	"Fecha" DATE NOT NULL,
	"Venta Sucursal" NUMERIC(18,2),
	CONSTRAINT hc_vxsuc_uq UNIQUE ("Fecha", "Sucursal")
);

CREATE INDEX IF NOT EXISTS idx_hc_vxsuc_fecha
	ON hc_ventas_por_sucursal ("Fecha");

CREATE INDEX IF NOT EXISTS idx_hc_vxsuc_sucursal
	ON hc_ventas_por_sucursal ("Sucursal");


-- 5) Venta general por día
CREATE TABLE IF NOT EXISTS hc_venta_general_por_dia (
	"Fecha" DATE PRIMARY KEY,
	"Venta General" NUMERIC(18,2)
);

CREATE INDEX IF NOT EXISTS idx_hc_vgpd_fecha
	ON hc_venta_general_por_dia ("Fecha");


CREATE TABLE IF NOT EXISTS hc_producto_categoria (
    "Producto" TEXT PRIMARY KEY,
    "Categoria" TEXT
);


-- Add foreign key constraint linking Producto in hc_ventas_por_dia_y_producto 
-- to Producto in hc_producto_categoria
ALTER TABLE hc_ventas_por_dia_y_producto
ADD CONSTRAINT fk_vxdyprod_producto
FOREIGN KEY ("Producto") REFERENCES hc_producto_categoria ("Producto");

ALTER TABLE hc_ventas_por_dia_y_categoria
ADD CONSTRAINT fk_vxdyc_categoria
FOREIGN KEY ("Categoria") REFERENCES hc_producto_categoria ("Categoria");

-- 6) Resumen de ventas por sucursal
CREATE TABLE IF NOT EXISTS hc_resumen_ventas_sucursal (
	"Sucursal" text,
	"Total ventas mes" NUMERIC(18,2),
	"Promedio mes" NUMERIC(18,2),
	"Total ventas hoy" NUMERIC(18,2),
	"Promedio ventas hoy" NUMERIC(18,2)
);

CREATE INDEX IF NOT EXISTS idx_hc_rvs_sucursal
	ON hc_resumen_ventas_sucursal ("Sucursal");

	-- Add primary key constraint to hc_resumen_ventas_sucursal
	ALTER TABLE hc_resumen_ventas_sucursal
	ADD CONSTRAINT hc_rvs_pk PRIMARY KEY ("Sucursal");

	-- Add foreign key constraint linking Sucursal in hc_resumen_ventas_sucursal 
	-- to Sucursal in hc_ventas_por_sucursal
	ALTER TABLE hc_resumen_ventas_sucursal
	ADD CONSTRAINT fk_rvs_sucursal
	FOREIGN KEY ("Sucursal") REFERENCES hc_ventas_por_sucursal ("Sucursal");

	-- 7) Metricas generales
	CREATE TABLE IF NOT EXISTS hc_venta_mensual (
		"Sucursal" TEXT NOT NULL,
		"Mes" TEXT NOT NULL,
		"Venta Mensual" NUMERIC(18,2),
	);

	CREATE INDEX IF NOT EXISTS idx_hc_mg_sucursal
		ON hc_metricas_generales ("Sucursal");

	CREATE INDEX IF NOT EXISTS idx_hc_mg_mes
		ON hc_metricas_generales ("Mes");