create table public.ventas (

    folio integer not null,
    fecha_venta timestamp not null,
    cliente text not null,
    sucursal text not null,
    metodo_venta text not null,
    tipo_pago text not null,
    unidades_vendidas integer not null,
    notaventa text not null,

    total numeric(12,2) not null,
    pago_recibido numeric(12,2),

    metodo_pago text,
    cuenta_deposito text,
    confirmacion_pago text,
    articulo text,
    se_paga text,

    comision_vendedor_puertas numeric(10,4),
    comision_vendedor_chapas numeric(10,4),
    comision_vendedor_instalaciones numeric(10,4),
    comision_vendedor_hc numeric(10,4),
    comision_chapas_hc numeric(10,4),

    comision_supervisor_puertas numeric(10,4),
    comision_supervisor_chapas numeric(10,4),
    comision_supervisor_instalaciones numeric(10,4),

    comision_coordinador numeric(10,4),
    comision_elena numeric(10,4),
    comision_osvaldo numeric(10,4),
    comision_july numeric(10,4),

    created_at timestamp default now(),

    constraint ventas_pk primary key (folio, sucursal, fecha_venta)

);


create table public.comisiones (

    fecha_inicial timestamp not null,
    fecha_final timestamp not null,
    sucursal text not null,

    total numeric(12,2) not null,
    total_chapa numeric(12,2) not null,
    instalaciones_vendedor integer not null,
    total_instalaciones numeric(12,2) not null,

    total_puertas_hc numeric(12,2) not null,
    total_c_hc numeric(12,2) not null,

    comision_vendedor numeric(12,2) not null,
    comision_chapas numeric(12,2) not null,
    comision_instalaciones numeric(12,2) not null,

    comision_vendedor_hc numeric(12,2) not null,
    comision_chapas_hc numeric(12,2) not null,

    puertas numeric(12,2) not null,
    instalaciones numeric(12,2) not null,
    chapas numeric(12,2) not null,

    coordinador numeric(12,2) not null,
    elena numeric(12,2) not null,
    osvaldo numeric(12,2) not null,
    july numeric(12,2) not null,

    created_at timestamp default now(),

    constraint comisiones_pk primary key (fecha_inicial, fecha_final, sucursal)

);

create index idx_ventas_fecha
on public.ventas(fecha_venta);

create index idx_ventas_sucursal
on public.ventas(sucursal);

create index idx_comisiones_fecha
on public.comisiones(fecha_inicial, fecha_final);

create index idx_comisiones_sucursal
on public.comisiones(sucursal);