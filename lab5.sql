create table if not exists deudaCiudad(
codigoCiudad numeric(5) references ciudad,
valorDeuda float
);

insert into deudaCiudad
select ciudadresidencia, sum(valormulta) as total from
((parte natural join infraccionparte) cross join personas) cross join ciudad
where parte.conductor = personas.cedula
and personas.ciudadresidencia = ciudad.codciudad
and (infraccionparte.nroparte NOT IN (select nroparte from pago))
group by ciudadresidencia;


create or replace function actualizarDeudita() returns TRIGGER AS $$
	declare
		ciudadQ numeric(5);
		totalQ float;
	begin
		select ciudadresidencia from infraccionparte 
		inner join parte on (infraccionparte.nroParte = parte.nroParte)
		inner join personas on (parte.conductor = personas.cedula)
		where new.nroParte = parte.nroParte
		into ciudadQ;
		
		select sum(valormulta) as total from
		((parte natural join infraccionparte) cross join personas)
		where parte.conductor = personas.cedula
		and personas.ciudadresidencia = ciudadQ
		and (infraccionparte.nroparte NOT IN (select nroparte from pago))
		into totalQ;
		
		update deudaCiudad set valorDeuda = totalQ
		where codigoCiudad = ciudadQ;
		
	return NULL;
	end;
$$	LANGUAGE plpgsql;

create trigger actualizarDeuda
after update or insert or delete on infraccionparte
for each row execute procedure
actualizarDeudita();

/* Update para probar el punto */
update infraccionparte set valormulta = 0
where (nroParte = 9307398);

/*
Crear	un	trigger	que	evite	actualizaciones	del	atributo	multaSalariosMin	en	la	tabla	infracción	cuando	
el	nuevo	valor	es	menor	que	el	80%	del valor anterior
*/

create or replace function evitarActualizacion() returns TRIGGER AS $$
	begin
		if (new.multaSalariosMin < (old.multaSalariosMin*0.8)) then
			update infraccion set multaSalariosMin = old.multaSalariosMin where multaSalariosMin = new.multaSalariosMin;
			RAISE NOTICE 'Valor nuevo por debajo del 80 porciento, no se ha actualizado';
		end if;
	return NULL;
	end;
$$	LANGUAGE plpgsql;
	
create trigger evitarActualizacion
after update on infraccion
for each row execute procedure
evitarActualizacion();

update infraccion set multasalariosmin = 26
where (multaSalariosMin = 25);


/*
Cuando una persona va a pagar un parte, se piden los datos de la persona (cédula y nombre) y los
datos del parte (número) y el valor a pagar. Se requiere un trigger para verificar que la persona NO
tiene partes anteriores (con fecha mas antigua) pendientes de pago. Si los hay, el trigger debe registrar
el pago cancelando primero los partes anteriores, aplicando valores parciales de la deuda hasta donde
alcance el dinero del pago. En este caso se generan varios pagos, uno por cada parte al que se abone
pago.
Ejemplo: María Jimenez tiene los siguientes partes pendientes de pago:
Parte 5812 de Mayo 8 de 2008 por valor de $350000
Parte 9481 de Agosto 23 de 20012 por valor de $165000
Parte 18276 de Abril 11 de 2016 por valor de $645000
María va a pagar el parte 18276 por $ 645000, dado que hay partes más antiguos, el pago se aplica de
la siguiente manera:
Recibo número 29823, fecha actual, valor $350000, para el parte número 5812
Recibo número 29824, fecha actual, valor $165000, para el parte número 9481
Recibo número 29825, fecha actual, valor $130000, para el parte número 18276
*/

create or replace function pagarParte() returns TRIGGER AS $$
	declare 
		/* Profe aqui no vi como obtener la cedula a partir del new o el old ya que se toma el trigger
		   para la tabla pago y alli no se esta especificando una cedula, tampoco asi que cree un materialized
		   view que uniera pago y parte para asi obtener la cedula*/
		cedulacond alias for TG_ARGV;
		valorpago numeric(10,2);
		curs cursor is
		select nroparte,fechahora,sum(valormulta) as deudaactual
		from parte 
		natural join infraccionparte 
		where nroparte in (select nroparte from
			(select nroparte,sum(valormulta) as debe from infraccionparte group by nroparte) as subconsulta1
			natural join
			(select nroparte,sum(valor) as pagado from pago group by nroparte) as subconsulta2 
			where (pagado < debe)
			union 
			select nroparte from  parte where (nroparte not in (select nroparte from pago)) and conductor = cedulacond)
		group by nroparte order by fechahora
		for update of pago;
		r record;
	begin
		valorpago = new.valor;
		open curs;
		loop
		fetch curs into r;
		exit when not found;
		if valorpago > r.deudaactual then 
			insert into pago 
				values (new.nroRecibo,CURRENT_TIMESTAMP,
					r.deudaactual,r.nroparte);
			valorpago = valorpago - r.deudaactual;
		else 
			insert into pago 
				values (new.nroRecibo,CURRENT_TIMESTAMP,
					valorpago,r.nroparte);
			valorpago = 0;
		end if;
		end loop;
		return null;
	end; 
$$ language plpgsql;

create materialized view pagos as (select pago.*,parte.conductor from pago cross join parte where (parte.nroParte = pago.nroParte));

/* este punto no funciona ya que no me deja poner el trigger para inserts en una materialized view, se me ocurrio tambien hacer
   un alter table a pago y agrear la columna cedula del conductor pero no sabia si eso funcionaria*/
create trigger pagarParte
before insert on pago
for each row execute procedure pagarParte(conductor);


/*
4. Cree índices que agilicen la ejecución de las siguientes consultas:
a. SELECT apellido, nombre, cedula
FROM personas
ORDER BY apellido, nombre;
b. SELECT cedula, apellido, nroparte, fechaparte 
FROM personas JOIN parte ON (conductor = cedula)
WHERE fechaparte >= '2005/08/01';
c. Proponga una consulta que use uno de los índices creados en los puntos a o b
5.
a. Cree una vista que seleccione los datos (cédula y nombre) de las personas que tienen algún
parte que no se ha pagado completamente (el valor de los pagos es menor que el valor de las
multas).
b. Use la vista anterior en una consulta donde se seleccionen los datos de las personas que están
al día en el pago de sus multas.
*/

CREATE INDEX nombres ON personas (Apellidos desc,Nombres desc,cedula);

CREATE MATERIALIZED VIEW tablatemp AS (SELECT cedula, apellidos, nroparte, fechahora 
FROM personas JOIN parte ON (conductor = cedula)
WHERE fechahora >= '2005/08/01');

CREATE INDEX cedulas ON tablatemp (cedula, apellidos, nroparte, fechahora);

select * from personas inner join parte on (conductor = cedula) WHERE fechahora >= '2005/08/01' order by apellidos, nombres desc;

create view pagoincompleto as (select cedula, nombres from
							   personas inner join parte on (personas.cedula = parte.conductor)
							   where nroparte not in (select nroparte from (select nroparte,sum(valormulta) as debe from 
							   infraccionParte group by nroparte) as query1 natural join (select nroParte, sum(valor) as pago
							   from pago group by nroparte) as query2
							   where (debe=pago))
							  );

select * from personas where cedula not in (select cedula from pagoincompleto) order by nombres, apellidos asc;