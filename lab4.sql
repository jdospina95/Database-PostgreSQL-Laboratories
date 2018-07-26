create or replace function punto1(year integer) RETURNS numeric(10,2) AS $$
	declare
		variable numeric(10,2);
	begin
		select sum(valor) from pago where extract(year from fechahora)=year into variable;
	RETURN variable;
	end;
$$ LANGUAGE plpgsql;

create or replace function punto2(numeric(10)) RETURNS TABLE(nroPartea numeric(10), fechaHoraa timestamp, carroa char(6), cedulaa numeric(10)) AS $$
	declare
		cedulainput alias for $1;
	begin
	FOR nroPartea, fechaHoraa, carroa, cedulaa in select nroParte, fechahora, carro, conductor from parte where conductor = cedulainput LOOP
		RETURN NEXT;
	END LOOP;
	end;
$$ LANGUAGE plpgsql;

create type registros_type as (vnroMatricula numeric, vplaca char(6), vdescripcion varchar(20));

create or replace function punto3(int) returns setof registros_type AS $$
	declare
		year alias for $1;
		resultado registros_type;

	begin
		return query
		select matricula.nroMatricula, matricula.placa, tipocarro.descripcion
		from ((matricula cross join carro) cross join tipocarro)
		where matricula.placa = carro.placa and carro.tipo = tipocarro.codigo and year = extract(year from (matricula.fechaexpedicion));
	end
$$ LANGUAGE plpgsql;

create type sinparte_type as (cedula numeric(10), nombre varchar(30), apellido varchar(30));

create or replace function punto4() returns setof sinparte_type AS $$
	begin
		return query
		select personas.cedula, personas.nombres, personas.apellidos
		from personas 
		EXCEPT
		select personas.cedula, personas.nombres, personas.apellidos
		from personas cross join parte where parte.conductor = personas.cedula;
	end
$$ LANGUAGE plpgsql;

create or replace function punto5(int) returns TABLE (vnombreciudad varchar(20), valor float) AS $$
	declare
		year alias for $1;
	begin
		for vnombreciudad, valor in 
		select ciudad.nombreciudad, sum(pago.valor)
		from (((ciudad cross join personas) cross join parte) cross join pago)
		where ciudad.codciudad = personas.ciudadresidencia and personas.cedula = parte.conductor and 
		parte.nroparte = pago.nroparte and extract(year from(pago.fechahora)) = year group by ciudad.nombreciudad LOOP
		return next;
		end loop;
	end;
$$ LANGUAGE plpgsql;

create or replace function punto6() returns void AS $$
	declare
		rPers RECORD;				
		curs3	CURSOR	IS	select * from personas order by nombres;
		numero int;
	BEGIN
		numero = 1;
		OPEN curs3;
		LOOP
		FETCH curs3 INTO rPers;
		EXIT WHEN NOT FOUND;
		if numero % 3 = 0 	THEN
			RAISE	NOTICE	'Nombre:	%	%',	rPers.nombres,	
			rPers.apellidos;
		END IF;
		numero = numero + 1;
		END	LOOP;
		CLOSE curs3;
		return;			
	end;
$$	LANGUAGE plpgsql;	

create or replace function punto7(int) returns void AS $$
	declare
		rPers RECORD;				
		numero int;
		year alias for $1;
		curs3	CURSOR	IS	select  nombres,apellidos,sum(valormulta) as total from personas 
		inner join parte on (cedula = conductor)
		inner join infraccionparte on (parte.nroParte=infraccionparte.nroParte)
		where extract(year from(parte.fechahora)) = year
		group by nombres,apellidos order by sum(valormulta) desc;
	begin
		numero = 1;
		OPEN curs3;
		LOOP
		FETCH curs3 INTO rPers;
		EXIT WHEN NOT FOUND;
		if numero <=10 THEN
			RAISE NOTICE '% % esta en la posicion % con $%',rPers.nombres,rPers.apellidos,numero,rPers.total;
		end if;
		numero = numero + 1;
		END LOOP;
		CLOSE curs3;
		RETURN;		
	end;
$$	LANGUAGE plpgsql;

ALTER TABLE parte ADD COLUMN nroParteConductor integer;

create or replace function punto8() returns void AS $$
	declare
		curs3 CURSOR is select conductor, fechahora from parte
		order by conductor, fechahora asc
		for update of parte;
		rRecord RECORD;
		rRecord2 RECORD;
		numero int;
	begin
		numero = 1;
		OPEN curs3;
		FETCH curs3 INTO rRecord;
		IF NOT FOUND THEN RETURN;
		end if;
		LOOP
		UPDATE	parte SET nroParteConductor = numero
		WHERE	CURRENT	OF	curs3;
		FETCH curs3 INTO rRecord2;
		EXIT WHEN NOT FOUND;
		if rRecord.conductor = rRecord2.conductor THEN
			numero = numero + 1;
			UPDATE	parte SET nroParteConductor = numero
			WHERE	CURRENT	OF	curs3;
		else numero = 1;
		end if;
		rRecord := rRecord2;
		end LOOP;
		close curs3;
		return;
	end;
$$	LANGUAGE plpgsql;

ALTER TABLE parte DROP COLUMN nroParteConductor;

/*La	Secretaría	de	Transito	ha	decidido	dar	un	beneficio	a	aquellas	personas	que	tienen	dos	o	más	partes	
sin	 pagar.	 A	 estas	 personas	 se	 les	 borrará	 la	 segunda	 infracción	 del	 parte	 más	 antiguo.	 La	 segunda	
infracción	se	elige	ordenando	las	infracciones	por	su	valor,	de	mayor	a	menor. Escriba	una	función	que	
use	CURSOR	FOR	UPDATE	para	borrar	las	infracciones	que	cumplen	las	condiciones	especificadas.
*/

create or replace function punto9() returns void AS $$
	declare
		curs3 CURSOR is select * from (select conductor, parte.nroparte, valormulta from parte
		inner join infraccionparte on (infraccionparte.nroparte = parte.nroparte)
		except select conductor, parte.nroparte, valormulta from parte
		inner join pago on (parte.nroparte = pago.nroparte)
		inner join infraccionparte on (parte.nroparte = infraccionparte.nroparte)) as todo
		order by conductor, valormulta desc
		for update of parte;
		rRecord RECORD;
		rRecord2 RECORD;
		numero int;
	begin
		numero = 1;
		OPEN curs3;
		FETCH curs3 INTO rRecord;
		IF NOT FOUND THEN RETURN;
		end if;
		LOOP
		FETCH curs3 INTO rRecord2;
		EXIT WHEN NOT FOUND;
		if rRecord.conductor = rRecord2.conductor THEN
			numero = numero + 1;
			if numero = 2 THEN
				DELETE FROM PARTE
				WHERE CURRENT OF curs3;
			end if;
		else numero = 1;
		end if;
		rRecord := rRecord2;
		end LOOP;
		close curs3;
		return;
	end;
$$	LANGUAGE plpgsql;
	
	
	
	
	
	
	
	
