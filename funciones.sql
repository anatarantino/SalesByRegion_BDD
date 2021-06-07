--a
create table intermedia
(
    quarter       text not null,
    month         text not null,
    week          text not null,
    product_type  text not null,
    territory     text not null,
    sales_channel text not null,
    customer_type text not null,
    revenue       double precision,
    cost          double precision,
    constraint intermedia_pkey
        primary key (month, week, product_type, sales_channel, customer_type)
);

--b
create table definitiva
(
    sales_date    date not null,
    product_type  text not null,
    territory     text not null,
    sales_channel text not null,
    customer_type text not null,
    revenue       double precision,
    cost          double precision
);

--c
create or replace FUNCTION newDate() returns TRIGGER
AS $$
    DECLARE auxMonth TEXT;
    DECLARE auxMonthNum int;
    DECLARE auxDay TEXT;
    DECLARE auxDayNum int;
    DECLARE auxYear TEXT;
    DECLARE sales_date date;

BEGIN
    auxMonth = substring(new.month,4,5);
    auxYear = substring(new.quarter,4,7);
    auxMonthNum = case
        WHEN auxMonth = 'Jan' THEN 01
        WHEN auxMonth = 'Feb' THEN 02
        WHEN auxMonth = 'Mar' THEN 03
        WHEN auxMonth = 'Apr' THEN 04
        WHEN auxMonth = 'May' THEN 05
        WHEN auxMonth = 'Jun' THEN 06
        WHEN auxMonth = 'Jul' THEN 07
        WHEN auxMonth = 'Aug' THEN 08
        WHEN auxMonth = 'Sep' THEN 09
        WHEN auxMonth = 'Oct' THEN 10
        WHEN auxMonth = 'Nov' THEN 11
        WHEN auxMonth = 'Dec' THEN 12
    END;
    auxDay = SUBSTRING(new.week, 1, 2);
    auxDayNum = case
        WHEN auxDay = 'W1' THEN 01
        WHEN auxDay = 'W2' THEN 08
        WHEN auxDay = 'W3' THEN 15
        WHEN auxDay = 'W4' THEN 22
        WHEN auxDay = 'W5' THEN 29
    END;

    sales_date=make_date(cast(auxYear as int),auxMonthNum,auxDayNum);
    INSERT INTO definitiva VALUES (sales_date,new.product_type,new.territory,new.sales_channel,new.customer_type,new.revenue,new.cost);
    RETURN new;
end;

$$ LANGUAGE plpgsql;

--\COPY intermedia FROM 'SalesbyRegion.csv' CSV HEADER DELIMITER ',';

--d
create or replace function MedianaMargenMovil(fecha date,n integer)
    returns double precision
    as $$
    declare fecha2 date;
    begin
        fecha2:= fecha - interval '1 month' * n + interval '1 day';
        if n=0 then
            raise info 'La cantidad de meses debe ser mayor a 0';
            return null;
        end if;

        return (select percentile_cont(0.5) within group ( order by revenue-cost )
           from definitiva where sales_date between fecha2 and fecha )::numeric(10,2);
    end;
$$LANGUAGE plpgsql;
select medianamargenmovil(to_date('2011-09-01','YYYY-MM-DD'),5);

--e
CREATE OR REPLACE FUNCTION ReporteVentas(n int)
    RETURNS VOID
    AS $$
    DECLARE
        customer_type_cursor CURSOR(year int) FOR
            SELECT CAST(EXTRACT(year FROM sales_date) AS INT), customer_type, sum(definitiva.revenue) as revenue, sum(definitiva.cost) as cost, sum(definitiva.revenue - definitiva.cost) as Margin
            FROM definitiva WHERE CAST(EXTRACT(year FROM sales_date) AS INT) = year GROUP BY EXTRACT(year FROM sales_date), customer_type;
        customer_type_rec RECORD;
        product_type_cursor CURSOR(year int) FOR
            SELECT CAST(EXTRACT(year FROM sales_date) AS INT), product_type, sum(definitiva.revenue) as revenue, sum(definitiva.cost) as cost, sum(definitiva.revenue - definitiva.cost) as Margin
            FROM definitiva WHERE CAST(EXTRACT(year FROM sales_date) AS INT) = year GROUP BY EXTRACT(year FROM sales_date), product_type;
        product_type_rec RECORD;
        sales_channel_cursor CURSOR(year int) FOR
            SELECT CAST(EXTRACT(year FROM sales_date) AS INT), sales_channel, sum(definitiva.revenue) as revenue, sum(definitiva.cost) as cost, sum(definitiva.revenue - definitiva.cost) as Margin
            FROM definitiva WHERE CAST(EXTRACT(year FROM sales_date) AS INT) = year GROUP BY EXTRACT(year FROM sales_date), sales_channel;
        sales_channel_rec RECORD;

        min_year int;
        year int;
        max_year int;
        aux_year int;
        total_revenue double precision;
        total_cost double precision;
        total_margin double precision;
        aux int;
    BEGIN
        IF n=0 then
            return ;
        end if;

        min_year = extract(year from (select min(sales_date) from definitiva));
        year = min_year;
        max_year = extract(year from (select max(sales_date) from definitiva));
        total_revenue := 0;
        total_cost := 0;
        total_margin := 0;
        aux :=0;

        IF (min_year + n > max_year) THEN
            aux_year = max_year;
        ELSE
            aux_year = min_year + n;
        end if;

        raise notice '-----------------------HISTORIC SALES REPORT--------------------------------';
        raise notice '----------------------------------------------------------------------------';
        raise notice 'Year---Category--------------------------------------Revenue---Cost---Margin';
        raise notice '----------------------------------------------------------------------------';

        WHILE(year <= aux_year) LOOP
            total_revenue = 0;
            total_cost = 0;
            total_margin = 0;
            OPEN customer_type_cursor(year);
            LOOP
                FETCH customer_type_cursor INTO customer_type_rec;
                EXIT WHEN NOT FOUND;
                IF (aux = 0) THEN
                     raise notice '% Customer Type: %               %   %   %',
                        year, customer_type_rec.customer_type, CAST(customer_type_rec.revenue AS INT),CAST(customer_type_rec.cost AS INT), CAST(customer_type_rec.Margin AS INT);
                ELSE
                     raise notice '---- Customer Type: %                %   %   %',
                        customer_type_rec.customer_type, CAST(customer_type_rec.revenue AS INT),CAST(customer_type_rec.cost AS INT), CAST(customer_type_rec.Margin AS INT);
                end if;

                total_revenue := total_revenue + customer_type_rec.revenue;
                total_cost := total_cost + customer_type_rec.cost;
                total_margin := total_margin + customer_type_rec.Margin;
                aux = 1;
            END LOOP;
            CLOSE customer_type_cursor;
            OPEN product_type_cursor(year);
            LOOP
                FETCH product_type_cursor INTO product_type_rec;
                EXIT WHEN NOT FOUND;
                raise notice '---- Product Type: %                  %   %   %',
                    product_type_rec.product_type, CAST(product_type_rec.revenue AS INT), CAST(product_type_rec.cost AS INT), CAST(product_type_rec.Margin AS INT);
            END LOOP;
            CLOSE product_type_cursor;
            OPEN sales_channel_cursor(year);
            LOOP
                FETCH sales_channel_cursor INTO sales_channel_rec;
                EXIT WHEN NOT FOUND;
                raise notice '---- Sales Channel: %                 %   %   %',
                    sales_channel_rec.sales_channel, CAST(sales_channel_rec.revenue AS INT), CAST(sales_channel_rec.cost AS INT), CAST(sales_channel_rec.Margin AS INT);
            END LOOP;
            raise notice '-------------------------------------------%    %   %', CAST(total_revenue AS INT), CAST(total_cost AS INT), CAST(total_margin AS INT);
            CLOSE sales_channel_cursor;
            year = year+1;
            aux = 0;
        END LOOP;
    END;
    $$LANGUAGE plpgsql;
SELECT ReporteVentas(2);



