--in reality not possible to create new schemas due to persmission, so here is a code as it is expected by design, 
--but real testing of solution was executed on top of provided in Sprint 6 schema, mine is stv2024080626
create schema if not exists pavelshubinityandexru__staging;
create schema if not exists pavelshubinityandexru__dwh;

create table if not exists pavelshubinityandexru__staging.currencies
(
	date_update timestamp(0) NOT NULL,
	currency_code int NOT NULL,
	currency_code_with int NOT NULL,
	currency_with_div numeric(12, 3) NOT NULL
)
order by date_update
segmented by hash(date_update) all nodes
partition by date_update;

create projection if not exists pavelshubinityandexru__staging.currencies_proj AS
select currency_code,
       currency_code_with,
       date_update
       currency_with_div
  from pavelshubinityandexru__staging.currencies
 order by date_update
segmented by hash(currency_code) ALL NODES;


create table if not exists pavelshubinityandexru__staging.transactions 
(
    operation_id uuid primary key, 
    account_number_from int, 
    account_number_to int, 
    currency_code int, 
    country varchar(20), 
    status varchar(20), 
    transaction_type varchar(30), 
    amount int, 
    transaction_dt timestamp(6)
) 
order by transaction_dt 
segmented by hash(operation_id) all nodes
partition by transaction_dt::date;

create projection if not exists pavelshubinityandexru__staging.transactions_proj AS
select operation_id,
       account_number_from, 
       account_number_to, 
       currency_code, 
       country, 
       status, 
       transaction_type, 
       amount, 
       transaction_dt
  from pavelshubinityandexru__staging.transactions
 order by transaction_dt
segmented by hash(operation_id) all nodes;


------Real tables for testing
/*
create table if not exists stv2024080626__staging.currencies
(
	date_update timestamp(0) NOT NULL,
	currency_code int NOT NULL,
	currency_code_with int NOT NULL,
	currency_with_div numeric(12, 3) NOT NULL
)
order by date_update
segmented by hash(date_update) all nodes
partition by date_update;

create projection if not exists stv2024080626__staging.currencies_proj AS
select currency_code,
       currency_code_with,
       date_update
       currency_with_div
  from stv2024080626__staging.currencies
 order by date_update
segmented by hash(currency_code) ALL NODES;

create table if not exists stv2024080626__staging.transactions 
(
    operation_id uuid primary key, 
    account_number_from int, 
    account_number_to int, 
    currency_code int, 
    country varchar(20), 
    status varchar(20), 
    transaction_type varchar(30), 
    amount int, 
    transaction_dt timestamp(6)
) 
order by transaction_dt 
segmented by hash(operation_id) all nodes
partition by transaction_dt::date;

create projection if not exists stv2024080626__staging.transactions_proj AS
select operation_id,
       account_number_from, 
       account_number_to, 
       currency_code, 
       country, 
       status, 
       transaction_type, 
       amount, 
       transaction_dt
  from stv2024080626__staging.transactions
 order by transaction_dt
segmented by hash(operation_id) all nodes;
*/