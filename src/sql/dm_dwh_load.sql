merge into stv2024080626__dwh.global_metrics AS global_metrics USING
(
select transaction_dt::date as date_update,
	   t.currency_code as currency_from,
	   sum(t.amount * coalesce(c.currency_with_div,1)) as amount_total,
	   count(1) as cnt_transactions,
	   sum(t.amount * coalesce(c.currency_with_div,1))/count(distinct t.account_number_from) as avg_transactions_per_account,
	   count(distinct t.account_number_from) as cnt_accounts_make_transactions
  from stv2024080626__staging.transactions as t
  left join (select currency_code,
			        currency_code_with,
			        date_update,
			        currency_with_div,
			        row_number() over (partition by currency_code, currency_code_with, date_update order by date_update asc) as rn
			   from STV2024080626__STAGING.currencies) as c 
	on t.transaction_dt::date = c.date_update
   and t.currency_code = c.currency_code
   and c.currency_code_with = 420
   and c.rn = 1
 where 1 = 1
   and t.account_number_from >= 0
   and t.account_number_to >= 0
   and transaction_dt::date = '{processed_dt}'::date
group by t.transaction_dt::date, 
		 t.currency_code) grouped_transactions
   ON grouped_transactions.date_update = global_metrics.date_update
  and grouped_transactions.currency_from = global_metrics.currency_from
 when matched then
 	update set  amount_total = grouped_transactions.amount_total, 
                cnt_transactions = grouped_transactions.cnt_transactions,  
                avg_transactions_per_account = grouped_transactions.avg_transactions_per_account,
                cnt_accounts_make_transactions = grouped_transactions.cnt_accounts_make_transactions
 when not matched then 
                insert (date_update, 
                        currency_from, 
                        amount_total, 
                        cnt_transactions, 
                        avg_transactions_per_account, 
                        cnt_accounts_make_transactions)
                values (grouped_transactions.date_update, 
                        grouped_transactions.currency_from, 
                        grouped_transactions.amount_total, 
                        grouped_transactions.cnt_transactions, 
                        grouped_transactions.avg_transactions_per_account, 
                        grouped_transactions.cnt_accounts_make_transactions);
    