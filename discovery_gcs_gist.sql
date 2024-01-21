export data options(uri='gs://gcs_bigquery_usecase/*.csv',format='csv',overwrite=true,header=true,field_delimiter=',')as
 (select custno,txnno,search(category,'Outdoor')as outdoor_cat,category,strpos(category,'R')as strpos_cat,
 rpad(category,30,' ')as rpad_cat,reverse(category)as rev_cat,length(category)as len_cat,amount,
 row_number()over(partition by custno order by amount)as rownum_amt,
 rank()over(partition by custno order by amount)as rnk_amt,
 dense_rank()over(partition by custno order by amount)as densernk_amt,
 cume_dist()over(partition by custno order by amount)as cumedist_amt,
 first_value(amount)over(partition by custno order by amount)as first_trans_amt,
 nth_value(amount,3)over(partition by custno order by amount)as third_highest_trans,
 lead(amount)over(partition by custno order by amount)as next_trans,
 lag(amount)over(partition by custno order by amount)as prev_trans from curatedds.trans_pos_part_cluster where loaddt=current_date());