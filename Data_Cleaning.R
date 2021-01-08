##############################################
# Single Family Loan-Level Dataset 
##############################################

# * Process flow ;
# * Step 1 : Read Origination & Performance files for all available quarters; 
# * Step 2 : Identify the first instances of loan being ever d30, ever d60, ever d90, ever d120, ever d180;
# * Step 3 : Create a dataset containing D180 instance or pre-d180 default instance for every loan;
# * Step 4 : Create a dataset with first instance of modification for every loan;
# * Step 5 : Merge all datasets to create a master dataset; 
# * Step 6 : Create format variables in the master dataset;


setwd("~/Sample/Data")
#library("zoo")
#library("dplyr")
#library("data.table")
#library("lubridate")


# * Step 1 : Read Origination & Performance files for all available quarters;


#Read Origination Data
origclass <- c ('integer','integer','character', 'integer', 'character', 'real', 'integer', 
                'character','real','integer','integer','integer','real','character','character','character',
                'character', 'character','character','character','character', 'integer', 'integer','character'
                ,'character' ,'character') 
origfile_1999 <- read.table("sample_orig_1999.txt", sep="|", header=FALSE, colClasses=origclass ) 
names(origfile_1999)=c('fico','dt_first_pi','flag_fthb','dt_matr','cd_msa',"mi_pct",'cnt_units',
                       'occpy_sts','cltv' ,'dti','orig_upb','ltv',
                       'int_rt','channel','ppmt_pnlty','prod_type','st', 'prop_type','zipcode','id_loan'
                       ,'loan_purpose', 'orig_loan_term','cnt_borr','seller_name','servicer_name', 'flag_sc') 

#Read Performing Data
svcgclass <- c('character','integer','real','character',  'integer','integer','character','character', 
               'character','integer','real','real','integer', 'integer', 'character','integer','integer',
               'integer','integer','integer','integer','real','real') 
svcgfile_1999<- read.table("sample_svcg_1999.txt", sep="|", header=FALSE, colClasses=svcgclass) 
names(svcgfile_1999)=c('id_loan','period','act_endg_upb','delq_sts','loan_age','mths_remng', 
                       'repch_flag','flag_mod', 'cd_zero_bal', 'dt_zero_bal','new_int_rt',
                       'amt_Non_Int_Brng_Upb','dt_lst_pi','mi_recoveries', 'net_sale_proceeds','non_mi_recoveries',
                       'expenses', 'legal_costs', 'maint_pres_costs','taxes_ins_costs',
                       'misc_costs','actual_loss', 'modcost')


##Remmeber current_upb=Act_endg_upb in SAS code.
class(svcgfile_1999)
head(svcgfile_1999)
View(svcgfile_1999)
svcgfile_1999=as.data.frame(svcgfile_1999)
origfile_1999=as.data.frame(origfile_1999)
svcgfile_1999=svcgfile_1999[order(svcgfile_1999$id_loan,svcgfile_1999$period), ]
svcg_dtls=merge(x = svcgfile_1999, y = origfile_1999[ , c("orig_upb", "id_loan")], 
                by = "id_loan")
svcg_dtls=svcg_dtls[order(svcg_dtls$id_loan,svcg_dtls$period), ]

svcg_dtls_1= svcg_dtls %>% as_tibble() %>% mutate(
  lag_id_loan  	 = lag(id_loan),
  lag2_id_loan 	 = lag(id_loan,2),
  lag_act_endg_upb = lag(act_endg_upb),
  lag_delq_sts 	 = lag(delq_sts),
  lag2_delq_sts 	 = lag(delq_sts,2),
  lag_period 	 = lag(period),
  lag_new_int_rt   = lag(new_int_rt),
  lag_non_int_brng_upb =lag(amt_Non_Int_Brng_Upb)
  
)

svcg_dtls_2=svcg_dtls_1 %>%
  group_by(id_loan) %>%
  arrange(period) %>%
  filter(row_number()== 1 ) 
svcg_dtls_3= svcg_dtls_2 %>% as_tibble() %>% mutate(
  prior_upb=0,
  prior_int_rt=new_int_rt,
  prior_delq_sts='00',
  prior_delq_sts_2='00',
  prior_period= period,
  prior_frb_upb = '.'
)
svcg_dtls_4=svcg_dtls_1 %>%
  group_by(id_loan) %>%
  arrange(period) %>%
  filter(!row_number()== 1 ) 
svcg_dtls_5= svcg_dtls_4 %>% as_tibble() %>% mutate(
  prior_delq_sts=lag_delq_sts,prior_delq_sts_2=ifelse(id_loan %in% lag2_id_loan,lag2_delq_sts,'.'),
  prior_period=lag_period,
  prior_upb=lag_act_endg_upb,
  prior_int_rt=lag_new_int_rt, 
  prior_frb_upb = lag_non_int_brng_upb
  )

svcg_dtls_3=svcg_dtls_3[order(svcg_dtls_3$id_loan,svcg_dtls_3$period), ]
svcg_dtls_5=svcg_dtls_5[order(svcg_dtls_5$id_loan,svcg_dtls_5$period), ]
svcg_dtls_6=rbind(svcg_dtls_3, svcg_dtls_5)
svcg_dtls_6=svcg_dtls_6[order(svcg_dtls_6$id_loan), ]
svcg_dtls_6=svcg_dtls_6 %>% select (-c(lag_act_endg_upb, lag2_id_loan,lag_delq_sts,
                                       lag2_delq_sts, lag2_delq_sts, lag_period,  lag_new_int_rt ))
svcg_dtls_7= svcg_dtls_6 %>% as_tibble() %>% mutate(period_diff_1=(period %% 100 )-(prior_period %% 100))
svcg_dtls_8= svcg_dtls_7 %>% as_tibble() %>% 
  mutate(period_diff=ifelse(period_diff_1 == -11, 1,period_diff_1)) %>% select(-c(period_diff_1))

 
svcg_dtls_9= svcg_dtls_8 %>% as_tibble() %>% 
  mutate(delq_sts_new=ifelse( delq_sts %in% 'R' & period_diff == 1 & prior_delq_sts == '5', 6,
                              ifelse(delq_sts %in% 'R' & period_diff == 1 & prior_delq_sts == '3', 4, 
                            ifelse(delq_sts %in% 'R' & period_diff == 1 & prior_delq_sts == '2', 3, delq_sts))))


# *Creat delinqunet data sets


for ( i in 1:6){
  
  pop_1=svcg_dtls_9[ which(svcg_dtls_9$delq_sts_new == i ),]
  
  pop_final= pop_1 %>% as_tibble() %>% 
    group_by(id_loan) %>%
    arrange(period) %>%
    filter(row_number()== 1 ) %>%
    mutate(dlq_in=1)
  
    nam=paste("dlq_in", i, sep = "_")
    assign(nam, pop_final$dlq_in)
    
    pop_final=pop_final[order(pop_final$id_loan),]
   
     pop_final= pop_final %>% as_tibble() %>% 
       mutate(dlq_up_bkt=ifelse(!act_endg_upb %in% c(0,.),act_endg_upb,
                                                        ifelse(!prior_upb %in% c(0,.),prior_upb, orig_upb)
                                                                 )) 
     nam_1=paste("dlq_up", i, sep = "_")
     assign(nam_1, pop_final$dlq_up_bkt)
     
     
     
    nam_2=paste("pop_final", i, sep = "_")
    assign(nam_2, pop_final)
}
# * Identify the first instances of loan being ever d30, ever d60, ever d90, ever d120, ever d180;

tmp.pop_1_final=cbind(pop_final_1,dlq_in_1,dlq_up_1)
tmp.pop_2_final=cbind(pop_final_2,dlq_in_2,dlq_up_2)
tmp.pop_3_final=cbind(pop_final_3,dlq_in_3,dlq_up_3)
tmp.pop_4_final=cbind(pop_final_4,dlq_in_4,dlq_up_4)
tmp.pop_5_final=cbind(pop_final_5,dlq_in_5,dlq_up_5)
tmp.pop_6_final=cbind(pop_final_6,dlq_in_6,dlq_up_6)
write.csv(tmp.pop_1_final,"tmp_pop_final_1.csv")
write.csv(tmp.pop_2_final,"tmp_pop_final_2.csv")
write.csv(tmp.pop_3_final,"tmp_pop_final_3.csv")
write.csv(tmp.pop_4_final,"tmp_pop_final_4.csv")
write.csv(tmp.pop_5_final,"tmp_pop_final_5.csv")
write.csv(tmp.pop_6_final,"tmp_pop_final_6.csv")


d180=svcg_dtls_9[ which(svcg_dtls_9$delq_sts_new == '6'),]
d180=as.data.frame(d180[order(d180$id_loan,d180$period),])
pred180=svcg_dtls_9 %>% 
  arrange(period,id_loan) %>%
  filter( cd_zero_bal=='03' | delq_sts =='R') %>%
  filter(!c(cd_zero_bal=='03' & delq_sts_new >= 6))
pred180=pred180[order(pred180$id_loan,pred180$period), ]

#If you need functions from both plyr and dplyr, please load plyr first, then dplyr:
library(plyr); library(dplyr)
d180_pr=rbind.fill(d180, pred180)
d180_pr=d180_pr[order(d180_pr$id_loan,d180_pr$period),]
# Sort and remove duplicate and keep " period=min(period)" 
d180_pr_1= d180_pr %>% 
  group_by(id_loan) %>%
  arrange(period) %>%
  distinct(id_loan, .keep_all = TRUE) 

tmp.pd_d180= d180_pr_1 %>%
  mutate( pd_d180_upb=ifelse(!act_endg_upb == 0,act_endg_upb, 
                             ifelse(! prior_upb== 0,prior_upb,orig_upb )
                            )) %>%
  mutate(pd_d180_ind=1)

# * Create a dataset containing D180 instance or pre-d180 default instance for every loan;

write.csv(tmp.pd_d180,"~/tmp_pd180.csv")

#* Create a dataset containing modification records;

mod_loan_0= svcg_dtls_9 %>% 
  group_by(id_loan) %>%
  filter(flag_mod =='Y')

mod_loan_1=merge(x = mod_loan_0, y = origfile_1999[ , c("orig_upb", "id_loan")], 
                by = "id_loan")

# remove dupliate; 
mod_loan_2= mod_loan_1 %>%
  group_by(id_loan) %>%
  distinct(id_loan, .keep_all = TRUE)  # Comment to remove duplicate

mod_loan_2=mod_loan_2[order(mod_loan_2$id_loan,mod_loan_2$period),]
nobs=nrow(mod_loan_2)

if(nobs > 0){
  mod_loan_3=mod_loan_2%>%
    arrange(id_loan,period) %>%
    mutate(mod_ind=1, prior_upb = lag(act_endg_upb))%>%
    filter(flag_mod =='Y')
  
  mod_loan_4=mod_loan_3 %>%
    group_by(id_loan) %>%
    arrange(period) %>%
    filter(row_number()== 1 ) %>%
    mutate(mod_upb=ifelse(!act_endg_upb == 0 ,act_endg_upb,
                          ifelse(!prior_upb==c('0','.'),prior_upb, orig_upb)))
  
  tmp.mod_rcd=mod_loan_4
  write.csv(tmp.mod_rcd,"~/tmp_mod_rcd.csv")
  
}

trm_rcd_0=svcg_dtls_9 %>%
  group_by(id_loan)%>%
  arrange(period)%>%
  filter(row_number()==n())

trm_rcd_1=trm_rcd_0%>%
  mutate(default_upb=ifelse(!cd_zero_bal %in% c('03','09'), 0,
                            ifelse(!act_endg_upb %in% c(0,.),act_endg_upb,
                                   ifelse(prior_upb %in% c(0,.),orig_upb,prior_upb)))) %>%
  mutate(current_int_rt=ifelse(!new_int_rt %in% c(0,.),new_int_rt, prior_int_rt ))%>%
 mutate(vintage=substr(id_loan,3,4)) 

dflt_0=trm_rcd_1 %>%
  mutate(dflt_delq_sts=ifelse(!cd_zero_bal %in% c('03','09'), 0,
                            ifelse(cd_zero_bal %in% '03',delq_sts,
                                   ifelse(!cd_zero_bal %in%'09',prior_delq_sts,prior_delq_sts_2)))) %>%
  mutate(acqn_to_dispn=ifelse(!prior_delq_sts %in% 'R',0,period_diff )) %>%
mutate(frb_upb = prior_frb_upb,vintage=substr(id_loan,3,4) )


tmp.all_orign=origfile_1999
tmp.all_dflt=dflt_0
tmp.all_trm_rcd=trm_rcd_1
write.csv(tmp.all_orign,"tmp_all_orign.csv")
write.csv(tmp.all_dflt,"tmp_all_dflt.csv")
write.csv(tmp.all_trm_rcd,"tmp_all_trm_rcd.csv")

# * Calculate dlq_accrued interest, and collateral loss for each liquidated loan;

dflts_with_loss_data_0 = tmp.all_trm_rcd %>% as_tibble() %>%
  filter(cd_zero_bal %in% c('03','09')) %>% 
  mutate(dt_lst_pi_1=na.locf(dt_lst_pi)) %>% # remove missing with the last obsercation
   mutate(dt_lst_pi_1=paste0(substr(dt_lst_pi_1,5,6), '01', 
                 substr(dt_lst_pi_1,1,4)))%>%
  mutate(period_1=paste0(substr(period,5,6), '01', 
                          substr(period,1,4))) 

dflts_with_loss_data_1= dflts_with_loss_data_0 %>% as_tibble() %>% 
    mutate(days_dlq=-(mdy(dt_lst_pi_1)-mdy(period_1)))

dflts_with_loss_data_2= dflts_with_loss_data_1 %>% as_tibble() %>%  
  filter(!substr(period,1,4) == "1999") %>% 
  mutate(net_sale_proceeds_1=ifelse(net_sale_proceeds =='C', 0, 
                                      as.numeric(net_sale_proceeds))) %>%
  mutate(collateral_Deficiency=ifelse(net_sale_proceeds_1==0 , 0, 
                                      default_upb - net_sale_proceeds_1 )) %>%
  mutate(dlq_accrued_interest=ifelse(net_sale_proceeds =='C', 0, 
                     (default_upb -as.numeric(prior_frb_upb))* (current_int_rt-0.35)* (days_dlq/360/100))) 
  
tmp.dflts_with_loss_data=merge(dflts_with_loss_data_1, 
                           dflts_with_loss_data_2[,c("collateral_Deficiency",
                                                     "net_sale_proceeds_1","dlq_accrued_interest","id_loan")],
                           by="id_loan", all.x=TRUE)
 

write.csv(tmp.dflts_with_loss_data,"tmp_dflts_with_loss_data.csv")

## Setp by step, added more column from diffrent data set which created befor: 
# First : rename some column
colnames(tmp.all_trm_rcd)[which(names(tmp.all_trm_rcd) == "dt_zero_bal")] <- "zero_bal_period"
colnames(tmp.all_trm_rcd)[which(names(tmp.all_trm_rcd) == "delq_sts")] <- "zero_bal_delq_sts"

all_orign_dtl_1= merge(tmp.all_orign, tmp.all_trm_rcd[,c("id_loan","current_int_rt","repch_flag",
                                                         "cd_zero_bal", "zero_bal_period",
                                                         "expenses",
                                                         "mi_recoveries",
                                                         "non_mi_recoveries",
                                                         "net_sale_proceeds",
                                                         "actual_loss",
                                                         "legal_costs",
                                                         "taxes_ins_costs",
                                                         "maint_pres_costs",
                                                         "misc_costs",
                                                         "modcost",
                                                         "dt_lst_pi",
                                                         "prior_upb",
                                                         "act_endg_upb",
                                                         
                                                         "loan_age" ,
                                                         "mths_remng",
                                                         "flag_mod",
                                                         "new_int_rt",
                                                         "amt_Non_Int_Brng_Upb",
                                                         
                                                         "zero_bal_delq_sts")], by= "id_loan",all.x=TRUE )
 
all_orign_dtl_2= merge(all_orign_dtl_1, tmp.dflts_with_loss_data[,c("id_loan","collateral_Deficiency", 
                                                                    "default_upb",
                                                                    "dlq_accrued_interest")], 
                       by="id_loan", all.x=TRUE)

all_orign_dtl_3=merge(all_orign_dtl_2,tmp.pop_1_final[,c("id_loan",
                                                        "dlq_in_1",
                                                       "dlq_up_1")],
                    by="id_loan" , all.x=TRUE)                                                         


all_orign_dtl_4=merge(all_orign_dtl_3, tmp.pop_2_final[,c("id_loan",
                                                        "dlq_in_2",
                                                       "dlq_up_2")],
                     by="id_loan", all.x=TRUE ) 

all_orign_dtl_5=merge(all_orign_dtl_4, tmp.pop_3_final[,c("id_loan",
                                                         "dlq_in_3",
                                                         "dlq_up_3")],
                     by="id_loan" , all.x=TRUE)  

all_orign_dtl_6=merge(all_orign_dtl_5, tmp.pop_4_final[,c("id_loan",
                                                          "dlq_in_4",
                                                         "dlq_up_4")],
                    by="id_loan" , all.x=TRUE )  

all_orign_dtl_7=merge(all_orign_dtl_6, tmp.pop_5_final[,c("id_loan",
                                                        "dlq_in_5",
                                                         "dlq_up_5")],
                     by="id_loan", all.x=TRUE ) 

all_orign_dtl_8=merge(all_orign_dtl_7, tmp.pop_6_final[,c("id_loan",
                                                          "dlq_in_6",
                                                          "dlq_up_6")],
                      by="id_loan" , all.x=TRUE )  

all_orign_dtl_9=merge(all_orign_dtl_8, tmp.mod_rcd[,c("id_loan",
                                                      "mod_ind",
                                                      "mod_upb")],
                      by="id_loan", all.x=TRUE ) 

all_orign_dtl_10=merge(all_orign_dtl_9, tmp.pd_d180[,c("id_loan",
                                                       "pd_d180_ind",
                                                       "pd_d180_upb")],
                       by="id_loan", all.x=TRUE ) 


tmp.all_orign_dtl= all_orign_dtl_10 %>% as_tibble()  %>% 
  mutate(prepay_count=ifelse(cd_zero_bal == c('01','06'), 1,0)) %>%
  mutate(default_count=ifelse(cd_zero_bal == c('03','09'), 1,0)) %>%
  mutate(prepay_upb=ifelse(cd_zero_bal == c('01','03'), prior_upb,0)) %>% 
  mutate(rmng_upb=ifelse(cd_zero_bal == '', act_endg_upb,0))

colnames(tmp.all_orign_dtl)[which(names(tmp.all_orign_dtl) == "dlq_in_1")]="dlq_ever30_ind"
colnames(tmp.all_orign_dtl)[which(names(tmp.all_orign_dtl) == "dlq_up_1")]= "dlq_ever30_upb"

colnames(tmp.all_orign_dtl)[which(names(tmp.all_orign_dtl) == "dlq_in_2")]="dlq_ever60_ind"
colnames(tmp.all_orign_dtl)[which(names(tmp.all_orign_dtl) == "dlq_up_2")]= "dlq_ever60_upb" 

colnames(tmp.all_orign_dtl)[which(names(tmp.all_orign_dtl) == "dlq_in_3")]="dlq_ever90_ind"
colnames(tmp.all_orign_dtl)[which(names(tmp.all_orign_dtl) == "dlq_up_3")]= "dlq_ever90_upb"

colnames(tmp.all_orign_dtl)[which(names(tmp.all_orign_dtl) == "dlq_in_4")]="dlq_ever120_ind"
colnames(tmp.all_orign_dtl)[which(names(tmp.all_orign_dtl) == "dlq_up_4")]= "dlq_ever120_upb"

colnames(tmp.all_orign_dtl)[which(names(tmp.all_orign_dtl) == "dlq_in_6")]="dlq_ever180_ind"
colnames(tmp.all_orign_dtl)[which(names(tmp.all_orign_dtl) == "dlq_up_6")]= "dlq_ever180_upb"

tmp.all_orign_dtl=tmp.all_orign_dtl[order(tmp.all_orign_dtl$id_loan),]

tmp.all_orign_dtl_1= tmp.all_orign_dtl %>% as_tibble()  %>%  
  mutate(orign_year=ifelse(substr(id_loan,3,4) == 99, 1999,paste0('20',substr(id_loan,3,4)))) %>%
  mutate(dispn_yr=substr(zero_bal_period,1,4)) %>% 
  mutate(vintage=substr(id_loan,3,4)) 

write.csv(tmp.all_orign_dtl_1,"tmp_all_orign_dtl_1.csv")

#############################
 #  Creaet Loss Given Default
#############################

lgd_1999=tmp.all_orign_dtl_1 %>% as_tibble() %>% 
  filter(default_count == 1)
write.csv(lgd_1999,"lgd_1999.csv")
  
  
