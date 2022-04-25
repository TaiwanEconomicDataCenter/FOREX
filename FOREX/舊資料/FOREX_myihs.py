# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
#from FOREX_concat import CONCATE, readExcelFile

ENCODING = 'utf-8-sig'

start_year = 2010
latest = True
NEW = True
if NEW == True:
    SUFFIX = '_new'
else:
    SUFFIX = ''
NAME = 'FOREX_myihs_'+str(start_year)+SUFFIX
data_path = './data/'
out_path = "./output/"
databank = 'FOREX'
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'last', 'base', 'quote', 'snl', 'source', 'form_e', 'form_c']
#merge_file = readExcelFile(out_path+'FOREX_key.xlsx', header_ = 0, sheet_name_='FOREX_key')
this_year = start_year+11 #datetime.now().year + 1
if latest == True:
    update = datetime.today()
else:
    update = str(start_year+10)+'-01-31'
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break

# 回報錯誤、儲存錯誤檔案並結束程式
def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()

def readFile(dir, default=pd.DataFrame(), acceptNoFile=False, \
             header_=None,skiprows_=None,index_col_=None,skipfooter_=0,encoding_=ENCODING):
    try:
        t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        encoding=encoding_,engine='python')
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            ERROR('找不到檔案：'+dir)
    except:
        try: #檔案編碼格式不同
            t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,\
                        engine='python')
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, \
             header_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,sheet_name_=None):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            ERROR('找不到檔案：'+dir)
    except:
        try: #檔案編碼格式不同
            t = pd.read_excel(dir, header=header_,skiprows=skiprows_,index_col=index_col_,sheet_name=sheet_name_)
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

def takeFirst(alist):
    return alist[0]

AREMOS_forex = readExcelFile(data_path+'forex2020.xlsx', header_ = [0], sheet_name_='forex')
Country = readFile(data_path+'Country.csv', header_ = 0)
CRC = Country.set_index('Country_Code').to_dict()
USD = ['REXA','REXE','REX']
EURO = ['EURDECB','EUREECB','EURD','EURE']
SDR = ['SDRA','SDRE']
OPP = ['REXD','REXI','EURECB','EURIECB','EURI','SDRDA','SDRDE','EUR']
AVG = ['EURDECB','EURECB','SDRA','SDRDA','REXA','REXD','EURD','EUR','REX','W']
END = ['EUREECB','EURIECB','SDRE','SDRDE','REXE','REXI','EURE','EURI']
def IHSBASE(name, suffix):
    if name[:1] == 'A':
        name = name+'.A'
        suffix = suffix+'.A'
    opp = ''
    for key in EURO:
        if name.find(key+suffix) >= 0:
            opp = False
            return 'Euro'
    for key in SDR:
        if name.find(key+suffix) >= 0:
            opp = False
            return 'Special Drawing Rights (SDR)'
    for key in OPP:
        if name.find(key+suffix) >= 0:
            opp = True
    for key in USD:
        if name.find(key+suffix) >= 0:
            opp = False
            return 'United States Dollar (USD)'
    if opp == True:
        code = name[1:4]
        if code == '001':
            code = '1'
        if code in CRC['Currency_Name']:
            return str(CRC['Currency_Name'][code])
        else:
            return ''
    else:
        return '' 
def IHSFORM(name, suffix):
    if name[:1] == 'A':
        name = name+'.A'
        suffix = suffix+'.A'
    for key in AVG:
        if name.find(key+suffix) >= 0:
            return 'Average of observations through period (A)'
    for key in END:
        if name.find(key+suffix) >= 0:
            return 'End of period (E)'
    
    return ''

def OLD_LEGACY(code):
    if code in CRC['Old_legacy_currency']:
        return str(CRC['Old_legacy_currency'][code])
    else:
        return code

Year_list = [tmp for tmp in range(start_year,this_year)]
HalfYear_list = []
for y in range(start_year,this_year):
    for s in range(1,3):
        HalfYear_list.append(str(y)+'-S'+str(s))
#print(HalfYear_list)
Quarter_list = []
for q in range(start_year,this_year):
    for r in range(1,5):
        Quarter_list.append(str(q)+'-Q'+str(r))
#print(Quarter_list)
Month_list = []
for y in range(start_year,this_year):
    for m in range(1,13):
        Month_list.append(str(y)+'-'+str(m).rjust(2,'0'))
#print(Month_list)
calendar.setfirstweekday(calendar.SATURDAY)
Week_list = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='W-SAT')
Week_list_s = pd.date_range(start = str(start_year)+'-01-01',end=update,freq='W-SAT').strftime('%Y-%m-%d')

KEY_DATA = []
SORT_DATA_A = []
SORT_DATA_S = []
SORT_DATA_Q = []
SORT_DATA_M = []
SORT_DATA_W = []
DATA_BASE_A = {}
DATA_BASE_S = {}
DATA_BASE_Q = {}
DATA_BASE_M = {}
DATA_BASE_W = {}
db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
db_table_S_t = pd.DataFrame(index = HalfYear_list, columns = [])
db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
db_table_W_t = pd.DataFrame(index = Week_list, columns = [])
DB_name_A = []
DB_name_S = []
DB_name_Q = []
DB_name_M = []
DB_name_W = []
DB_TABLE = 'DB_'
DB_CODE = 'data'
    
table_num_A = 1
table_num_S = 1
table_num_Q = 1
table_num_M = 1
table_num_W = 1
code_num_A = 1
code_num_S = 1
code_num_Q = 1
code_num_M = 1
code_num_W = 1
snl = 1
start_snl = snl
start_table_A = table_num_A
start_table_S = table_num_S
start_table_Q = table_num_Q
start_table_M = table_num_M
start_table_W = table_num_W
start_code_A = code_num_A
start_code_S = code_num_S
start_code_Q = code_num_Q
start_code_M = code_num_M
start_code_W = code_num_W
CONTINUE = []

#before1 = ['FOREIGN EXCHANGE',') PER','DATA)',')FROM','SOURCE','NOTE','RATESDR','RATESEMI','RATEEND','RATES','MARKET RATE','OFFICIAL RATE','PRINCIPAL RATE','USING','ONWARDD','WEDOLLAR','ESOFFICIAL','MILLIONS','NSAINTERNATIONAL','aA','aE','ReservesClaims','DollarsUnit','DollarSource','www.imf.org','FUNDCURRENCY','DATAU.S.','ORLUXEMBOURG','EMUEURO','Y DATA',' AS','HOUSEHOLDSCANNOT','NACIONALWHICH','WITH ',"#IES",'#']
#after1 = [' FOREIGN EXCHANGE ',') PER ','DATA): ',') FROM',', SOURCE',', NOTE','RATE SDR','RATE SEMI','RATE END','RATES ','MARKET RATE ','OFFICIAL RATE ','PRINCIPAL RATE ','USING ','ONWARD D','WE DOLLAR','ES OFFICIAL',' MILLIONS','NSA INTERNATIONAL','a A','a E','Reserves, Claims','Dollars; Unit','Dollar; Source','','FUND CURRENCY','DATA U.S.','OR LUXEMBOURG','EMU EURO','Y DATA ',' AS ','HOUSEHOLDS CANNOT','NACIONAL WHICH',' WITH ','IES',' ']
before2 = ['Ecb','1 Ecu','Sdr','Ifs','Ihs','Imf','Iso','Exchange S ','Rate S ','Am','Pm','Of ',"People S","People'S",'Usd','Us ','#Name?eekly','#Name?','Cfa','Cfp','Fx','Rate,,','Rate,','Nsa','Cofer','And ', 'In ',')Total','Or ','Luf','Emu ','Rexa','Rexeurd','Rexe','Rexeure','Rexi','Rexeuri','Subsidizedby']
after2 = ['ECB','1 ECU','SDR','IFS','IHS','IMF','ISO','Exchanges ','Rates ','am','pm','of ',"People's","People's",'USD','US ','weekly','','CFA','CFP','Foreign Exchange','Rate,','Rate.','NSA','COFER','and ','in ','): Total','or ','LUF','EMU ','REXA','REXEURD','REXE','REXEURE','REXI','REXEURI','Subsidized by']
before3 = ['CYPrus','EURo']
after3 = ['Cyprus','Euro']

def FOREX_DATA(ind, FOREX_t, AREMOS_forex, value, index, code_num, table_num, KEY_DATA, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, freqlist, frequency, freqnum=None, freqsuffix=[], keysuffix=[], suffix='', wed=False):
    freqlen = len(freqlist)
    NonValue = 'nan'
    if code_num >= 200:
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        if frequency == 'W':
            db_table_t = db_table_t.reindex(Week_list_s)
        DATA_BASE[db_table] = db_table_t
        DB_name.append(db_table)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])

    name = FOREX_t.index[ind].replace('.HIST','').replace('.ARCH','')
    if frequency == 'A': #FOREX_t.iloc[i]['Frequency'] == 'Annual':
        name = name.replace('.A','')

    AREMOS_key = AREMOS_forex.loc[AREMOS_forex['code'] == name].to_dict('list')
    if pd.DataFrame(AREMOS_key).empty == True:
        CONTINUE.append(name)
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl
    
    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    db_table_t[db_code] = ['' for tmp in range(freqlen)]
    if NEW == True:
        desc_e = str(FOREX_t.loc[FOREX_t.index[ind], 'Short Label']).replace('\n',' ')
    else:
        desc_e = str(FOREX_t.loc[FOREX_t.index[ind], 'Long Label']).replace('\n',' ')
    desc_e = desc_e.title()
    for ph in range(len(before2)):
        desc_e = desc_e.replace(before2[ph],after2[ph])
    loc2 = desc_e.find('ISO Code:')+10
    loc3 = loc2+3
    loc4 = desc_e.find('ISO Codes:')+11
    loc5 = loc4+3
    if loc2-10 >= 0:
        desc_e = desc_e.replace(desc_e[loc2:loc3],desc_e[loc2:loc3].upper())
    if loc4-11 >= 0:
        desc_e = desc_e.replace(desc_e[loc4:loc5],desc_e[loc4:loc5].upper())
    for ph in range(len(before3)):
        desc_e = desc_e.replace(before3[ph],after3[ph])
    base = IHSBASE(name, suffix)
    form_e = IHSFORM(name, suffix)
    if NEW == True:
        quote = str(AREMOS_key['quote currency'][0])
        source = str(AREMOS_key['source'][0])
    else:
        quote = str(FOREX_t.loc[FOREX_t.index[ind], 'Unit'])
        source = str(FOREX_t.loc[FOREX_t.index[ind], 'Source'])
    desc_c = ''
    form_c = ''
    
    start_found = False
    last_found = False
    found = False
    for k in range(len(value)):
        if not not keysuffix:
            for word in range(len(keysuffix)):
                if str(index[k]).find(keysuffix[word]) >= 0:
                    freq_index = str(index[k])[:freqnum]+freqsuffix[word]
                    if frequency == 'A':
                        freq_index = int(freq_index)
                    break
                else:
                    freq_index = 'Nan'
        else:
            if frequency == 'W':
                try:
                    if wed == True:
                        freq_index = (date.fromisoformat(index[k])-timedelta(days=4)).strftime('%Y-%m-%d')
                    else:
                        freq_index = (date.fromisoformat(index[k])-timedelta(days=6)).strftime('%Y-%m-%d')
                except ValueError:
                    freq_index = 'Nan'
            else:
                    freq_index = 'Nan'
        if freq_index in db_table_t.index:
            if str(value[k]) == NonValue:
                db_table_t[db_code][freq_index] = ''
            else:
                found = True
                db_table_t[db_code][freq_index] = float(value[k])
                if start_found == False and found == True:
                    if frequency == 'A':
                        start = int(freq_index)
                    else:
                        start = str(freq_index)
                    start_found = True
                if start_found == True:
                    if k == len(value)-1:
                        if frequency == 'A':
                            last = int(freq_index)
                        else:
                            last = str(freq_index)
                        last_found = True
                    else:
                        for st in range(k+1, len(value)):
                            if not not keysuffix:
                                for word in range(len(keysuffix)):
                                    if str(index[st]).find(keysuffix[word]) >= 0:
                                        if str(value[st]) != NonValue:
                                            last_found = False
                                            break
                                        else:
                                            last_found = True
                                    else:
                                        last_found = True
                            else:
                                if str(value[st]) != NonValue:
                                    last_found = False
                                else:
                                    last_found = True
                            if last_found == False:
                                break
                        if last_found == True:
                            if frequency == 'A':
                                last = int(freq_index)
                            else:
                                last = str(freq_index)
        else:
            continue

    if start_found == False:
        if found == True:
            ERROR('start not found: '+str(name))
    elif last_found == False:
        if found == True:
            ERROR('last not found: '+str(name))
    if found == False:
        start = 'Nan'
        last = 'Nan'               

    key_tmp= [databank, name, db_table, db_code, desc_e, desc_c, frequency, start, last, base, quote, snl, source, form_e, form_c]
    KEY_DATA.append(key_tmp)
    sort_tmp = [name, snl, db_table, db_code, start]
    SORT_DATA.append(sort_tmp)
    snl += 1

    code_num += 1
    
    return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl

#print(FOREX_t.head(10))
tStart = time.time()
if NEW == True:
    name_col = 2
else:
    name_col = 3


SHEET_NAME = ['Annual','Semi_Annual','Quarterly','Monthly','Weekly_Wed','Weekly_Fri']#'Daily_5_week','Daily_7_week'
for freq in SHEET_NAME:
    print('Reading file: '+NAME+', sheet: '+freq+' Time: ', int(time.time() - tStart),'s'+'\n')
    FOREX_t = readExcelFile(data_path+NAME+'.xlsx', header_ =0, index_col_=name_col, skiprows_=[0,1], skipfooter_=10, sheet_name_=freq) #3
    #print(FOREX_t)
    index = []
    for dex in FOREX_t.columns:
        if type(dex) == datetime:
            index.append(dex.strftime('%Y-%m-%d'))
        else:
            index.append(dex)
    
    nG = FOREX_t.shape[0]
    print('Total Columns:',nG,'Time: ', int(time.time() - tStart),'s'+'\n')        
    for i in range(nG):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
        sys.stdout.flush()

        value = list(FOREX_t.iloc[i])
        if freq == 'Annual': #str(FOREX_t.iloc[i]['Frequency']) == 'Annual':
            freqnum = 4
            freqsuffix = ['']
            frequency = 'A'
            keysuffix = ['-01-01']
            code_num_A, table_num_A, SORT_DATA_A, DATA_BASE_A, db_table_A, db_table_A_t, DB_name_A, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, value, index, code_num_A, table_num_A, KEY_DATA, SORT_DATA_A, DATA_BASE_A, db_table_A_t, DB_name_A, snl, Year_list, frequency, freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix)
        elif freq == 'Semi_Annual': #str(FOREX_t.iloc[i]['Frequency']) == 'Semi-Annual':
            freqnum = 5
            freqsuffix = ['S1','S2']
            frequency = 'S'
            keysuffix = ['01-01','07-01']
            code_num_S, table_num_S, SORT_DATA_S, DATA_BASE_S, db_table_S, db_table_S_t, DB_name_S, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, value, index, code_num_S, table_num_S, KEY_DATA, SORT_DATA_S, DATA_BASE_S, db_table_S_t, DB_name_S, snl, HalfYear_list, frequency, freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, suffix='.S')
        elif freq == 'Monthly': #str(FOREX_t.iloc[i]['Frequency']) == 'Monthly':
            freqnum = 7
            freqsuffix = ['']
            frequency = 'M'
            keysuffix = ['-']
            code_num_M, table_num_M, SORT_DATA_M, DATA_BASE_M, db_table_M, db_table_M_t, DB_name_M, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, value, index, code_num_M, table_num_M, KEY_DATA, SORT_DATA_M, DATA_BASE_M, db_table_M_t, DB_name_M, snl, Month_list, frequency, freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, suffix='.M')
        elif freq == 'Quarterly': #str(FOREX_t.iloc[i]['Frequency']) == 'Quarterly':
            freqnum = 5
            freqsuffix = ['Q1','Q2','Q3','Q4']
            frequency = 'Q'
            keysuffix = ['01-01','04-01','07-01','10-01']
            code_num_Q, table_num_Q, SORT_DATA_Q, DATA_BASE_Q, db_table_Q, db_table_Q_t, DB_name_Q, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, value, index, code_num_Q, table_num_Q, KEY_DATA, SORT_DATA_Q, DATA_BASE_Q, db_table_Q_t, DB_name_Q, snl, Quarter_list, frequency, freqnum=freqnum, freqsuffix=freqsuffix, keysuffix=keysuffix, suffix='.Q')
        elif freq == 'Weekly_Fri': #str(FOREX_t.iloc[i]['Frequency']) == 'Weekly (Fri)':
            frequency = 'W'
            code_num_W, table_num_W, SORT_DATA_W, DATA_BASE_W, db_table_W, db_table_W_t, DB_name_W, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, value, index, code_num_W, table_num_W, KEY_DATA, SORT_DATA_W, DATA_BASE_W, db_table_W_t, DB_name_W, snl, Week_list, frequency, suffix='.W')
        elif freq == 'Weekly_Wed': #str(FOREX_t.iloc[i]['Frequency']) == 'Weekly (Wed)':
            frequency = 'W'
            code_num_W, table_num_W, SORT_DATA_W, DATA_BASE_W, db_table_W, db_table_W_t, DB_name_W, snl = FOREX_DATA(i, FOREX_t, AREMOS_forex, value, index, code_num_W, table_num_W, KEY_DATA, SORT_DATA_W, DATA_BASE_W, db_table_W_t, DB_name_W, snl, Week_list, frequency, suffix='.W', wed=True)
            
    sys.stdout.write("\n\n") 

if db_table_A_t.empty == False:
    DATA_BASE_A[db_table_A] = db_table_A_t
    DB_name_A.append(db_table_A)
if db_table_S_t.empty == False:
    DATA_BASE_S[db_table_S] = db_table_S_t
    DB_name_S.append(db_table_S)
if db_table_M_t.empty == False:
    DATA_BASE_M[db_table_M] = db_table_M_t
    DB_name_M.append(db_table_M)
if db_table_Q_t.empty == False:
    DATA_BASE_Q[db_table_Q] = db_table_Q_t
    DB_name_Q.append(db_table_Q)
if db_table_W_t.empty == False:
    db_table_W_t = db_table_W_t.reindex(Week_list_s)
    DATA_BASE_W[db_table_W] = db_table_W_t
    DB_name_W.append(db_table_W)       

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_A.sort(key=takeFirst)
repeated_A = 0
for i in range(1, len(SORT_DATA_A)):
    if SORT_DATA_A[i][0] == SORT_DATA_A[i-1][0]:
        repeated_A += 1
        if str(SORT_DATA_A[i-1][4]) == 'Nan':
            target = i-1
            try:
                DATA_BASE_A[SORT_DATA_A[target][2]].drop(columns = SORT_DATA_A[target][3])
            except KeyError:
                target = i
        else:
            target = i
        #print(SORT_DATA_A[i][0],' ',SORT_DATA_A[i-1][1],' ',SORT_DATA_A[i][1],' ',SORT_DATA_A[i][2],' ',SORT_DATA_A[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_A[target][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_A[SORT_DATA_A[target][2]] = DATA_BASE_A[SORT_DATA_A[target][2]].drop(columns = SORT_DATA_A[target][3])
        if DATA_BASE_A[SORT_DATA_A[target][2]].empty == True:
            DB_name_A.remove(SORT_DATA_A[target][2])
    sys.stdout.write("\r"+str(repeated_A)+" repeated annual data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_Q.sort(key=takeFirst)
repeated_Q = 0
for i in range(1, len(SORT_DATA_Q)):
    if SORT_DATA_Q[i][0] == SORT_DATA_Q[i-1][0]:
        repeated_Q += 1
        if str(SORT_DATA_Q[i-1][4]) == 'Nan':
            target = i-1
            try:
                DATA_BASE_Q[SORT_DATA_Q[target][2]].drop(columns = SORT_DATA_Q[target][3])
            except KeyError:
                target = i
        else:
            target = i
        #print(SORT_DATA_Q[i][0],' ',SORT_DATA_Q[i-1][1],' ',SORT_DATA_Q[i][1],' ',SORT_DATA_Q[i][2],' ',SORT_DATA_Q[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_Q[target][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_Q[SORT_DATA_Q[target][2]] = DATA_BASE_Q[SORT_DATA_Q[target][2]].drop(columns = SORT_DATA_Q[target][3])
        if DATA_BASE_Q[SORT_DATA_Q[target][2]].empty == True:
            DB_name_Q.remove(SORT_DATA_Q[target][2])
    sys.stdout.write("\r"+str(repeated_Q)+" repeated quarter data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_M.sort(key=takeFirst)
repeated_M = 0
for i in range(1, len(SORT_DATA_M)):
    if SORT_DATA_M[i][0] == SORT_DATA_M[i-1][0]:
        repeated_M += 1
        if str(SORT_DATA_M[i-1][4]) == 'Nan':
            target = i-1
            try:
                DATA_BASE_M[SORT_DATA_M[target][2]].drop(columns = SORT_DATA_M[target][3])
            except KeyError:
                target = i
        else:
            target = i
        #print(SORT_DATA_M[i][0],' ',SORT_DATA_M[i-1][1],' ',SORT_DATA_M[i][1],' ',SORT_DATA_M[i][2],' ',SORT_DATA_M[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_M[target][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_M[SORT_DATA_M[target][2]] = DATA_BASE_M[SORT_DATA_M[target][2]].drop(columns = SORT_DATA_M[target][3])
        if DATA_BASE_M[SORT_DATA_M[target][2]].empty == True:
            DB_name_M.remove(SORT_DATA_M[target][2])
    sys.stdout.write("\r"+str(repeated_M)+" repeated month data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_S.sort(key=takeFirst)
repeated_S = 0
for i in range(1, len(SORT_DATA_S)):
    if SORT_DATA_S[i][0] == SORT_DATA_S[i-1][0]:
        repeated_S += 1
        if str(SORT_DATA_S[i-1][4]) == 'Nan':
            target = i-1
            try:
                DATA_BASE_S[SORT_DATA_S[target][2]].drop(columns = SORT_DATA_S[target][3])
            except KeyError:
                target = i
        else:
            target = i
        #print(SORT_DATA_S[i][0],' ',SORT_DATA_S[i-1][1],' ',SORT_DATA_S[i][1],' ',SORT_DATA_S[i][2],' ',SORT_DATA_S[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_S[target][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_S[SORT_DATA_S[target][2]] = DATA_BASE_S[SORT_DATA_S[target][2]].drop(columns = SORT_DATA_S[target][3])
        if DATA_BASE_S[SORT_DATA_S[target][2]].empty == True:
            DB_name_S.remove(SORT_DATA_S[target][2])
    sys.stdout.write("\r"+str(repeated_S)+" repeated semiannual data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_W.sort(key=takeFirst)
repeated_W = 0
for i in range(1, len(SORT_DATA_W)):
    if SORT_DATA_W[i][0] == SORT_DATA_W[i-1][0]:
        repeated_W += 1
        if str(SORT_DATA_W[i-1][4]) == 'Nan':
            target = i-1
            try:
                DATA_BASE_W[SORT_DATA_W[target][2]].drop(columns = SORT_DATA_W[target][3])
            except KeyError:
                target = i
        else:
            target = i
        #print(SORT_DATA_W[i][0],' ',SORT_DATA_W[i-1][1],' ',SORT_DATA_W[i][1],' ',SORT_DATA_W[i][2],' ',SORT_DATA_W[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_W[target][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_W[SORT_DATA_W[target][2]] = DATA_BASE_W[SORT_DATA_W[target][2]].drop(columns = SORT_DATA_W[target][3])
        if DATA_BASE_W[SORT_DATA_W[target][2]].empty == True:
            DB_name_W.remove(SORT_DATA_W[target][2])
    sys.stdout.write("\r"+str(repeated_W)+" repeated week data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')
df_key = pd.DataFrame(KEY_DATA, columns = key_list)
df_key = df_key.sort_values(by=['name', 'db_table'], ignore_index=True)
if df_key.iloc[0]['snl'] != start_snl:
    df_key.loc[0, 'snl'] = start_snl
for s in range(1,df_key.shape[0]):
    sys.stdout.write("\rSetting new snls: "+str(s))
    sys.stdout.flush()
    df_key.loc[s, 'snl'] = df_key.loc[0, 'snl'] + s
sys.stdout.write("\n")
#if repeated_A > 0 or repeated_Q > 0 or repeated_M > 0:
print('Setting new files, Time: ', int(time.time() - tStart),'s'+'\n')

DATA_BASE_A_new = {}
DATA_BASE_Q_new = {}
DATA_BASE_M_new = {}
DATA_BASE_S_new = {}
DATA_BASE_W_new = {}
db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
db_table_S_t = pd.DataFrame(index = HalfYear_list, columns = [])
db_table_W_t = pd.DataFrame(index = Week_list_s, columns = [])
DB_name_A_new = []
DB_name_Q_new = []
DB_name_M_new = []
DB_name_S_new = []
DB_name_W_new = []
db_table_new = 0
db_code_new = 0
for f in range(df_key.shape[0]):
    sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
    sys.stdout.flush()
    if df_key.iloc[f]['freq'] == 'A':
        if start_code_A >= 200:
            DATA_BASE_A_new[db_table_A] = db_table_A_t
            DB_name_A_new.append(db_table_A)
            start_table_A += 1
            start_code_A = 1
            db_table_A_t = pd.DataFrame(index = Year_list, columns = [])
        db_table_A = DB_TABLE+'A_'+str(start_table_A).rjust(4,'0')
        db_code_A = DB_CODE+str(start_code_A).rjust(3,'0')
        db_table_A_t[db_code_A] = DATA_BASE_A[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_A
        df_key.loc[f, 'db_code'] = db_code_A
        start_code_A += 1
        db_table_new = db_table_A
        db_code_new = db_code_A
    elif df_key.iloc[f]['freq'] == 'Q':
        if start_code_Q >= 200:
            DATA_BASE_Q_new[db_table_Q] = db_table_Q_t
            DB_name_Q_new.append(db_table_Q)
            start_table_Q += 1
            start_code_Q = 1
            db_table_Q_t = pd.DataFrame(index = Quarter_list, columns = [])
        db_table_Q = DB_TABLE+'Q_'+str(start_table_Q).rjust(4,'0')
        db_code_Q = DB_CODE+str(start_code_Q).rjust(3,'0')
        db_table_Q_t[db_code_Q] = DATA_BASE_Q[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_Q
        df_key.loc[f, 'db_code'] = db_code_Q
        start_code_Q += 1
        db_table_new = db_table_Q
        db_code_new = db_code_Q
    elif df_key.iloc[f]['freq'] == 'M':
        if start_code_M >= 200:
            DATA_BASE_M_new[db_table_M] = db_table_M_t
            DB_name_M_new.append(db_table_M)
            start_table_M += 1
            start_code_M = 1
            db_table_M_t = pd.DataFrame(index = Month_list, columns = [])
        db_table_M = DB_TABLE+'M_'+str(start_table_M).rjust(4,'0')
        db_code_M = DB_CODE+str(start_code_M).rjust(3,'0')
        db_table_M_t[db_code_M] = DATA_BASE_M[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_M
        df_key.loc[f, 'db_code'] = db_code_M
        start_code_M += 1
        db_table_new = db_table_M
        db_code_new = db_code_M
    elif df_key.iloc[f]['freq'] == 'S':
        if start_code_S >= 200:
            DATA_BASE_S_new[db_table_S] = db_table_S_t
            DB_name_S_new.append(db_table_S)
            start_table_S += 1
            start_code_S = 1
            db_table_S_t = pd.DataFrame(index = HalfYear_list, columns = [])
        db_table_S = DB_TABLE+'S_'+str(start_table_S).rjust(4,'0')
        db_code_S = DB_CODE+str(start_code_S).rjust(3,'0')
        db_table_S_t[db_code_S] = DATA_BASE_S[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_S
        df_key.loc[f, 'db_code'] = db_code_S
        start_code_S += 1
        db_table_new = db_table_S
        db_code_new = db_code_S
    elif df_key.iloc[f]['freq'] == 'W':
        if start_code_W >= 200:
            DATA_BASE_W_new[db_table_W] = db_table_W_t
            DB_name_W_new.append(db_table_W)
            start_table_W += 1
            start_code_W = 1
            db_table_W_t = pd.DataFrame(index = Week_list_s, columns = [])
        db_table_W = DB_TABLE+'W_'+str(start_table_W).rjust(4,'0')
        db_code_W = DB_CODE+str(start_code_W).rjust(3,'0')
        db_table_W_t[db_code_W] = DATA_BASE_W[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_W
        df_key.loc[f, 'db_code'] = db_code_W
        start_code_W += 1
        db_table_new = db_table_W
        db_code_new = db_code_W
    
    if f == df_key.shape[0]-1:
        if db_table_A_t.empty == False:
            DATA_BASE_A_new[db_table_A] = db_table_A_t
            DB_name_A_new.append(db_table_A)
        if db_table_Q_t.empty == False:
            DATA_BASE_Q_new[db_table_Q] = db_table_Q_t
            DB_name_Q_new.append(db_table_Q)
        if db_table_M_t.empty == False:
            DATA_BASE_M_new[db_table_M] = db_table_M_t
            DB_name_M_new.append(db_table_M)
        if db_table_S_t.empty == False:
            DATA_BASE_S_new[db_table_S] = db_table_S_t
            DB_name_S_new.append(db_table_S)
        if db_table_W_t.empty == False:
            DATA_BASE_W_new[db_table_W] = db_table_W_t
            DB_name_W_new.append(db_table_W)
sys.stdout.write("\n")
DATA_BASE_A = DATA_BASE_A_new
DATA_BASE_Q = DATA_BASE_Q_new
DATA_BASE_M = DATA_BASE_M_new
DATA_BASE_S = DATA_BASE_S_new
DATA_BASE_W = DATA_BASE_W_new
DB_name_A = DB_name_A_new
DB_name_Q = DB_name_Q_new
DB_name_M = DB_name_M_new
DB_name_S = DB_name_S_new
DB_name_W = DB_name_W_new

print(df_key)
#print(DATA_BASE_t)

print('Time: ', int(time.time() - tStart),'s'+'\n')
"""if merge_file.empty == False:
    df_key, DATA_BASE = CONCATE(df_key, DATA_BASE_A, DB_name_A)
    df_key.to_excel(out_path+NAME+"key.xlsx", sheet_name=NAME+'key')
    with pd.ExcelWriter(out_path+NAME+"database.xlsx") as writer: # pylint: disable=abstract-class-instantiated
        for key in sorted(DATA_BASE.keys()):
            sys.stdout.write("\rOutputing sheet: "+str(key))
            sys.stdout.flush()
            DATA_BASE[key].to_excel(writer, sheet_name = key)
    sys.stdout.write("\n")"""
df_key.to_excel(out_path+NAME+"_key.xlsx", sheet_name=NAME+'_key')
with pd.ExcelWriter(out_path+NAME+"_database.xlsx") as writer: # pylint: disable=abstract-class-instantiated
    for d in DB_name_A:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_A[d].empty == False:
            DATA_BASE_A[d].to_excel(writer, sheet_name = d)
    sys.stdout.write("\n")
    for d in DB_name_M:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_M[d].empty == False:
            DATA_BASE_M[d].to_excel(writer, sheet_name = d)
    sys.stdout.write("\n")
    for d in DB_name_Q:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_Q[d].empty == False:
            DATA_BASE_Q[d].to_excel(writer, sheet_name = d)
    sys.stdout.write("\n")
    for d in DB_name_S:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_S[d].empty == False:
            DATA_BASE_S[d].to_excel(writer, sheet_name = d)
    sys.stdout.write("\n")
    for d in DB_name_W:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_W[d].empty == False:
            DATA_BASE_W[d].to_excel(writer, sheet_name = d)
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')

#print('Total items not found: ',len(CONTINUE), '\n')

OLCurrency = []
SDR = []
LEFT = []
DF_NAME = list(df_key['name'])
freq_list = ['A','M','Q','S','W']
for i in range(AREMOS_forex.shape[0]):
    if str(AREMOS_forex.loc[i, 'code']) not in DF_NAME:
        LEFT.append(AREMOS_forex.loc[i, 'code'])
    if OLD_LEGACY(str(AREMOS_forex.loc[i, 'country_code'])) == 'Y' and str(AREMOS_forex.loc[i, 'code'])[:1] in freq_list and str(AREMOS_forex.loc[i, 'code']).find('REX') >= 0:
        if str(AREMOS_forex.loc[i, 'code']) not in DF_NAME:
            OLCurrency.append(AREMOS_forex.loc[i, 'code'])
    elif OLD_LEGACY(str(AREMOS_forex.loc[i, 'country_code'])) == 'S' and str(AREMOS_forex.loc[i, 'code'])[:1] in freq_list and str(AREMOS_forex.loc[i, 'code']).find('REX') >= 0:
        if str(AREMOS_forex.loc[i, 'code']) not in DF_NAME:
            SDR.append(AREMOS_forex.loc[i, 'code'])
print('Total Old Legacy Currency items not found: ', len(OLCurrency), '\n')
print('Total International Monetary Fund (IMF) SDRs items not found: ', len(SDR), '\n')
print('Items not found: ', len(LEFT), '\n')
print('Time: ', int(time.time() - tStart),'s'+'\n')
