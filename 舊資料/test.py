# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date

ENCODING = 'utf-8-sig'
out_path = './output/'
column = ['code','country_code', 'frequency', 'from', 'to', 'description', 'rate', 'base currency', 'quote currency', 'source', 'attribute', 'note']

"""
tStart = time.time()
print('Reading file: QNIA_key'+NAME1+', Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = readExcelFile(data_path+'QNIA_key'+NAME1+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='QNIA_key')
print('Reading file: QNIA_key'+NAME2+', Time: ', int(time.time() - tStart),'s'+'\n')
df_key = readExcelFile(data_path+'QNIA_key'+NAME2+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='QNIA_key')
#print('Reading file: MEI_database, Time: ', int(time.time() - tStart),'s'+'\n')
#DATA_BASE_t = readExcelFile(data_path+'MEI_database.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
"""
with open('./forex2020.txt','r') as f:
    lines = f.readlines()
for l in range(len(lines)):
    lines[l] = lines[l].replace('\n','').replace('"','')
#print(lines)

#frequency = 'DAILY'
forex = []
g_t = []
code = ''
country_code = ''
frequency = ''
fromt = ''
to = ''
description = ''
rate = ''
base = ''
quote = ''
source = ''
attribute = ''
note = ''
#last = ''
countS = 0
ignore = False
for l in range(len(lines)):
    #print(lines[l])
    sys.stdout.write("\rLoading...("+str(int((l+1)*100/len(lines)))+"%)*")
    sys.stdout.flush()
    if l+1 >= len(lines):
        forex.append(g_t)
        break
    if not lines[l] or lines[l] == ' ':
        if lines[l+1].find('SERIES') >= 0:
            if g_t != []:
                forex.append(g_t)
            g_t = []
            code = ''
            country_code = ''
            frequency = ''
            fromt = ''
            to = ''
            description = ''
            rate = ''
            base = ''
            quote = ''
            source = ''
            attribute = ''
            note = ''
            ignore = False
    elif ignore == True:
        continue
    elif lines[l].find('#') >= 0 or lines[l].find('HISTORY') >= 0:
        continue
    else:
        if lines[l].find('SERIES') >= 0:
            countS+=1
            loc1 = lines[l].find(':')+1
            loc2 = lines[l].find(' ', loc1)
            loc1n = lines[l].find(':')+2
            loc2n = lines[l].find(':')+5
            code = lines[l][loc1:loc2]
            try:
                country_code = int(lines[l][loc1n:loc2n])
            except:
                country_code = lines[l][loc1n:loc2n]
            g_t.append(code)
            g_t.append(country_code)
        elif lines[l].find('Data') >= 0:
            locf1 = lines[l].find('Data')-1
            frequency = lines[l][:locf1]
            g_t.append(frequency)
            loc3 = lines[l].find('from')+5
            loc4 = lines[l].find('to')-2
            loc5 = lines[l].find('to')+3
            #loc6 = lines[l].find('201', loc5)+4
            fromt = lines[l][loc3:loc4]
            to = lines[l][loc5:]
            if frequency == 'ANNUAL':
                fromt = int(fromt)
                to = int(to)
            g_t.append(fromt)
            g_t.append(to)
        else:
            d = lines[l]
            des = ''
            m = l
            while lines[m+1].find('SERIES') < 0 and lines[l].find('#') < 0:
                des = des+d
                m+=1
                d = lines[m]
                if m+1 >= len(lines):
                    break
            g_t.append(des)
            if des.find('NOTE') >= 0 and des.find('SOURCE') >= 0:
                loc7 = des.find('NOTE')+6
                loc8 = des.find('SOURCE')
                loc9 = des.find('SOURCE')+7
                #loc10 = lines[l].find(':')+2
                #loc11 = lines[l].find(' per')
                #loc12 = lines[l].find('per')+4
                #loc13 = lines[l].find('- ')-1
                note = des[loc7:loc8]
                source = des[loc9:]
                if source.find('IHS C') >= 0:
                    if source.find('=') >= 0:
                        loct1 = source.find('=')+1
                        loct2 = source.find('WITH')
                        note = source[loct1:loct2]
                    source = 'IHS Calculation'
            elif des.find('NOTE') >= 0:
                loc7 = des.find('NOTE')+6
                note = des[loc7:]
            elif des.find('SOURCE') >= 0:
                loc8 = des.find('SOURCE')+7
                source = des[loc8:]
                if source.find('IHS C') >= 0:
                    if source.find('=') >= 0:
                        loct1 = source.find('=')+1
                        loct2 = source.find('WITH')
                        note = source[loct1:loct2]
                    source = 'IHS Calculation'
            elif des.find('Source') >= 0:
                loc8 = des.find('Source')+7
                source = des[loc8:]
                if source.find('IHS C') >= 0:
                    if source.find('=') >= 0:
                        loct1 = source.find('=')+1
                        loct2 = source.find('WITH')
                        note = source[loct1:loct2]
                    source = 'IHS Calculation'
            if des.find('AVERAGE OF') >= 0:
                attribute = 'average of periods'
            elif des.find('END OF') >= 0:
                attribute = 'end of period'
            if des.find('Exchange Rate:') >= 0 or des.find('Exchange Rate (') >= 0:
                loc10 = des.find('-')+1
                loc11 = des.find('- ', loc10)
                loc12 = des.find('- ', loc10)+2
                loc13 = des.find(':')+2
                loc14 = des.find(' per')
                loc15 = des.find('per')+4
                loc16 = des.find('- ')-1
                source = des[loc10:loc11]
                attribute = des[loc12:]
                base = des[loc15:loc16]
                quote = des[loc13:loc14]
            if des.find('Exchange') >= 0 or des.find('EXCHANGE') >= 0 or des.find('FX') >= 0:
                rate = 'exchange rate'
            elif des.find('Interest') >= 0 or des.find('INTEREST') >= 0:
                rate = 'interest rate'
            #base = lines[l][loc12:loc13]
            #quote = lines[l][loc10:loc11]
            g_t.append(rate)
            g_t.append(base)
            g_t.append(quote)
            g_t.append(source)
            g_t.append(attribute)
            g_t.append(note)
            ignore = True
        #else:
        #    g_t.append(lines[l])
        
    #last = l
sys.stdout.write("\n\n")
#print(forex)
"""
for g in forex:
    if len(g) > 7:
        print(g)
""" 
print(countS)
ger = pd.DataFrame(forex, columns=column)
print(ger)
ger.to_excel(out_path+"forex2020.xlsx", sheet_name='forex')



"""
print('Concating file: QNIA_key'+NAME1+', Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = pd.concat([KEY_DATA_t, df_key], ignore_index=True)

print('Concating file: MEI_database, Time: ', int(time.time() - tStart),'s'+'\n')
for d in DB_name_A:
    sys.stdout.write("\rConcating sheet: "+str(d))
    sys.stdout.flush()
    if d in DATA_BASE_t.keys():
        DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_A[d])
    else:
        DATA_BASE_t[d] = DB_A[d]
sys.stdout.write("\n")
for d in DB_name_Q:
    sys.stdout.write("\rConcating sheet: "+str(d))
    sys.stdout.flush()
    if d in DATA_BASE_t.keys():
        DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_Q[d])
    else:
        DATA_BASE_t[d] = DB_Q[d]
sys.stdout.write("\n")
for d in DB_name_M:
    sys.stdout.write("\rConcating sheet: "+str(d))
    sys.stdout.flush()
    if d in DATA_BASE_t.keys():
        DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_M[d])
    else:
        DATA_BASE_t[d] = DB_M[d]
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = KEY_DATA_t.sort_values(by=['name', 'db_table'], ignore_index=True)
unrepeated = 0
#unrepeated_index = []
for i in range(1, len(KEY_DATA_t)):
    if KEY_DATA_t['name'][i] != KEY_DATA_t['name'][i-1] and KEY_DATA_t['name'][i] != KEY_DATA_t['name'][i+1]:
        print(list(KEY_DATA_t.iloc[i]),'\n')
        unrepeated += 1
        #repeated_index.append(i)
        #print(KEY_DATA_t['name'][i],' ',KEY_DATA_t['name'][i-1])
        #key = KEY_DATA_t.iloc[i]
        #DATA_BASE_t[key['db_table']] = DATA_BASE_t[key['db_table']].drop(columns = key['db_code'])
        #unrepeated_index.append(i)
        
    #sys.stdout.write("\r"+str(repeated)+" repeated data key(s) found")
    #sys.stdout.flush()
#sys.stdout.write("\n")
print('unrepeated: ', unrepeated)
#for i in unrepeated_index:
    #sys.stdout.write("\rDropping repeated data key(s): "+str(i))
    #sys.stdout.flush()
    #KEY_DATA_t = KEY_DATA_t.drop([i])
#sys.stdout.write("\n")

KEY_DATA_t.reset_index(drop=True, inplace=True)
if KEY_DATA_t.iloc[0]['snl'] != 1:
    KEY_DATA_t.loc[0, 'snl'] = 1
for s in range(1,KEY_DATA_t.shape[0]):
    sys.stdout.write("\rSetting new snls: "+str(s))
    sys.stdout.flush()
    KEY_DATA_t.loc[s, 'snl'] = KEY_DATA_t.loc[0, 'snl'] + s
sys.stdout.write("\n")
"""
