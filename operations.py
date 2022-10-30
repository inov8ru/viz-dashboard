import json
import time
import os
import json
import csv
import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from threading import Thread
import params as p
from tqdm import tqdm
from tvizbase.api import Api

def collect_ops_from_block(block_number): #Запись в op_string транзакций из блока block_number
    tx = p.api.get_ops_in_block(block_number)
    for operation in tx:
        p.op_string.append(json.dumps(operation)+'\n')

def collect_ops_in_range(first_block, last_block): #Запись в op_string транзакций из блоков в диапазоне.
    for block in range(first_block, last_block):
        tx = p.api.get_ops_in_block(block)
        time.sleep(0.1)
        for operation in tx:
            p.op_string.append(json.dumps(operation)+'\n')

def blocknum(block): #Получение номера блока.
    block=json.loads(block)
    return block['block']

def parser(): #Парсер транзакций, начиная с последнего записанного в файлы блока. Сохраняет все транзакции в файлы.
    for item in tqdm(range(p.last_block_in_files, p.last_block_in_chain, p.step)):
        th = 0
        for x in range(item, item+p.step, p.r):
            exec("thread{} = Thread(target=collect_ops_in_range, args=(x, x+{},))".format(th,p.r))
            th+=1
        for th in range(p.th_num):
            exec("thread{}.start()".format(th))
        for th in range(p.th_num):
            exec("thread{}.join()".format(th))
        print(len(p.op_string), 'transactions,', x, 'blocks readed')
        if len(p.op_string) >= p.str_len:
            p.op_string.sort(key=blocknum)
            with open('tx_raw/tx' + str(p.f_count) + '.txt', 'w') as fp:
                fp.writelines(p.op_string)
            print('В файл tx'+str(p.f_count)+'.txt записано', len(p.op_string), 'транзакций')
            p.f_count+=1
            p.op_string=[]
    p.op_string.sort(key=blocknum)
    with open('tx_raw/tx' + str(p.f_count) + '.txt', 'w') as fp:
        fp.writelines(p.op_string)
    print('В файл tx'+str(p.f_count)+'.txt записано', len(p.op_string), 'транзакций')
    p.op_string=[]    

def create_csv():
    f_count = len(os.listdir('tx_raw'))
    with open('tx_raw/tx' + str(f_count) +'.txt', 'r') as fp:
        f = fp.readlines()
    last_block_timestamp = datetime.fromisoformat(json.loads(f[(len(f)-1)])['timestamp'])

    print('Чтение транзакций из сохраненных файлов...')
    for f_number in tqdm(range(f_count, 0, -1)):
        with open('tx_raw/tx' + str(f_number) +'.txt', 'r') as fp:
            f = fp.readlines()
        for tx in reversed(f):
            tx_timestamp = datetime.fromisoformat(json.loads(tx)['timestamp'])
            if tx_timestamp.year == p.scan_year:
                interval_in_secs = (last_block_timestamp-tx_timestamp).seconds
                interval_in_days = (last_block_timestamp-tx_timestamp).days
                interval = interval_in_days*24*60*60 + interval_in_secs
                operation = json.loads(tx)
                if operation['op'][0] == 'receive_award':
                    p.block_num.append(operation['block'])
                    p.operation_type.append(operation['op'][0])
                    p.shares.append(float(operation['op'][1]['shares'][:-7]))
                    p.op_in_trx.append(operation['op_in_trx'])
                    p.timestamp.append((datetime.fromisoformat(operation['timestamp']))+timedelta(hours=1))
                    p.trx_id.append(operation['trx_id'])
                    p.trx_in_block.append(operation['trx_in_block'])
                    p.virtual_op.append(operation['virtual_op'])
                    p.initiator.append(operation['op'][1]['initiator'])
                    p.receiver.append(operation['op'][1]['receiver'])
                    p.custom_sequence.append(operation['op'][1]['custom_sequence'])
                    original_memo = operation['op'][1]['memo']
                    if re.match(r'telegram:\d', original_memo) !=None:
                        p.memo.append('Чаты телеграм')
                    elif re.match(r'channel:@', original_memo) != None:     
                        p.memo.append('Каналы телеграм')
                    elif re.match(r'split:', original_memo) !=None:
                        p.memo.append('Чаты телеграм (сплит)')
                    elif re.match(r'chat:', original_memo) !=None:
                        p.memo.append('Эирдропы в чатах телеграм')
                    elif re.match(r'viz://@', original_memo) !=None:
                        p.memo.append('Награды за посты в Readdle.me')
                    elif original_memo in ['', 'award']:
                        if operation['op'][1]['initiator'] == operation['op'][1]['receiver'] == 'viz-social-bot':
                            p.memo.append('Самоапы viz-social-bot')
                        elif operation['op'][1]['initiator'] == operation['op'][1]['receiver']:
                            p.memo.append('Самоапы')
                        else:
                            p.memo.append('Прямые награды')
                    elif re.match(r'Награда за пост: https://golos.io/@', original_memo) !=None:
                        p.memo.append('Награды за посты в GOLOS.IO')
                    elif operation['op'][1]['receiver'] == 'social':
                        p.memo.append('Награды через Визонатор')
                    elif operation['op'][1]['initiator'] == 'dice.id':
                        p.memo.append('Игровой бот dice.id')
                    else:
                        p.memo.append('Другое')
                if operation['op'][0] == 'benefactor_award':
                    p.block_num.append(operation['block'])
                    p.operation_type.append(operation['op'][0])
                    p.shares.append(float(operation['op'][1]['shares'][:-7]))
                    p.op_in_trx.append(operation['op_in_trx'])
                    p.timestamp.append(datetime.fromisoformat(operation['timestamp']))
                    p.trx_id.append(operation['trx_id'])
                    p.trx_in_block.append(operation['trx_in_block'])
                    p.virtual_op.append(operation['virtual_op'])
                    p.initiator.append(operation['op'][1]['initiator'])
                    p.receiver.append(operation['op'][1]['receiver'])
                    p.custom_sequence.append(operation['op'][1]['custom_sequence'])
                    p.trx_id.append(operation['trx_id'])
                    if operation['op'][1]['initiator'] == 'dice.id':
                        p.memo.append('Игровой бот dice.id')
                    else:
                        p.memo.append('Бенефициарские')
                if operation['op'][0] == 'witness_reward':
                    p.block_num.append(operation['block'])
                    p.operation_type.append(operation['op'][0])
                    p.shares.append(float(operation['op'][1]['shares'][:-7]))
                    p.memo.append('Награды делегатам')
                    p.op_in_trx.append(operation['op_in_trx'])
                    p.trx_id.append(operation['trx_id'])
                    p.virtual_op.append(operation['virtual_op'])
                    p.witness.append(operation['op'][1]['witness'])
                    p.timestamp.append(datetime.fromisoformat(operation['timestamp']))
            if interval >= p.scan_delta:
                break
        if interval >= p.scan_delta:
            break
    del f
    print('Создание датафрейма, сортировка...')
    df = pd.DataFrame({'Дата':p.timestamp, 'Shares':p.shares, 'Когорта':p.memo})
    del p.block_num, p.operation_type, p.shares, p.witness, p.op_in_trx, p.timestamp, p.trx_id, p.trx_in_block, p.virtual_op, p.initiator, p.receiver, p.memo, p.custom_sequence
    df = df.sort_values('Дата', ascending=True)

    print('Агрегация данных...')
    df2 = df[['Дата', 'Shares', 'Когорта']]
    df2['Дата по часу'] = df2['Дата'].dt.to_period('H').dt.to_timestamp('H')
    temp_df = pd.pivot_table(df2, values='Shares', index=['Дата по часу'], columns=['Когорта'], aggfunc=np.sum, fill_value=0)
    del df2, df

    print('Сохранение результатов в CSV-файл...')
    temp_df.to_csv('receive_awards_by_hour-'+str(p.scan_year)+'.csv')
    del temp_df