import re
from datetime import datetime

# Столбцы файлов данных. Многие функции ссылаются на порядок столбцов в нём
header = 'Дата,Бенефициарские,Другое,Игровой бот dice.id,Каналы телеграм,Награды делегатам,Награды за посты в Readdle.me,Награды через Визонатор,Прямые награды,Самоапы,Самоапы viz-social-bot,Чаты телеграм,Чаты телеграм (сплит),Эирдропы в чатах телеграм,Номер последнего блока'

#Получение типа транзакции и размера награды в виде {Тип_награды : Размер_в_SHARES}
def parse_tx(transaction):
    tx_timestamp = datetime.fromisoformat(transaction['timestamp'])
    tx_shares = float(transaction['op'][1]['shares'][:-7])
    if transaction['op'][0] == 'receive_award':
        transaction_memo = transaction['op'][1]['memo']
        if re.match(r'telegram:\d', transaction_memo) != None:
            tx_type = 'Чаты телеграм'
        elif re.match(r'channel:@', transaction_memo) != None:     
            tx_type = 'Каналы телеграм'
        elif re.match(r'split:', transaction_memo) !=None:
            tx_type = 'Чаты телеграм (сплит)'
        elif re.match(r'chat:', transaction_memo) !=None:
            tx_type = 'Эирдропы в чатах телеграм'
        elif re.match(r'viz://@', transaction_memo) !=None:
            tx_type = 'Награды за посты в Readdle.me'
        elif transaction_memo in ['', 'award']:
            if transaction['op'][1]['initiator'] == transaction['op'][1]['receiver'] == 'viz-social-bot':
                tx_type = 'Самоапы viz-social-bot'
            elif transaction['op'][1]['initiator'] == transaction['op'][1]['receiver']:
                tx_type = 'Самоапы'
            else:
                tx_type = 'Прямые награды'
        elif re.match(r'Награда за пост: https://golos.io/@', transaction_memo) !=None:
            tx_type = 'Награды за посты в GOLOS.IO'
        elif transaction['op'][1]['receiver'] == 'social':
            tx_type = 'Награды через Визонатор'
        elif transaction['op'][1]['initiator'] == 'dice.id':
            tx_type = 'Игровой бот dice.id'
        else:
            tx_type = ('Другое')
    if transaction['op'][0] == 'benefactor_award':
        if transaction['op'][1]['initiator'] == 'dice.id':
            tx_type = 'Игровой бот dice.id'
        else:
            tx_type = 'Бенефициарские'
    if transaction['op'][0] == 'witness_reward':
            tx_type = 'Награды делегатам'
    return[tx_type, tx_shares]

# Проверка наличия файла и создание его, в случае необходимости
def check_file_exist(filename):
    try:
        f = open(filename)
        f.close()
    except FileNotFoundError:
        f = open(filename, 'w+', encoding='utf-8')
        f.write(header)
        f.close()

# Проверка последней сохраненной даты и номера блока. При отсутствии данных оба принимают значение 0
def get_last_date_and_blocknumber_in_file(filename):
    f = open(filename, 'r', encoding='utf-8')
    line = f.readlines()[-1]
    try:
        last_block_in_data = int(line.split(',')[-1])
        last_date_in_data = datetime.strptime(line.split(',')[0], '%Y-%m-%d %H:%M:%S')
    except:
        last_block_in_data = 0
        last_date_in_data = datetime.strptime('2018-09-29 10:00:00', '%Y-%m-%d %H:%M:%S')
    return{'Последняя сохраненная дата':last_date_in_data, 'Последний сохраненный блок':last_block_in_data}

#Задание когорт, по которым будут собираться и делиться данные по наградам
cohorts = [
    'Дата',
    'Бенефициарские',
    'Другое',
    'Игровой бот dice.id',
    'Каналы телеграм',
    'Награды делегатам',
    'Награды за посты в Readdle.me',
    'Награды через Визонатор',
    'Прямые награды',
    'Самоапы',
    'Самоапы viz-social-bot',
    'Чаты телеграм',
    'Чаты телеграм (сплит)',
    'Эирдропы в чатах телеграм',
    'Номер последнего блока'
]
