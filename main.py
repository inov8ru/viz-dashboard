
import datetime
from time import sleep
import operations as ops
from tvizbase.api import Api


api = Api()

awards_in_interval = dict.fromkeys(ops.cohorts, 0) 
count_awards_in_interval = dict.fromkeys(ops.cohorts, 0)
# ops.check_file_exist('receive_awards_by_hour.csv')
# ops.check_file_exist('count_awards_by_hour.csv')

#Узнаем дату, время и номер блока последней записи в файл. При отсутствии данных берем час создания первого блока
last_receive_award = ops.get_last_date_and_blocknumber_in_file('receive_awards_by_hour.csv')
last_receive_award_date = last_receive_award['Последняя сохраненная дата']
last_receive_award_block = last_receive_award['Последний сохраненный блок']

# Задаем длительность блока и интервал сканирования.
block_time_size = datetime.timedelta(seconds=3)
scan_interval = datetime.timedelta(minutes=60)

#Определяем номер блока, с которого нужно начать сканирование.
scan_start_block = last_receive_award_block+1

#Определяем время, с которого нужно начать таймер.
scan_start_at = last_receive_award_date + block_time_size

# Парсим.
while True:
    # Проверяем, прошел ли интервал сканирования с времени последнего сканирования
    if (datetime.datetime.now() - last_receive_award_date) >= scan_interval:
            
        block = api.get_ops_in_block(scan_start_block)
        block_time = datetime.datetime.fromisoformat(block[0]['timestamp'])
        for transaction in block:
            if transaction['op'][0] in ('receive_award', 'benefactor_award', 'witness_reward'):
                award_in_tx = ops.parse_tx(transaction)
                cohort_in_tx = (award_in_tx[0])

                awards_in_interval[cohort_in_tx] += award_in_tx[1]
                count_awards_in_interval[cohort_in_tx] += 1
            
        # Проверяем, не является ли блок последним в интервале таймера сканирования
        if block_time - scan_start_at + block_time_size >= scan_interval:
            awards_in_interval_list = []
            count_awards_in_interval_list = []
            # print('Добавляем данные в файлы')
            # Сбрасываем в 0 агрегированные данные по когортам
            for item in ops.cohorts[1:-1]:
                awards_in_interval_list.append(str(awards_in_interval[item]))
                count_awards_in_interval_list.append(str(count_awards_in_interval[item]))
                awards_in_interval[item] = 0
                count_awards_in_interval[item] = 0

            with open('receive_awards_by_hour.csv', 'a', encoding='utf-8') as f:
                f.write(str(block_time) + ',' + ','.join(awards_in_interval_list) + ',' + str(scan_start_block) + '\n')

            with open('count_receive_awards_by_hour.csv', 'a', encoding='utf-8') as f:
                f.write(str(block_time) + ',' + ','.join(count_awards_in_interval_list) + ',' + str(scan_start_block) + '\n')

            print(datetime.datetime.now(),'Файлы обновлены. Добавлены данные к дате', block_time)
            scan_start_at = block_time + block_time_size
        
        scan_start_block += 1
    else:
        sleep(scan_interval.seconds)
        print(datetime.datetime.now(), 'Ожидание новых данных...')