#coding=utf-8
try:
    import ujson as json
except:
    import json
import time
import pymongo
import requests
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

'''
    脚本说明：
    用于下载的命运2大json并进行处理，导出csv文件
    （json的数据结构需要保证没有大的1变化）
    
    - json存到本地
    - 清理mydb数据库
    - 将json简单解析存入mongo
    - 整理mongo数据，并把结果存入新的表
    - 把新的表导出为csv文件
'''

MONGO_LOCAL = 'mongodb://127.0.0.1:27017'
conn = pymongo.MongoClient(MONGO_LOCAL)
db_game = conn.mydb

all_components = '100,101,102,103,104,105,200,201,202,203,204,205,300,301,302,303,304,305,306,307,308,400,401,402,500,600,700,800,900,1000'

# json文件的存放路径
JSON_DIR = '/Users/chenjiayu/Documents/test/destiny2_json'
JSON_FILE_NAME = 'destiny_json_%s' % time.strftime("%Y%m%d", time.localtime(int(time.time())))

# 导出csv文件的路径
OUTPUT_FILE_PATH = '/Users/chenjiayu/Downloads/wiki_excels/destiny2_output'


def api_data(param=None):
    url = 'https://www.bungie.net/Platform/Destiny2/Manifest/'

    headers = {}
    headers['X-API-Key'] = '13f039a81b8348738e76d4795476bf14'

    date_str = time.strftime("%Y%m%d", time.localtime(int(time.time())))

    r = requests.get(url,headers=headers,timeout=15, verify=False)

    r_json = json.loads(r.content)
    try:
        json_path = r_json['Response']['jsonWorldContentPaths']['zh-chs']
        print json_path
    except:
        print 'get json path error'

     
    url = 'https://www.bungie.net' + json_path
    r = requests.get(url,headers=headers,timeout=10, verify=False)

    
    date_str = time.strftime("%Y%m%d", time.localtime(int(time.time())))
    tmp_file_name = 'destiny_json_%s' % date_str
    print tmp_file_name

    with open('%s/%s' % (JSON_DIR, tmp_file_name), mode='wb+') as destination:
        destination.write(r.content)

    print r.status_code


def read_json():
    f = open('%s/%s' % (JSON_DIR, JSON_FILE_NAME), mode='r')
    context = f.read()

    f.close()
    # print 'json load begin'
    r = json.loads(context)
    # print 'json load end'

    count = 1
    category = ''
    for i in r:
        # print count,i,len(r[i])
        count += 1

        item_ids = ['590099826','3154740035','1926152773']
        # item_ids = ['4136768282']
        item_ids = ['737144025', '143442373', '4085212425']
        item_ids = ['4172267910', '3666208348', '599007201', '354401740']
        item_ids = ['2837207746', '4043523819', '1240592695', '2762071195']
        item_ids = ['4230993599']
        item_ids = ['4172267910', '3666208348', '599007201', '354401740', '2285418970']
        item_ids = ['4136768282']
        item_ids = ['1885944937', '1935470627', '1943323491', '3897883278', '392767087']
        item_ids = ['4174481098', '4230993599'] # 两个钢铁女巫
        item_ids = ['3558075535']  
        item_ids = ['1001496800']

        for item_id in item_ids:
            if r[i].get(item_id):
                print '\ngoal is in:', i, item_id
                category = i
                
                goal = r[i][item_id]
                try:
                    name = goal['displayProperties']['name']
                    print '1', name
                    print '2', json.dumps(goal).decode('unicode-escape')
                    print '3', json.dumps(goal['displayProperties']).decode('unicode-escape')
                    print '\n\n'
                except:
                    print '4', json.dumps(goal).decode('unicode-escape')
                    pass

    category = 'DestinyItemCategoryDefinition'
    if category:
        cc = r[category]
        # cat_dict(cc)

        for i in cc:
            if cc[i].get('displayProperties',{}).get('name','') == '塑钢强化模组':
                print json.dumps(cc[i]).decode('unicode-escape')


def cat_dict(dd):
    count = 1
    for i in dd:
        print i, dd[i]['displayProperties'].get('name', 'No name')
        print json.dumps(dd[i]).decode('unicode-escape'),'\n'
        print 
        count += 1
        if count > 2:
            # print '+++++++++++++++++++++++++++\n'
            break


def save_data_into_mongo():
    f = open('%s/%s' % (JSON_DIR, JSON_FILE_NAME), mode='r')
    context = f.read()

    f.close()
    r = json.loads(context)

    count = 0

    for i in r:
        print count,i,len(r[i])
        count += 1

        if i not in [
            'DestinyRecordDefinition',
            'DestinyLoreDefinition',
            'DestinyVendorDefinition',
            'DestinyInventoryItemDefinition',
            'DestinyCollectibleDefinition',
            'DestinyPlugSetDefinition',
            'DestinySandboxPerkDefinition',
            'DestinyStatDefinition',
        ]:
            continue

        print_error = True
        for j in r[i]:
            single_item = r[i][j]

            # DestinyVendorDefinition类存在key='BungieNet.Engine.Contract.Destiny.World.Definitions.IDestinyDisplayDefinition.displayProperties', 直接存入数据库会异常
            value = single_item
            if i == 'DestinyVendorDefinition':
                itemList = single_item.get('itemList', [])
                if not itemList:
                    continue
                value = {'itemList': itemList, 'hash': single_item.get('hash', '')}
                # print single_item

            # db_game[i].update({'key': j}, {'$set':{'value': single_item}}, upsert=True)
            # 改为插入，每次运行都需要把之前的数据删掉
            item = {'key': j, 'value': value}
            try:
                db_game[i].insert(item)
            except:
                if print_error:
                    print 'error, category: %s, item:%s' % (i, item)
                print_error = False

    print '共计 %s 个大类' % count


def arrange_data(name):
    total = 0
    fail_times = 0

    start_id = last_id = (db_game[name].find({}, {'_id': 1}).sort('_id', 1).limit(1)[0]['_id'])
    limit = 3000
    while True:
        query = {'_id': {'$gte': last_id} if last_id == start_id else {'$gt': last_id}}
        r = db_game[name].find(query).sort('_id', 1).limit(limit)
        count = 0
        for i in r:
            last_id = i['_id']

            count += 1
            k = i['key']
            v = i['value']

            if name == 'DestinyPlugSetDefinition':
                item = {}
            else:
                item = v.get('displayProperties', {})
            item['hash'] = str(v['hash'])
            
            flag = get_attr_by_name(v, item, name)
            if not flag:
                fail_times += 1
                continue
            
            # print '[%s] [%s] [%s] done' % (count, k, item.get('name', ''))
            # print item
            db_game['test_%s'%name].update({'hash': item['hash']}, {'$set': item}, upsert=True)
        
        total += count
        print 'name:%s, count:%s, fail_times:%s' % (name, total, fail_times)
        if count < limit:
            break
            
    print 'total:%s, fail_times:%s' % (total, fail_times)


def get_all_stats_key():
    stats_keys = set()
    statTypeHash_keys = set()

    name='DestinyInventoryItemDefinition'
    start_id = last_id = (db_game[name].find({}, {'_id': 1}).sort('_id', 1).limit(1)[0]['_id'])
    limit = 3000

    while True:
        query = {'_id': {'$gte': last_id} if last_id == start_id else {'$gt': last_id}}
        r = db_game[name].find(query).sort('_id', 1).limit(limit)
        count = 0
        for i in r:
            last_id = i['_id']
            count += 1

            v = i['value']

            stats = v.get('stats', {}).get('stats', {})
            for k in stats:
                if k not in stats_keys:
                    stats_keys.add(k)

            investmentStats = v.get('investmentStats', [])
            for j in investmentStats:
                statTypeHash = j.get('statTypeHash')
                if not statTypeHash:
                    continue
                if statTypeHash not in statTypeHash_keys:
                    statTypeHash_keys.add(statTypeHash)
            
        if count < limit:
            break
    return list(stats_keys), list(statTypeHash_keys)


def sp_table1():
    name='DestinyInventoryItemDefinition'

    stats_keys, statTypeHash_keys = get_all_stats_key()
    print '[stats_keys]: ', stats_keys
    print '[statTypeHash_keys]: ', statTypeHash_keys

    total = 0
    fail_times = 0

    start_id = last_id = (db_game[name].find({}, {'_id': 1}).sort('_id', 1).limit(1)[0]['_id'])
    limit = 3000
    while True:
        query = {'_id': {'$gte': last_id} if last_id == start_id else {'$gt': last_id}}
        r = db_game[name].find(query).sort('_id', 1).limit(limit)
        count = 0
        for i in r:
            last_id = i['_id']
            count += 1

            v = i['value']

            if not v.get('stats', {}):
                continue

            item = v['displayProperties']
            item['hash'] = str(v['hash'])
            item['itemTypeDisplayName'] = v.get('itemTypeDisplayName', '')

            # get_attr_by_name(v, item, name)
            item['defaultDamageType'] = v.get('defaultDamageType', '')
            equippingBlock = v.get('equippingBlock', {})
            item['ammoType'] = equippingBlock.get('ammoType', '')
            item['equipmentSlotTypeHash'] = equippingBlock.get('equipmentSlotTypeHash', '')

            quality = v.get('quality', {})
            item['currentVersion'] = quality.get('currentVersion', '')
            versions = quality.get('versions', [])
            item['powerCapHash'] = []
            for j in versions:
                item['powerCapHash'].append(str(j.get('powerCapHash', '')))
            item['powerCapHash'] = ','.join(item['powerCapHash'])

            stats = v.get('stats', {}).get('stats', {})
            for k in stats_keys:
                item[k] = stats.get(k, {}).get('value', '')

            for k in statTypeHash_keys:
                item[str(k)] = ''
            investmentStats = v.get('investmentStats', [])
            for j in investmentStats:
                statTypeHash = str(j.get('statTypeHash', ''))
                if not statTypeHash:
                    continue
                item[statTypeHash] = j.get('value', -1)
            
            db_game['test_%s_2'%name].update({'hash': item['hash']}, {'$set': item}, upsert=True)
        
        total += count
        if count < limit:
            break
            
    print 'count:%s, fail_times:%s' % (count, fail_times)


def get_attr_by_name(v, item, name):
    if name == 'DestinyInventoryItemDefinition':
        item['index'] = v.get('index', -1)
        item['tierTypeHash'] = v.get('inventory', {}).get('tierTypeHash', '')
        item['stackUniqueLabel'] = v.get('inventory', {}).get('stackUniqueLabel', '')
        item['tierTypeName'] = v.get('inventory', {}).get('tierTypeName', '')
        item['itemTypeAndTierDisplayName'] = v.get('itemTypeAndTierDisplayName', '')
        item['itemTypeDisplayName'] = v.get('itemTypeDisplayName', '')
        item['screenshot'] = v.get('screenshot', '')

        item['equipmentSlotTypeHash'] = v.get('equippingBlock', {}).get('equipmentSlotTypeHash', -1)

        item['socketCategoryHash'] = []
        item['socketIndexes'] = []

        sockets = v.get('sockets', {})
        socketCategories = sockets.get('socketCategories', [])
        for i in socketCategories:
            hash_ = i.get('socketCategoryHash', -1)
            indexes = i.get('socketIndexes', [])
            item['socketCategoryHash'].append(str(hash_))
            item['socketIndexes'].append(indexes)

        item['socketCategoryHash'] = ','.join(item['socketCategoryHash'])

        socketEntries = sockets.get('socketEntries', [])
        keys = ['singleInitialItemHash', 'reusablePlugSetHash', 'randomizedPlugSetHash']
        for k in keys:
            item[k] = []
        for i in socketEntries:
            for key in keys:
                value = i.get(key)
                if value is not None:
                    item[key].append(str(value))
        for k in ['reusablePlugSetHash', 'randomizedPlugSetHash']:
            item[k] = ','.join(item[k])

        item['secondarySpecial'] = v.get('secondarySpecial', '')
        item['secondaryIcon'] = v.get('secondaryIcon', '')
        item['loreHash'] = v.get('loreHash', '')
        item['collectibleHash'] = v.get('collectibleHash', '')

        # 根据socketIndexes取singleInitialItemHash, "|"分割
        item['singleInitialItemHash_2'] = []
        try:
            l = []
            for i in item['socketIndexes']:
                l1 = []
                for j in i:
                    l1.append(item['singleInitialItemHash'][j])
                l.append(','.join(l1))
            item['singleInitialItemHash_2'] = '|'.join(l)
        except:
            item['singleInitialItemHash_2'] = 'error'
        item.pop('socketIndexes', '')
        item.pop('singleInitialItemHash', '')

    elif name == 'DestinyCollectibleDefinition':
        item['index'] = v.get('index', -1)
        item['runOnlyAcquisitionRewardSite'] = v.get('acquisitionInfo', {}).get('runOnlyAcquisitionRewardSite', '')
        item['itemHash'] = str(v.get('itemHash', -1))
        item['displayStyle'] = v.get('presentationInfo', {}).get('displayStyle', -1)
        t = v.get('presentationInfo', {}).get('parentPresentationNodeHashes', [])
        tt = []
        for i in t:
            tt.append(str(i))
        item['parentPresentationNodeHashes'] = tt

        item['sourceString'] = v.get('sourceString', '')

    elif name == 'DestinyPlugSetDefinition':
        item['index'] = v.get('index', -1)
        item['reusablePlugItems']= []
        reusablePlugItems = v.get('reusablePlugItems', [])
        for i in reusablePlugItems:
            item['reusablePlugItems'].append(str(i.get('plugItemHash', -1)))
        item['isFakePlugSet'] = v.get('isFakePlugSet', '')
    
    elif name == 'DestinySandboxPerkDefinition':
        item['damageType'] = v.get('damageType', -1)
        item['index'] = v.get('index', -1)

    elif name == 'DestinyRecordDefinition':
        loreHash = v.get('loreHash', '')
        if not loreHash:
            return False

        lore = db_game['DestinyLoreDefinition'].find_one({'key': str(loreHash)})
        item['description'] = lore['value']['displayProperties']['description']
    
    elif name == 'DestinyVendorDefinition':
        itemList = v.get('itemList', [])
        if not itemList:
            return False

        itemHashes = []
        for j in itemList:
            itemHash = j.get('itemHash')
            itemHashes.append(str(itemHash))
        item['itemHashes'] = itemHashes

    elif name == 'DestinyLoreDefinition':
        if item['description'] == '':
            return False

        item['subtitle'] = v.get('subtitle', '')

    return True

# Q:  报错dyld: Library not loaded: /usr/local/opt/openssl/lib/libssl.1.0.0.dylib
# A:  brew switch openssl 1.0.2s
def export_mongo_to_csv():
    '''
    mongoexport -h 127.0.0.1 --port 27017 -d mydb -c test_DestinyCollectibleDefinition 
    --query '' --fields="hash,name,description,icon,index,runOnlyAcquisitionRewardSite,itemHash,displayStyle,parentPresentationNodeHashes" 
    --csv -o ./DestinyCollectibleDefinition.csv
    '''

    cmd = '''
    mongoexport -h 127.0.0.1 --port 27017 -d mydb -c {table_name} --query "" --fields="{columns_name}" --type=csv -o {output_file_path}/{output_file_name}.csv
    '''

    for table_name in [
        'DestinyCollectibleDefinition',
        'DestinyInventoryItemDefinition',
        'DestinyLoreDefinition',
        'DestinyPlugSetDefinition',
        'DestinyRecordDefinition',
        'DestinySandboxPerkDefinition',
        'DestinyVendorDefinition',

        # special
        'DestinyInventoryItemDefinition_2',
    ]:
        columns_name = []
        date_str = time.strftime("%Y%m%d", time.localtime(int(time.time())))
        output_file_name = '%s_%s' % (date_str, table_name)

        table_name_t = 'test_%s' % table_name
        r = db_game[table_name_t].find_one()
        for i in r:
            if i == '_id':
                continue
            columns_name.append(str(i))
        columns_name = ','.join(columns_name)

        command = cmd.format(
            table_name=table_name_t, 
            columns_name=columns_name, 
            output_file_path=OUTPUT_FILE_PATH, 
            output_file_name=output_file_name
        )
        os.system(command)
        print command
    

def arrange_tables():
    tables = [
        'DestinyInventoryItemDefinition',
        'DestinyCollectibleDefinition',
        'DestinyPlugSetDefinition',
        'DestinySandboxPerkDefinition',

        'DestinyRecordDefinition',
        'DestinyVendorDefinition',
        'DestinyLoreDefinition',
    ]
    for t in tables:
        arrange_data(t)

    sp_table1()    


def main(download=True, clear_db=True, save_data=True, arrange_data=True, export_csv=True):
    if download:
        # 将api的json数据保存到本地
        api_data('205,300,302,304')
        print 'download finish'

    if clear_db:
        # 清除数据库的旧内容
        conn.drop_database('mydb')
        print 'clear db finish'

    if save_data:
        # 将json写入mongo
        save_data_into_mongo()
        print 'save to mongo finish'

    if arrange_data:
        # 整理数据
        arrange_tables()
        print 'arrange tables finish'

    if export_csv:
        # 导出
        export_mongo_to_csv()
        print 'export csv finish'


if __name__ == '__main__':
    print '[+] start'
    main()
    # main(download=False, clear_db=True, save_data=True, arrange_data=True, export_csv=True)
    # main(download=False, clear_db=False, save_data=True, arrange_data=True, export_csv=True)
    print '[+] end'