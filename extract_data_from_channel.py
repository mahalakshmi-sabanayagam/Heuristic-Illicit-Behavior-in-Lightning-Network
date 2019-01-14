import json, csv
import pandas as pd
from collections import Counter

def extract_data():
    with open('/home/aravind/Desktop/Maha/Blockchain/channel_closed_1ml.json') as f:
        data = json.load(f)
    
    nodes = []
    node1_pub = []
    node2_pub = []
    channel_id = []
    channel_tx_id = []
    timestamp = []
    preimage_hash = []

    with open('/home/aravind/Desktop/Maha/Blockchain/channel_data_few.csv','w') as csv_file:
        fieldnames = ['node1', 'node2', 'channel_id', 'channel_tx_id', 'preimage_hash', 'timestamp']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(len(data)):
            #writer.writerow({'node1': data[i]['node1_pub'], 'node2': data[i]['node2_pub'], 'channel_id': data[i]['channel_id'], 'channel_tx_id': data[i]['chan_point'], 'preimage_hash': 'nil', 'timestamp': data[i]['last_update']})
            node1_pub.append(data[i]['node1_pub'])
            node2_pub.append(data[i]['node2_pub'])
            channel_id.append(data[i]['channel_id'])
            channel_tx_id.append(data[i]['chan_point'])
            timestamp.append(data[i]['last_update'])
            preimage_hash.append(0)

            #nodes.append(data[i]['node1_pub'])
            #nodes.append(data[i]['node2_pub'])

        nodes = node1_pub + node2_pub
        nodes_unique = pd.unique(nodes).tolist()
        #print(nodes_unique)
        print(len(nodes_unique),len(nodes))
        count_unique_nodes = Counter(nodes)
        count_unique_nodes = count_unique_nodes.most_common(5)
        print(count_unique_nodes)

        write_ind = []
        for node in count_unique_nodes:
            print(node[0])
            index1 = [index1 for index1, value in enumerate(node1_pub) if value == node[0]]
            index2 = [index2 for index2, value in enumerate(node2_pub) if value == node[0]]
            index = index1 + index2
            print(index1, index2, index)
            for ind in index:
                if ind not in write_ind:
                    writer.writerow({'node1': node1_pub[ind], 'node2': node2_pub[ind], 'channel_id': channel_id[ind], 'channel_tx_id': channel_tx_id[ind], 'preimage_hash': preimage_hash[ind], 'timestamp': timestamp[ind]})
                    write_ind.append(ind)
            
        print(write_ind)
    
extract_data()