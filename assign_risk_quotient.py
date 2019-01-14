import json, csv
import pandas as pd
import numpy as np
from collections import Counter


def assign_risk_quotient():
    node1 = []
    node2 = []
    preimage_hash = []
    timestamp = []
    unique_nodes = []

    with open('/home/aravind/Desktop/Maha/Blockchain/channel_data_few.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            node1.append(row['node1'])
            node2.append(row['node2'])
            preimage_hash.append(row['preimage_hash'])
            timestamp.append(row['timestamp'])
            
    unique_nodes = node1 + node2
    unique_nodes = pd.unique(unique_nodes).tolist()    

    #Illegal nodes assignment..
    #Total nodes 42
    #Lets assign 3 nodes as illegal
    illegal_nodes = ['026fb4f0b5f6f85d7666a808e8f5d7a0c92b18b802b94a87528bbddab611a9fe36', 
                     '029671acc9ecfae0af2699239b9e63ca3ce6bf87fb58c222cbc1d758bc72e71ec5',
                     '032cc4541b25e86e39a7d450a979c1a9adbe2878df3a93fcb59c96c700bfe26aa3']

    legal_sensitivity_param = {}

    for node in unique_nodes:
        legal_sensitivity_param.update({node:0})

    illegal_nodes_index = []
    for node in illegal_nodes:
        #1. Assign 1 to illegal nodes
        legal_sensitivity_param.update({node:1})

        #2. Assign 0.7 - direct channel updates for closed channels
        index1 = [index1 for index1, value in enumerate(node1) if value == node]
        index2 = [index2 for index2, value in enumerate(node2) if value == node]
        
        for ind in index1:
            legal_sensitivity_param.update({node2[ind]:0.7})
        for ind in index2:
            legal_sensitivity_param.update({node1[ind]:0.7})

        illegal_nodes_index = illegal_nodes_index + index1 + index2
    
    #3. identify the preimage_hash and order the tx based on timestamp
    unique_preimage_hash = pd.unique(preimage_hash).tolist()
    preimage_hash_involving_illegal_nodes = []
    preimage_hash_index_dict = {}
    for hsh in unique_preimage_hash:
        if(hsh != '0'):
            hash_ts = np.array([[0,0]])
            for ind, (value, ts) in enumerate(zip(preimage_hash,timestamp)):
                if value == hsh:
                    hash_ts = np.append(hash_ts,[[ind,ts]], axis=0)
                    if ind in illegal_nodes_index and hsh not in preimage_hash_involving_illegal_nodes:
                        preimage_hash_involving_illegal_nodes.append(hsh)
            hash_ts = np.sort(hash_ts, axis=0)
            #print(hash_ts[1:,0])
            preimage_hash_index_dict.update({hsh:hash_ts[1:,0]})

    #4.1 Assign param starting from the hashes that involve illegal nodes
    for hsh in preimage_hash_involving_illegal_nodes:
        print(preimage_hash_index_dict[hsh])
        assigned_node = []
        n = len(preimage_hash_index_dict[hsh])

        #find start node
        ind_0 = int(preimage_hash_index_dict[hsh][0])
        n1 = node1[ind_0]
        n2 = node2[ind_0]
        start = 0
        start_sensitivity = -1

        if n > 1:
            ind_1 = int(preimage_hash_index_dict[hsh][1])
            n11 = node1[ind_1]
            n21 = node2[ind_1]
            if n1==n11 or n1==n21:
                start = n2
                start_sensitivity = legal_sensitivity_param[start]
            elif n2==n11 or n2==n21:
                start = n1
                start_sensitivity = legal_sensitivity_param[start]
        
        n1_sensitivity = legal_sensitivity_param[n1]
        n2_sensitivity = legal_sensitivity_param[n2]
        if start != 0:
            start_sensitivity = max(0.5, start_sensitivity)
            legal_sensitivity_param.update({start:start_sensitivity})
        else:
            start_sensitivity = max(n1_sensitivity,n2_sensitivity)
            if(start_sensitivity < 0.5):
                start_sensitivity = 0.5
                legal_sensitivity_param.update({n1:start_sensitivity})
                legal_sensitivity_param.update({n2:start_sensitivity})        
        print(n1, legal_sensitivity_param[n1], n2, legal_sensitivity_param[n2])

        if n > 2:
            for i in preimage_hash_index_dict[hsh][1:n-1]:
                old_sensitivity = legal_sensitivity_param[node1[int(i)]]
                new_sensitivity = max(old_sensitivity, round(0.2*start_sensitivity, 3))
                legal_sensitivity_param.update({node1[int(i)]:new_sensitivity})

                old_sensitivity = legal_sensitivity_param[node2[int(i)]]
                new_sensitivity = max(old_sensitivity, round(0.2*start_sensitivity, 3))
                legal_sensitivity_param.update({node2[int(i)]:new_sensitivity})

                assigned_node.append(node1[int(i)])
                assigned_node.append(node2[int(i)])
                print(node1[int(i)], legal_sensitivity_param[node1[int(i)]], node2[int(i)], legal_sensitivity_param[node2[int(i)]])

        ind_n = int(preimage_hash_index_dict[hsh][n-1])
        n11 = node1[ind_n]
        n21 = node2[ind_n]
        end_node = 0

        if n11 in assigned_node:
            end_node = n21
        elif n21 in assigned_node:
            end_node = n11

        if end_node == 0:
            old_sensitivity = legal_sensitivity_param[n11]
            new_sensitivity = max(old_sensitivity, round(0.7*start_sensitivity, 3))
            legal_sensitivity_param.update({n11:new_sensitivity})
            old_sensitivity = legal_sensitivity_param[n21]
            new_sensitivity = max(old_sensitivity, round(0.7*start_sensitivity, 3))
            legal_sensitivity_param.update({n21:new_sensitivity})
        else:
            old_sensitivity = legal_sensitivity_param[end_node]
            new_sensitivity = max(old_sensitivity, round(0.7*start_sensitivity, 3))
            legal_sensitivity_param.update({end_node:new_sensitivity})

        print(n11, legal_sensitivity_param[n11], n21, legal_sensitivity_param[n21])
    
    #4.2 Other hashes
    for hsh in preimage_hash_index_dict:
        if hsh not in preimage_hash_involving_illegal_nodes:
            print(preimage_hash_index_dict[hsh])
            assigned_node = []
            n = len(preimage_hash_index_dict[hsh])

            #find start index
            ind_0 = int(preimage_hash_index_dict[hsh][0])
            n1 = node1[ind_0]
            n2 = node2[ind_0]
            start = 0
            start_sensitivity = -1

            if n > 1:
                ind_1 = int(preimage_hash_index_dict[hsh][1])
                n11 = node1[ind_1]
                n21 = node2[ind_1]
                if n1==n11 or n1==n21:
                    start = n2
                    start_sensitivity = legal_sensitivity_param[start]
                elif n2==n11 or n2==n21:
                    start = n1
                    start_sensitivity = legal_sensitivity_param[start]
        
            n1_sensitivity = legal_sensitivity_param[n1]
            n2_sensitivity = legal_sensitivity_param[n2]
            if start == 0:
                start_sensitivity = max(n1_sensitivity,n2_sensitivity)
                legal_sensitivity_param.update({n1:start_sensitivity})
                legal_sensitivity_param.update({n2:start_sensitivity})
            else:
                av = round((n1_sensitivity+n2_sensitivity)/2, 3)  
                if av>start_sensitivity:
                    start_sensitivity = av
                    legal_sensitivity_param.update({start:start_sensitivity})      
            print(n1, legal_sensitivity_param[n1], n2, legal_sensitivity_param[n2])

            if n > 2:
                for i in preimage_hash_index_dict[hsh][1:n-1]:
                    old_sensitivity = legal_sensitivity_param[node1[int(i)]]
                    new_sensitivity = max(old_sensitivity, round(0.2*start_sensitivity, 3))
                    legal_sensitivity_param.update({node1[int(i)]:new_sensitivity})

                    old_sensitivity = legal_sensitivity_param[node2[int(i)]]
                    new_sensitivity = max(old_sensitivity, round(0.2*start_sensitivity, 3))
                    legal_sensitivity_param.update({node2[int(i)]:new_sensitivity})

                    assigned_node.append(node1[int(i)])
                    assigned_node.append(node2[int(i)])
                    print(node1[int(i)], legal_sensitivity_param[node1[int(i)]], node2[int(i)], legal_sensitivity_param[node2[int(i)]])

            ind_n = int(preimage_hash_index_dict[hsh][n-1])
            n11 = node1[ind_n]
            n21 = node2[ind_n]
            end_node = 0

            if n11 in assigned_node:
                end_node = n21
            elif n21 in assigned_node:
                end_node = n11

            if end_node == 0:
                old_sensitivity = legal_sensitivity_param[n11]
                new_sensitivity = max(old_sensitivity, round(0.7*start_sensitivity, 3))
                legal_sensitivity_param.update({n11:new_sensitivity})
                old_sensitivity = legal_sensitivity_param[n21]
                new_sensitivity = max(old_sensitivity, round(0.7*start_sensitivity, 3))
                legal_sensitivity_param.update({n21:new_sensitivity})
            else:
                old_sensitivity = legal_sensitivity_param[end_node]
                new_sensitivity = max(old_sensitivity, round(0.7*start_sensitivity, 3))
                legal_sensitivity_param.update({end_node:new_sensitivity})

            print(n11, legal_sensitivity_param[n11], n21, legal_sensitivity_param[n21])


    print("Legal sensitivity params")
    print(legal_sensitivity_param)
            
            

    #print(preimage_hash_index_dict)



    



assign_risk_quotient()