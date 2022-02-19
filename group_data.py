import os
import linecache

def get_op(op):
    op = op.strip('\n')
    arr = op[2:-1].split(',')
    return {
        'op_type': op[0],
        'var': arr[0],
        'val': arr[1],
        'client_id': int(arr[2]),
        'tra_id': int(arr[3]),
    }

for i in range(100):
    folder_name = 'output/' + str(i) + '/'
    file_list = [fn for fn in os.listdir(folder_name) if fn.endswith('.txt')]
    ops = []
    for file in file_list:
        ops += linecache.getlines(folder_name + file)
    ops_per_trans = 25
    with open(folder_name + 'result.txt', 'w') as f:
        now_id = 0
        cnt = ops_per_trans
        for op in ops:
            op_dict = get_op(op)
            cnt = cnt - 1
            op_dict['tra_id'] = now_id
            if cnt == 0:
                cnt = ops_per_trans
                now_id += 1
            f.write(str(op_dict['op_type']) + '(' + str(op_dict['var']) + ',' + str(op_dict['val']) + ',' + str(op_dict['client_id']) + ',' + str(op_dict['tra_id']) + ')\n')

