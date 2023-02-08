res = ''

map_ = {
    'XXX_Merge(src proto.Message)',
    'XXX_Unmarshal(b []byte) error',
    'XXX_Marshal(b []byte,',
    'XXX_Merge(',
    'XXX_Size() int',
    'XXX_DiscardUnknown()',
    # 'MarshalTo(dAtA []byte) (int, error)',
    # 'MarshalToSizedBuffer(dAtA []byte) (int, er',
}
file = './etcdserverpb/etcdserver.pb.go'
with open(file, 'r', encoding='utf8') as f:
    flag = False
    for line in f.readlines():
        if ') Marshal() (' in line:
            print(line.strip()+'return json.Marshal(m)}')
        if ') Size() (' in line:
            print(line.strip() + 'marshal,_:= json.Marshal(m) return len(marshal) }')
        if ') Unmarshal(' in line:
            print(line.strip() + 'return json.Unmarshal(dAtA,m) }')
        # if not flag:
        #     for item in map_:
        #         if item in line:
        #             flag = True
        # if flag:
        #     if line == '}\n':
        #         flag = False
        #         continue
        # if line.startswith('var xxx_'):
        #     continue
        # if not flag:
        #     res += line
# with open(file, 'w', encoding='utf8') as f:
#     f.write(res)
