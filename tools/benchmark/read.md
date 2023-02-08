串行化的读请求
线性读请求



＃线性读请求
export IP_1=127.0.0.1:2379
benchmark --endpoints=${IP_1} --conns=1 --clients=1 range KEY --total=10000
benchmark --endpoints=${IP_1},${IP_2},${IP_3} --conns=1 --clients=1 range KEY --total=10000
benchmark --endpoints=${IP_1},${IP_2},${IP_3} --conns=1 --clients=100 range KEY --total=10000

＃对每个etcd节点发起可串行化读请求,并对各个节点的数据求和
for endpoint in ${IP_1} ${IP_2} ${IP_3};do
 benchmark --endpoints=$endpoint conns=1 --clients=1 range KEY --total=10000
done
for endpoint in ${IP_1} ${IP_2} ${IP_3};do
 benchmark --endpoints=$endpoint conns=100 --clients=100 range KEY --total=10000
done

benchmark --endpoints=${IP_1} --conns=1 --clients=1 put --key-size=8 --sequential-keys --total=10000 --val-size=256
benchmark --endpoints=${IP_1} --conns=100 --clients=100 put --key-size=8 --sequential-keys --total=10000 --val-size=256
benchmark --endpoints=${IP_1},${IP_2},${IP_3} --conns=100 --clients=100 put --key-size=8 --sequential-keys --total=10000 --val-size=256


