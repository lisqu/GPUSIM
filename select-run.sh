#! /bin/bash

#cp bin/edu/umich/clarity/sim.jar ./

#n_bg=(1 2 4 6 8)
n_bg=(8)

tg=$1
bg=$2

service=(face dig imc pos ner)
bg_service1=(face dig imc)
bg_service2=(pos ner)

for b in ${n_bg[@]}; do
    echo "end_to_end" > simulated/sim-${tg}-${n_bg}-${bg}.csv
        for((i=0;i<50;i++))
        do
            java -jar sim.jar ${tg} ${bg} 1 ${b} 200 10000
        done
done

#for tg in ${service[@]}; do
#    for bg in ${bg_service1[@]}; do
#        for b in ${n_bg[@]}; do
#            java -jar sim.jar ${tg} ${bg} 1 ${b} 300 3000
#        done
#    done
#done

#for tg in ${service[@]}; do
#    for bg in ${bg_service2[@]}; do
#        for b in ${n_bg[@]}; do
#            java -jar sim.jar ${tg} ${bg} 1 ${b} 300 10000
#        done
#    done
#done
