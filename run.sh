#! /bin/bash

#cp bin/edu/umich/clarity/sim.jar ./

n_bg=(1 2 4 6 8)

service=(face dig imc pos ner)
bg_service1=(face dig imc)
bg_service2=(pos ner)

for tg in ${service[@]}; do
    for bg in ${bg_service1[@]}; do
        for b in ${n_bg[@]}; do
            java -jar sim.jar ${tg} ${bg} 1 ${b} 1000 3000
        done
    done
done

for tg in ${service[@]}; do
    for bg in ${bg_service2[@]}; do
        for b in ${n_bg[@]}; do
            java -jar sim.jar ${tg} ${bg} 1 ${b} 1000 10000
        done
    done
done
