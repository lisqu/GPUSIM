#! /bin/bash

#cp bin/edu/umich/clarity/sim.jar ./

n_bg=(1 2 4 6 8)

service=(face dig imc pos ner)
service=(face dig imc pos ner)

for tg in ${service[@]}; do
    for bg in ${service[@]}; do
        for b in ${n_bg[@]}; do
            java -jar sim.jar ${tg} ${bg} 1 ${b} 200 5000
        done
    done
done
